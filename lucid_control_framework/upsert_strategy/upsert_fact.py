from .upsert_strategy import UpsertStrategy
from typing import Optional, Dict, Any
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

class UpsertFact(UpsertStrategy):
    """
    This class implements the upsert strategy for fact tables. It inherits from the abstract base class `UpsertStrategy`.

    The `upsert_to_table` method performs an upsert operation on a Delta fact table based on the provided configuration.
    """

    def upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'default') -> None:
        """
        Performs an upsert operation on a Delta fact table based on the provided configuration.

        :param config: Configuration for upserting a fact table including 'table_name', 'dataframe',
                    and 'match_keys'. Assumes audit columns are relevant but less complex.
        :param write_method: The method to use for saving the table ('default' or 'abfss').
        :param storage_container_endpoint: Endpoint if using 'abfss' write method.
        """
        # Ensure config contains the necessary keys
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            match_keys = config['match_keys']
        except KeyError as e:
            self.logger.error(f"Config is missing necessary key: {str(e)}")
            raise

        skey_column = config.get('skey_column')
        nkey_column = config.get('nkey_column')
        current_ts = current_timestamp()

        # Generate keys using the unified transformation method
        try:
            if self.transformer:
                df_source = self.transformer.stage_dataframe_with_key(df_source, df_source.columns, nkey_column, match_keys, False)
                df_source = self.transformer.stage_dataframe_with_key(df_source, df_source.columns, skey_column, match_keys, True)
        except Exception as e:
            self.logger.error(f"Error occurred during key generation: {str(e)}")

        # Enhance df_source with necessary audit fields
        df_source = (df_source
                    .withColumn("inserted_date_time", current_ts)
                    .withColumn("updated_date_time", current_ts))

        # Try to access the Delta table, create if it does not exist
        try:
            deltaTable = DeltaTable.forName(self.spark, table_name)
        except Exception:
            self.logger.info(f"Fact table {table_name} does not exist. Creating it.")

            # Choose the write method based on the provided arguments
            write_methods = {
                'default': lambda: df_source.write.format("delta").saveAsTable(table_name),
                'abfss': lambda: df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}") if storage_container_endpoint else ValueError("storage_container_endpoint is required for 'abfss' write method")
            }

            try:
                write_methods[write_method]()
            except KeyError:
                self.logger.error(f"Invalid write method: {write_method}")
                raise
            except ValueError as e:
                self.logger.error(str(e))
                raise

        # Define match condition for the MERGE operation
        match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])
        
        # Check delete logic
        has_is_deleted = 'is_deleted' in df_source.columns
        
        # Generate excluded column list
        excluded_columns = match_keys + ["inserted_date_time", "updated_date_time", skey_column, nkey_column, "is_deleted" if has_is_deleted else ""]
        
        # Get non-match key fields excluding audit fields
        non_match_keys = [col for col in df_source.columns if col not in excluded_columns]
        
        # Create a condition to check if any non-match key field has changed
        update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_match_keys])
        
        # Configure the merge operation to update existing records and insert new records
        merge_operation = deltaTable.alias("target") \
            .merge(df_source.alias("source"), match_condition)
        
        # Determine if deletes are required and update the condition accordingly
        try:
            if has_is_deleted:
                merge_operation = merge_operation.whenMatchedDelete(condition="source.is_deleted = 'true'")
                update_condition = f"source.is_deleted <> 'true' AND ({update_condition})"
        except Exception as e:
            self.logger.error(f"Error occurred during delete logic check: {str(e)}")
        
        # Configure the merge operation to update existing records and insert new records
        merge_operation = merge_operation.whenMatchedUpdate(
            condition=update_condition,
            set={field: f"source.{field}" for field in non_match_keys} | {"updated_date_time": "source.updated_date_time"}) \
            .whenNotMatchedInsertAll()
        
        # Execute the merge operation
        try:
            merge_operation.execute()
        except Exception as e:
            self.logger.error(f"Error occurred during merge operation: {str(e)}")
        
        self.logger.info(f"Fact upsert for {table_name} complete.")