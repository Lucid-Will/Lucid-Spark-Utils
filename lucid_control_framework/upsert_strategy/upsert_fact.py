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
                    'match_keys' and 'skey_column'. Assumes audit columns are relevant but less complex.
        :param write_method: The method to use for saving the table ('default' or 'abfss').
        :param storage_container_endpoint: Endpoint if using 'abfss' write method.
        """
        # Ensure config contains the necessary keys
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            match_keys = config['match_keys']
            skey_column = config['skey_column']
        except KeyError as e:
            self.logger.error(f"Config is missing necessary key: {str(e)}")
            raise
        
        current_ts = current_timestamp()

        try:
            # Set nkey_column to None by default
            nkey_column = None
            
            # Assign nkey_column if match_keys provided
            if match_keys:
                # Set natural key column name
                nkey_column = skey_column.replace("key", "natural_key")
        except Exception as e:
            raise ValueError(f"Natural key column could not be generated: {str(e)}")

        try:
            # Generate keys using the transformation method
            if self.transformer:
                # Stage the dataframe with keys
                df_source = self.transformer.stage_dataframe_with_keys(table_name, df_source, df_source.columns, skey_column, nkey_column, match_keys)
        except Exception as e:
            self.logger.error(f"Failed to generate keys for table {table_name}: {e}")
            raise

        # Enhance df_source with necessary audit fields
        df_source = (df_source
                    .withColumn("inserted_date_time", current_ts)
                    .withColumn("updated_date_time", current_ts))

        # Try to access the Delta table, create if it does not exist
        try:
            deltaTable = DeltaTable.forName(self.spark, table_name)
        except Exception:
            self.logger.info(f"Fact table {table_name} does not exist. Creating it.")

            # Write the dataframe to the Delta table
            try:
                if write_method == 'abfss':
                    # Check for storage container endpoint
                    if not storage_container_endpoint:
                        raise ValueError("Storage container endpoint must be provided when using 'abfss' write method.")
                    df_source.write.format('delta').save(f'abfss://{storage_container_endpoint}/Tables/{table_name}')
                elif write_method == 'default':
                    df_source.write.format('delta').saveAsTable(table_name)
                else:
                    raise ValueError("Invalid write method provided. Use 'default' or 'abfss'.")
            except Exception as e:
                self.logger.info(f'Writing of initial load for table {table_name} failed with error: {str(e)}')
                raise

            self.logger.info(f'Initial load for table {table_name} complete. Exiting upsert operation.')
            return

        # Define match condition for the MERGE operation
        match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])
        
        # Exclude key columns from change detection
        key_columns = [skey_column, nkey_column]
        audit_columns = ['inserted_date_time', 'updated_date_time']
        exclude_columns = match_keys + key_columns + audit_columns

        # Get non-match key fields excluding audit fields
        non_match_keys = [col for col in df_source.columns if col not in exclude_columns]
        
        # Create a condition to check if any non-match key field has changed
        update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_match_keys])
        
        # Configure the merge operation to update existing records and insert new records
        merge_operation = deltaTable.alias("target") \
            .merge(df_source.alias("source"), match_condition)
        
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