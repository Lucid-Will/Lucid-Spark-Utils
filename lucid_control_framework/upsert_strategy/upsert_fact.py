from .upsert_strategy import UpsertStrategy
from typing import Optional, Dict, Any
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

class UpsertFact(UpsertStrategy):
    """
    This class implements the upsert strategy for fact tables. It inherits from the abstract base class `UpsertStrategy`.

    The `upsert_to_table` method performs an upsert operation on a Delta fact table based on the provided configuration.
    """

    def upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'path') -> None:
        """
        Performs an upsert operation on a Delta fact table based on the provided configuration.

        :param config: Configuration for upserting a fact table including 'table_name', 'dataframe',
                    'composite_columns' and 'primary_key_column'. Assumes audit columns are relevant but less complex.
        :param write_method: The method to use for saving the table ('path' or 'catalog').
        :param storage_container_endpoint: Endpoint if using 'path' write method.
        """
        # Ensure config contains the necessary keys
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            composite_columns = config['composite_columns']
            primary_key_column = config['primary_key_column']
        except KeyError as e:
            self.logger.error(f"Config is missing necessary key: {str(e)}")
            raise
        
        current_ts = current_timestamp()

        try:
            # Set composite_column to None by default
            composite_key_column = None
            
            # Assign composite_key_column if composite_columns provided
            if composite_columns:
                # Set composite key column name
                composite_key_column = primary_key_column.replace("key", "composite_key")
        except Exception as e:
            raise ValueError(f"Composite key column could not be generated: {str(e)}")

        try:
            # Generate keys using the transformation method
            if self.transformer:
                # Stage the dataframe with keys
                df_source = self.transformer.stage_dataframe_with_keys(storage_container_endpoint, table_name, df_source, primary_key_column, composite_key_column, composite_columns)
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
                if write_method == 'path':
                    # Check for storage container endpoint
                    if not storage_container_endpoint:
                        raise ValueError("Storage container endpoint must be provided when using 'path' write method.")
                    df_source.write.format('delta').mode('overwrite').save(f'{storage_container_endpoint}/Tables/{table_name}')
                elif write_method == 'catalog':
                    df_source.write.format('delta').saveAsTable(table_name)
                else:
                    raise ValueError("Invalid write method provided. Use 'path' or 'catalog'.")
            except Exception as e:
                self.logger.info(f'Writing of initial load for table {table_name} failed with error: {str(e)}')
                raise

            self.logger.info(f'Initial load for table {table_name} complete. Exiting upsert operation.')
            return

        # Define match condition for the MERGE operation
        match_condition = " AND ".join([f"target.{k} = source.{k}" for k in composite_columns])
        
        # Exclude key columns from change detection
        key_columns = [primary_key_column, composite_key_column]
        audit_columns = ['inserted_date_time', 'updated_date_time']
        exclude_columns = composite_columns + key_columns + audit_columns

        # Get non-match key fields excluding audit fields
        non_composite_columns = [col for col in df_source.columns if col not in exclude_columns]
        
        # Create a condition to check if any non-match key field has changed
        update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_composite_columns])
        
        # Configure the merge operation to update existing records and insert new records
        merge_operation = deltaTable.alias("target") \
            .merge(df_source.alias("source"), match_condition)
        
        # Configure the merge operation to update existing records and insert new records
        merge_operation = merge_operation.whenMatchedUpdate(
            condition=update_condition,
            set={field: f"source.{field}" for field in non_composite_columns} | {"updated_date_time": "source.updated_date_time"}) \
            .whenNotMatchedInsertAll()

        # Execute the merge operation
        try:
            merge_operation.execute()
        except Exception as e:
            self.logger.error(f"Error occurred during merge operation: {str(e)}")
        
        self.logger.info(f"Fact upsert for {table_name} complete.")