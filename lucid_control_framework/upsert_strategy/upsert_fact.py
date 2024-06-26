from .upsert_strategy import UpsertStrategy
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

class UpsertFact(UpsertStrategy):
    """
    This class implements the upsert strategy for fact tables. It inherits from the abstract base class `UpsertStrategy`.

    The `upsert_to_table` method performs an upsert operation on a Delta fact table based on the provided configuration.
    """

    def add_insert_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Adds audit columns to the source dataframe for upsert.
        """
        # Get the current timestamp
        current_ts = current_timestamp()

        # Add the audit columns to the dataframe and return
        return (df.withColumn("inserted_date_time", current_ts)
                .withColumn("updated_date_time", current_ts))
    
    def upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None:
        """
        Performs an upsert operation on a Delta fact table based on the provided configuration.

        :param config: Configuration for upserting a fact table. It should include the following keys:
            'table_name': The name of the table to upsert.
            'dataframe': The dataframe containing the data to upsert.
            'composite_columns': A list of columns to use for creating a composite key. If this is None or empty, a composite key will not be used.
            'primary_key_column': The name of the primary key column.
            This function assumes that audit columns are relevant but less complex.
        :param storage_container_endpoint: The endpoint of the storage container if using 'path' write method. This is optional and defaults to None.
        :param write_method: The method to use for saving the table. This should be either 'path' or 'catalog'. Defaults to 'catalog'.

        This function performs the following steps:
        1. Checks the config for necessary keys.
        2. Generates a composite key column if composite_columns is provided.
        3. Stages the dataframe with keys using the transformation method if a transformer is provided.
        4. Adds audit columns to the dataframe.
        5. Attempts to access the Delta table and performs an initial load if the table is not found.
        6. Defines a match condition for the MERGE operation.
        7. Excludes key columns and audit columns from change detection.
        8. Creates a condition to check if any non-match key field has changed.
        9. Configures the merge operation to update existing records and insert new records.
        10. Executes the merge operation.
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
                df_source = self.transformer.stage_dataframe_with_keys(
                    table_name, 
                    df_source, 
                    primary_key_column, 
                    composite_key_column, 
                    composite_columns,
                    write_method,
                    storage_container_endpoint
                )
        except Exception as e:
            self.logger.error(f"Failed to generate keys for table {table_name}: {e}")
            raise

        # Add audit columns to the dataframe
        try:
            df_source = self.add_insert_audit_columns(df_source)
        except Exception as e:
            self.logger.info(f'Addition of audit columns for table {table_name} failed with error: {str(e)}')
            raise

        # Attempt to access the Delta table and perform initial load if not found
        try:
            if write_method == 'path':
                storage_container_endpoint_path = f'{storage_container_endpoint}/Tables/{table_name}'
                deltaTable = DeltaTable.forPath(self.spark, storage_container_endpoint_path)
            elif write_method == 'catalog':
                deltaTable = DeltaTable.forName(self.spark, table_name)
            else:
                raise ValueError(f"Unsupported write method: {write_method}")
        except Exception:
            self.logger.info(f'Table {table_name} not found. Creating table.')

            # Write the dataframe to a Delta table
            try:
                self.table_manager.write_delta_table(
                    df_source, 
                    table_name, 
                    storage_container_endpoint, 
                    write_method
                )
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