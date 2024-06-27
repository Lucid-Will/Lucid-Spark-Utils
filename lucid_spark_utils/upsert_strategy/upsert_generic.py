from .upsert_strategy import UpsertStrategy
from pyspark.sql import DataFrame
from typing import Any, Dict, Optional
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

class UpsertGeneric(UpsertStrategy):

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
        Performs an upsert operation (merge) on a Delta table based on the provided configuration.
        This function supports generic upsert and auditing with specified columns.

        :param config: Configuration for upserting a Delta table. It should include the following keys:
            'table_name': The name of the table to upsert.
            'dataframe': The dataframe containing the data to upsert.
            'composite_columns': A list of columns to use for creating a composite key. If this is None or empty, a composite key will not be used.
            'primary_key_column': The name of the primary key column.
            This function assumes that audit columns are relevant but less complex.
        :param storage_container_endpoint: The endpoint of the storage container if using 'path' write method. This is optional and defaults to None.
        :param write_method: The method to use for saving the table. This should be either 'path' or 'catalog'. Defaults to 'catalog'.

        :return: None. The function performs an upsert operation on the Delta table.

        Raises:
            Exception: If any error occurs during the upsert operation.

        Example:
            config = {
                'table_name': 'target_table',
                'dataframe': spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                'composite_columns': ['ID', 'First Name'],
                'upsert_type': 'generic',
                'primary_key_column': 'ID',
                'add_composite_key': True
            }
            write_method = 'catalog'
            storage_container_endpoint = None
            upsert_to_table(config, storage_container_endpoint, write_method)

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
        # Confirm required configuration provided
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            composite_columns = config.get('composite_columns')
            primary_key_column = config.get('primary_key_column')
            add_composite_key = config.get('add_composite_key', False)
        except KeyError as e:
            raise ValueError(f"Configuration must include 'table_name', 'dataframe', 'composite_columns', and 'primary_key_column': {str(e)}'")
        
        try:
            # Set composite_key_column to None by default
            composite_key_column = None
            
            # Determine if all data columns are part of the composite key
            if composite_columns:
                all_columns_are_composite_columns = set(composite_columns) == set(df_source.columns)

            # Set composite key column name
            if add_composite_key == True:
                composite_key_column = primary_key_column.replace("_key", "_composite_key")
            
        except Exception as e:
            raise ValueError(f"Composite key column could not be generated: {str(e)}")
        
        # Add audit columns to the dataframe
        try:
            df_source = self.add_insert_audit_columns(df_source)
        except Exception as e:
            self.logger.info(f'Addition of audit columns for table {table_name} failed with error: {str(e)}')
            raise

        try:
            # Generate keys using the unified transformation method
            if self.transformer:
                # Stage the dataframe with keys
                df_source = self.transformer.stage_dataframe_with_keys(
                    table_name, 
                    df_source, 
                    primary_key_column, 
                    composite_key_column, 
                    composite_columns,
                    write_method,
                    storage_container_endpoint,
                    add_composite_key
                )
        except Exception as e:
            self.logger.error(f"Failed to generate keys for table {table_name}: {e}")
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
            deltaTable.show()

            # Write the dataframe to a Delta table
            try:
                self.table_manager.write_delta_table(
                    df_source, 
                    table_name, 
                    storage_container_endpoint, 
                    write_method
                )
            except Exception as e:
                self.logger.info(f'Writing of table {table_name} failed with error: {str(e)}')
                raise

            self.logger.info(f'Writing of table {table_name} complete. Exiting upsert operation.')
            return
        
        # Begin conditional upsert operation
        try:
            # Check if composite_columns is None or empty
            if not composite_columns:
                # Directly insert data from df_source into deltaTable
                self.table_manager.write_delta_table(
                    df_source, 
                    table_name, 
                    storage_container_endpoint, 
                    write_method,
                    write_mode = 'append'
                )

                print(f'Upsert operation for table {table_name} complete.')
                return
            
            elif not all_columns_are_composite_columns:            
                # Exclude key columns from change detection
                key_columns = [primary_key_column, composite_key_column]
                audit_columns = ['inserted_date_time', 'updated_date_time']
                exclude_columns = composite_columns + key_columns + audit_columns
                print('Exclude columns:', exclude_columns)

                # Get columns for change detection
                change_detection_columns = [col for col in df_source.columns if col not in exclude_columns]
                print('Change detection columns:', change_detection_columns)

                # Create merge conditions
                match_condition = ' AND '.join([f'target.{col} = source.{col}' for col in composite_columns])
                update_condition = ' OR '.join([f'target.{col} != source.{col}' for col in change_detection_columns])
                print('Match condition:', match_condition)
                print('Update condition:', update_condition)

                # Set record expiration expression
                current_ts = current_timestamp()
                update_expr = {
                    'updated_date_time': current_ts
                }
                display(df)
                # Build merge operation
                merge_operation = deltaTable.alias('target').merge(
                    source=df_source.alias('source'),
                    condition=match_condition
                ).whenMatchedUpdate(
                    condition=update_condition, 
                    set=update_expr
                ).whenNotMatchedInsertAll()
                print('Merge operation:', merge_operation)

            else:
                # Create merge conditions
                match_condition = ' AND '.join([f'target.{col} = source.{col}' for col in composite_columns])

                # Alternate merge logic: Insert new rows where there's no match, do nothing for matches.
                merge_operation = deltaTable.alias("target") \
                    .merge(
                        source = df_source.alias("source"), 
                        condition = match_condition
                    ).whenNotMatchedInsertAll()

            # Execute the merge operation
            merge_operation.execute()

            print(f'Upsert operation for table {table_name} complete.')
        except Exception as e:
            self.logger.info(f'Insert operation for table {table_name} failed with error: {str(e)}')
            raise