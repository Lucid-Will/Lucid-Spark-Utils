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
        This function supports generic updsert and auditing with specified columns.

        :param config: Configuration for upserting a Delta table including 'table_name', 'dataframe',
                    'composite_columns', and 'primary_key_column'. Expects audit columns in the dataframe.
        :param write_method: The method to use for saving the table ('path' or 'catalog').
        :param storage_container_endpoint: Endpoint if using 'path' write method.

        :return: None. The function performs an upsert operation on the Delta table.

        Raises:
            Exception: If any error occurs during the upsert operation.

        Example:
            config = {
                'table_name': 'target_table',
                'dataframe': spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                'composite_columns': ['ID', 'First Name'],
                'upsert_type': 'generic',
                'primary_key_column': 'primary_key'
            }
            write_method = 'catalog'
            storage_container_endpoint = None
            upsert_delta_table(config, write_method, storage_container_endpoint)
        """
        # Confirm required configuration provided
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            composite_columns = config['composite_columns']
            primary_key_column = config.get('primary_key_column')
        except KeyError as e:
            raise ValueError(f"Configuration must include 'table_name', 'dataframe', 'composite_columns', and 'primary_key_column': {str(e)}'")
        
        try:
            # Determine if all data columns are part of the composite key
            all_columns_are_composite_columns = set(composite_columns) == set(df_source.columns)
            
            # Set composite_key_column to None by default
            composite_key_column = None
            
            # Assign composite_key_column if composite_columns provided
            if composite_columns:
                # Set composite key column name
                composite_key_column = primary_key_column.replace("key", "composite_key")
        except Exception as e:
            raise ValueError(f"Composite key column could not be generated: {str(e)}")

        try:
            # Generate keys using the unified transformation method
            if self.transformer:
                # Stage the dataframe with keys
                df_source = self.transformer.stage_dataframe_with_keys(
                    storage_container_endpoint, 
                    table_name, 
                    df_source, 
                    primary_key_column, 
                    composite_key_column, 
                    composite_columns
                )
        except Exception as e:
            self.logger.error(f"Failed to generate keys for table {table_name}: {e}")
            raise

        # Attempt to access the Delta table and perform initial load if not found
        try:
            deltaTable = DeltaTable.forName(self.spark, table_name)
        except Exception:
            self.logger.info(f'Table {table_name} not found. Creating table.')

            # Add audit columns to the dataframe
            try:
                df_source = self.add_insert_audit_columns(df_source)
            except Exception as e:
                self.logger.info(f'Addition of audit columns for table {table_name} failed with error: {str(e)}')
                raise

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
        
        # Create merge conditions
        match_condition = ' AND '.join([f'target.{col} = source.{col}' for col in composite_columns]) + ' AND target.is_current = true'
        update_condition = ' OR '.join([f'target.{col} != source.{col}' for col in change_detection_columns])
        
        # Generate field exclusions for the merge operation
        try:
            # Begin conditional merge
            if not all_columns_are_composite_columns:            
                # Exclude key columns from change detection
                key_columns = [primary_key_column, composite_key_column]
                audit_columns = ['inserted_date_time', 'updated_date_time']
                exclude_columns = composite_columns + key_columns + audit_columns

                # Get columns for change detection
                change_detection_columns = [col for col in df_source.columns if col not in exclude_columns]

                # Set record expiration expression
                current_ts = current_timestamp()
                update_expr = {
                    'updated_date_time': current_ts
                }
                
                # Build merge operation
                merge_operation = deltaTable.alias('target').merge(
                    source=df_source.alias('source'),
                    condition=match_condition
                ).whenMatchedUpdate(
                    condition=update_condition, 
                    set=update_expr
                ).whenNotMatchedInsertAll()

            else:
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