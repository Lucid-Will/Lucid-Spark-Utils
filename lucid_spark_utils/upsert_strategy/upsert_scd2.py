from .upsert_strategy import UpsertStrategy
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql import DataFrame
from typing import Any, Dict, Optional
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
from decimal import Decimal
import datetime

class UpsertSCD2(UpsertStrategy):

    def insert_unknown_record(self, df_source):
        """
        Inserts an unknown record into the source dataframe. This function creates a new record with default values for each field in the dataframe and appends it to the dataframe.

        :param df_source: The source dataframe where the unknown record will be inserted.

        :return: The source dataframe with the unknown record inserted.

        Raises:
            ValueError: If the field type is not supported.

        This function performs the following steps:
        1. It creates a dictionary with default values for each field in the dataframe. The default values depend on the field type.
        2. It creates a new row with the same schema as the source dataframe using the dictionary of default values.
        3. It appends the new row to the source dataframe.
        4. It returns the source dataframe with the unknown record inserted.

        Example:
        df_source = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
        df_source = insert_unknown_record(df_source)
        """
        # Create a dictionary with default values for each field in df_source
        unknown_record = {}
        for field in df_source.schema.fields:
            if field.dataType.typeName() == 'string':
                unknown_record[field.name] = 'Unknown'
            elif field.dataType.typeName() in ['integer', 'double', 'long', 'short', 'byte', 'float']:
                unknown_record[field.name] = -1
            elif field.dataType.typeName() == 'date':
                unknown_record[field.name] = datetime.date(1901, 1, 1)
            elif field.dataType.typeName() == 'timestamp':
                unknown_record[field.name] = datetime.datetime(1901, 1, 1)
            elif field.dataType.typeName() == 'boolean':
                unknown_record[field.name] = False
            elif field.dataType.typeName() == 'decimal':
                unknown_record[field.name] = Decimal(-1.0)
            else:
                raise ValueError(f"Unsupported field type: {field.dataType.typeName()}")

        # Create a row with the same schema as df_source
        row = Row(**unknown_record)
        df_source = self.spark.createDataFrame([row], df_source.schema)

        return df_source
    
    def add_insert_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Adds audit columns to the source dataframe for an initial load.

        :param df: The source dataframe to which audit columns will be added.

        :return: The source dataframe with added audit columns.

        This function performs the following steps:
        1. It gets the current timestamp.
        2. It adds the 'inserted_date_time' and 'updated_date_time' columns to the dataframe, setting their values to the current timestamp.
        3. It adds the 'effective_from_date_time' column to the dataframe, setting its value to January 1, 1901 at 00:00:00.
        4. It adds the 'effective_to_date_time' column to the dataframe, setting its value to December 31, 9999 at 23:59:59.
        5. It adds the 'is_current' column to the dataframe, setting its value to True.
        6. It adds the 'is_deleted' column to the dataframe, setting its value to False.

        Example:
        df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
        df = add_insert_audit_columns(df)
        """
        # Get the current timestamp
        current_ts = current_timestamp()

        # Add the audit columns to the dataframe and return
        return (df.withColumn("inserted_date_time", current_ts)
                .withColumn("updated_date_time", current_ts)
                .withColumn("effective_from_date_time", lit(datetime.datetime(1901, 1, 1, 00, 00, 00)))
                .withColumn("effective_to_date_time", lit(datetime.datetime(9999, 12, 31, 23, 59, 59)))
                .withColumn("is_current", lit(True))
                .withColumn("is_deleted", lit(False)))
    
    def add_update_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Adds audit columns to the source dataframe for an upsert operation.

        :param df: The source dataframe to which audit columns will be added.

        :return: The source dataframe with added audit columns.

        This function performs the following steps:
        1. It gets the current timestamp.
        2. It adds the 'inserted_date_time' and 'updated_date_time' columns to the dataframe, setting their values to the current timestamp.
        3. It adds the 'effective_from_date_time' column to the dataframe, setting its value to the current timestamp.
        4. It adds the 'effective_to_date_time' column to the dataframe, setting its value to December 31, 9999 at 23:59:59.
        5. It adds the 'is_current' column to the dataframe, setting its value to True.
        6. It adds the 'is_deleted' column to the dataframe, setting its value to False.

        Example:
        df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
        df = add_update_audit_columns(df)
        """
        # Get the current timestamp
        current_ts = current_timestamp()

        # Add the audit columns to the dataframe and return
        return (df.withColumn("inserted_date_time", current_ts)
                .withColumn("updated_date_time", current_ts)
                .withColumn("effective_from_date_time", current_ts)
                .withColumn("effective_to_date_time", lit(datetime.datetime(9999, 12, 31, 23, 59, 59)))
                .withColumn("is_current", lit(True))
                .withColumn("is_deleted", lit(False)))
    
    def upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None:
        """
        Performs an upsert operation (merge) on a Delta table based on the provided configuration.
        This function supports SCD2 and auditing with specified columns.

        :param config: The table configuration. It should include the following keys:
            'table_name': The name of the table to upsert.
            'dataframe': The dataframe to upsert into the table.
            'composite_columns': A list of columns to use for creating a composite key.
            'primary_key_column': The column that forms the primary key.
        :param write_method: The method to use for saving the table ('path' or 'catalog').
        :param storage_container_endpoint: Endpoint if using 'path' write method.

        :return: None. The function performs an upsert operation on the Delta table.

        Raises:
            ValueError: If required configuration parameters are missing.
            Exception: If any error occurs during the upsert operation.

        This function performs the following steps:
        1. Confirms that the required configuration is provided, including 'table_name', 'dataframe', 'composite_columns', and 'primary_key_column'.
        2. Generates keys using a unified transformation method if a transformer is available.
        3. Attempts to access the Delta table and perform an initial load if the table is not found.
        4. If the table is not found, it creates unknown records if necessary, and combines them with the source dataframe.
        5. Adds audit columns to the dataframe.
        6. Writes the dataframe to a Delta table if it was created new.
        7. Excludes key and audit columns from change detection and generates columns for change detection.
        8. Creates unique names for the temporary views based on the table name.
        9. Creates merge conditions for the upsert operation.
        10. Identifies new records for insertion and records that need to be updated.
        11. Adds audit columns to the new and updated records.
        12. Builds the source dataframe based on the existence of new and updated records.
        13. Performs the expiry upsert operation, updating existing records as needed.
        14. Performs the insert upsert operation, inserting new records as needed.

        Example:
            config = {
                'table_name': 'target_table',
                'dataframe': spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                'composite_columns': ['ID', 'First Name'],
                'upsert_type': 'scd2',
                'primary_key_column': 'primary_key',
                'add_composite_key': True
            }
            write_method = 'path'
            storage_container_endpoint = None
            upsert_to_table(config, write_method, storage_container_endpoint)
        """
        # Confirm required configuration provided
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            composite_columns = config['composite_columns']
            primary_key_column = config.get('primary_key_column')
            add_composite_key = config.get('add_composite_key', False)

            # Set composite_key_column to None by default
            composite_key_column = None
            
            # Assign composite_key_column if composite_columns provided
            if composite_columns and add_composite_key == True:
                # Set composite key column name
                composite_key_column = primary_key_column.replace("_key", "_composite_key")

        except KeyError as e:
            raise ValueError(f"Configuration must include 'table_name', 'dataframe', 'composite_columns', and 'primary_key_column': {str(e)}'")

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

            # If not date table generate unknown record
            try:
                if 'date' not in table_name.lower():
                    df_unknown_records = self.insert_unknown_record(df_source)
            except Exception as e:
                self.logger.info(f'Creation of unknown record for table {table_name} failed with error: {str(e)}')
                raise
            
            # Combine unknown record with source dataframe
            try:
                df_source = df_source.union(df_unknown_records)
            except Exception as e:
                self.logger.info(f'Union of unknown record for table {table_name} failed with error: {str(e)}')
                raise

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
                self.logger.info(f'Writing of initial load for table {table_name} failed with error: {str(e)}')
                raise

            self.logger.info(f'Initial load for table {table_name} complete. Exiting upsert operation.')
            return
        
        # Generate field exclusions for the merge operation
        try:
            # Exclude key columns from change detection
            key_columns = [primary_key_column, composite_key_column]
            audit_columns = ['inserted_date_time', 'updated_date_time', 'effective_from_date_time', 'effective_to_date_time', 'is_current', 'is_deleted']
            exclude_columns = composite_columns + key_columns + audit_columns

            # Get columns for change detection
            change_detection_columns = [col for col in df_source.columns if col not in exclude_columns]
        except Exception as e:
            self.logger.info(f'Exclusion of audit and key columns for table {table_name} failed with error: {str(e)}')
            raise

        # Generate unique names for the temporary views based on table name and a unique identifier
        temp_table_name = table_name.replace('.', '_')
        source_view_name = f'{temp_table_name}_source'
        target_view_name = f'{temp_table_name}_target'
        
        # Create merge conditions
        match_condition = ' AND '.join([f'target.{col} = source.{col}' for col in composite_columns]) + ' AND target.is_current = true'
        update_condition = ' OR '.join([f'target.{col} != source.{col}' for col in change_detection_columns])
        insert_condition = " AND ".join([f"source.{key} = target.{key}" for key in composite_columns])
        
        # Convert DeltaTable to DataFrame and register both DataFrames as temporary views with unique names
        df_source.createOrReplaceTempView(source_view_name)
        deltaTable.toDF().createOrReplaceTempView(target_view_name)

        # Identify new records for insertion and changed records for expiry
        sql_query_new_records = f"""
        SELECT source.*
        FROM {source_view_name} AS source
        LEFT ANTI JOIN {target_view_name} AS target
        ON {insert_condition}
        """

        # Execute the query to identify new records
        df_new_records = self.spark.sql(sql_query_new_records)
        df_new_records = self.add_insert_audit_columns(df_new_records)

        # Identify records that need to be updated
        sql_query_updates = f"""
        SELECT source.*
        FROM {source_view_name} AS source
        INNER JOIN {target_view_name} AS target
        ON {insert_condition}
        WHERE target.is_current = true AND ({update_condition})
        """

        # Execute the query to identify records that need to be updated
        df_update_records = self.spark.sql(sql_query_updates)
        df_update_records = self.add_update_audit_columns(df_update_records)

        # Build the merge operation
        try:
            # Initialize variables to hold table existence flags
            new_records = False
            update_records = False

            # Check if the new records table exists
            try:
                df_new_records.count() > 0
                new_records = True
            except Exception:
                self.logger.info(f'No new records to load.')

            # Check if the update records table exists
            try:
                df_update_records.count() > 0
                update_records = True
                
            except Exception:
                self.logger.info(f'No update records to load.')
        
            # Build the source DataFrame based on table existence
            try:
                # Determine action based on table existence
                if new_records and update_records:
                    # If both tables exist, perform a union of the two tables
                    df_source = df_new_records.union(df_update_records)
                elif new_records:
                    # If only the new records table exists, load data from it
                    df_source = df_new_records
                elif update_records:
                    # If only the update records table exists, load data from it
                    df_source = df_update_records
                else:
                    # If neither table exists, log and return
                    self.logger.info(f'No new or updated records found for table {table_name}. Exiting upsert operation.')
                    return
            except Exception as e:
                self.logger.info(f'Building source DataFrame for table {table_name} failed with error: {str(e)}')
                raise

            # Checkpoint DataFrames before merge
            self.spark.sparkContext.setCheckpointDir('tmpdir')
            df_source = df_source.checkpoint()

            # Perform the expiry upsert operation
            try:
                # Set record expiration expression
                current_ts = current_timestamp()
                expire_expr = {
                    'is_current': 'false',
                    'effective_to_date_time': current_ts,
                    'updated_date_time': current_ts
                }
                
                # Set expiry merge operation
                expiry_merge_operation = deltaTable.alias('target').merge(
                    source=df_source.alias('source'),
                    condition=match_condition
                ).whenMatchedUpdate(
                    condition=update_condition, 
                    set=expire_expr
                )

                # Execute the merge operation
                expiry_merge_operation.execute()
            except Exception as e:
                self.logger.info(f'Expiry operation for table {table_name} failed with error: {str(e)}')
                raise

            # Perform the insert upsert operation
            try:
                # Set merge operation to insert new records
                insert_merge_operation = deltaTable.alias('target').merge(
                    source=df_source.alias('source'),
                    condition=match_condition
                ).whenNotMatchedInsertAll()

                # Execute the merge operation
                insert_merge_operation.execute()
            except Exception as e:
                self.logger.info(f'Insert operation for table {table_name} failed with error: {str(e)}')
                raise
            
            print(f'Upsert operation for table {table_name} complete.')
        except Exception as e:
            self.logger.info(f'Upsert operation for table {table_name} failed with error: {str(e)}')
            raise