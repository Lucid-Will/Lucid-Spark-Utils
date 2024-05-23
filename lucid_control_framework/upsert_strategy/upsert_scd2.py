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
        Inserts an unknown record into the source dataframe.
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
        Adds audit columns to the source dataframe for upsert.
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
    
    def upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'default') -> None:
        """
        Performs an upsert operation (merge) on a Delta table based on the provided configuration.
        This function supports SCD2 and auditing with specified columns.

        :param config: Configuration for upserting a Delta table including 'table_name', 'dataframe',
                    'match_keys', and 'skey_column'. Expects audit columns in the dataframe.
        :param write_method: The method to use for saving the table ('default' or 'abfss').
        :param storage_container_endpoint: Endpoint if using 'abfss' write method.

        :return: None. The function performs an upsert operation on the Delta table.

        Raises:
            Exception: If any error occurs during the upsert operation.

        Example:
            config = {
                'table_name': 'target_table',
                'dataframe': spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                'match_keys': ['ID', 'First Name'],
                'skey_column': 'skey'
            }
            write_method = 'default'
            storage_container_endpoint = None
            upsert_delta_table(config, write_method, storage_container_endpoint)
        """
        # Confirm required configuration provided
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            match_keys = config['match_keys']
            skey_column = config.get('skey_column')

            # Set nkey_column to None by default
            nkey_column = None
            
            # Assign nkey_column if match_keys provided
            if match_keys:
                # Set natural key column name
                nkey_column = skey_column.replace("key", "natural_key")

        except KeyError as e:
            raise ValueError(f"Configuration must include 'table_name', 'dataframe', 'match_keys', and 'skey_column': {str(e)}'")

        try:
            # Generate keys using the unified transformation method
            if self.transformer:
                # Stage the dataframe with keys
                df_source = self.transformer.stage_dataframe_with_keys(table_name, df_source, df_source.columns, skey_column, nkey_column, match_keys)
        except Exception as e:
            self.logger.error(f"Failed to generate keys for table {table_name}: {e}")
            raise

        # Attempt to access the Delta table and perform initial load if not found
        try:
            deltaTable = DeltaTable.forName(self.spark, table_name)
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

            # Write the dataframe to the Delta table
            try:
                if write_method == 'abfss':
                    # Check for storage container endpoint
                    if not storage_container_endpoint:
                        raise ValueError("Storage container endpoint must be provided when using 'abfss' write method.")
                    df_source.write.format('delta').mode('overwrite').save(f'abfss://{storage_container_endpoint}/Tables/{table_name}')
                elif write_method == 'default':
                    df_source.write.format('delta').mode('overwrite').saveAsTable(table_name)
                else:
                    raise ValueError("Invalid write method provided. Use 'default' or 'abfss'.")
            except Exception as e:
                self.logger.info(f'Writing of initial load for table {table_name} failed with error: {str(e)}')
                raise

            self.logger.info(f'Initial load for table {table_name} complete. Exiting upsert operation.')
            return
        
        # Generate field exclusions for the merge operation
        try:
            # Exclude key columns from change detection
            key_columns = [skey_column, nkey_column]
            audit_columns = ['inserted_date_time', 'updated_date_time', 'effective_from_date_time', 'effective_to_date_time', 'is_current', 'is_deleted']
            exclude_columns = match_keys + key_columns + audit_columns

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
        match_condition = ' AND '.join([f'target.{col} = source.{col}' for col in match_keys]) + ' AND target.is_current = true'
        update_condition = ' OR '.join([f'target.{col} != source.{col}' for col in change_detection_columns])
        insert_condition = " AND ".join([f"source.{key} = target.{key}" for key in match_keys])
        
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