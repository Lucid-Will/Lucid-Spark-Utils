from lucid_spark_utils.transformation_manager import TransformationManager
from lucid_spark_utils.delta_table_manager import DeltaTableManager
from pyspark.sql.functions import lit, col, when, concat_ws, abs, hash
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from typing import Optional, List
import logging
import datetime

class Validation:
    """
    This class is used for validating and testing data in Spark.
    It includes methods for logging table validation results.
    """

    def __init__(self, spark, logger=None):
        """
        Initialize the Validation object.

        :param transform_manager: TransformManager object
        """
        self.spark = spark
        self.logger = logger if logger else logging.getLogger(__name__)
        self.transform_manager = TransformationManager(self.spark, self.logger)
        self.table_manager = DeltaTableManager(self.spark, self.logger)

    def log_table_validation(
            self,
            target_table_name: str,
            log_table_name: str,
            read_method: str = 'catalog',
            write_method: str = 'catalog',
            target_storage_container_endpoint: Optional[str] = None,
            log_storage_container_endpoint: Optional[str] = None,
            primary_key_column: Optional[str] = None,
            stage_count: Optional[int] = None,
            invalid_count: Optional[int] = None,
            duplicate_count: Optional[int] = None,
            delete_count: Optional[int] = None
        ) -> None:
        """
        Log the validation results for a table.

        :param target_table_name: The name of the target table.
        :param log_table_name: The name of the log table.
        :param read_method: The method used to read the target table (catalog or path).
        :param write_method: The method used to write the log table (catalog or path).
        :param target_storage_container_endpoint: The storage container endpoint of the target table.
        :param log_storage_container_endpoint: The storage container endpoint of the log table.
        :param primary_key_column: The primary key column used for filtering.
        :param stage_count: The count of rows in the staging DataFrame.
        :param invalid_count: Count of invalid records.
        :param duplicate_count: Count of duplicate records.
        :param delete_count: Count of records flagged for deletion.

        :return: None. The function logs the validation results into the specified log table.

        Example:
            log_table_validation(
                "storage_container_endpoint", 
                "my_table", 
                "log_storage_container_endpoint", 
                "log_table", 
                "id", 
                100, 
                5, 
                10, 
                3
            )

        This function performs the following steps:
        1. Determines if the 'is_current' column exists in the target table for count validation.
        2. Reads the target table and filters current records if the 'is_current' column exists.
        3. Filters unknown records based on the primary key column.
        4. Captures row counts for staging, target table, invalid records, duplicate records, and delete flagged records.
        5. Defines the logging schema and creates a DataFrame with validation data.
        6. Adds a timestamp column to the validation DataFrame.
        7. Checks if the log table exists and creates it if not.
        8. Creates a surrogate key column in the validation DataFrame.
        9. Writes the validation DataFrame to the log table.
        """
        try:
            # Determine if the 'is_current' column exists in the target table for count validation
            table_schema = self.table_manager.read_delta_table(target_table_name, target_storage_container_endpoint, read_method).schema
            columns = [field.name for field in table_schema]
            
            # Set target records
            target_records = self.table_manager.read_delta_table(target_table_name, target_storage_container_endpoint, read_method)
            
            # Filter current records
            if 'is_current' in columns:
                target_records = target_records.filter((col("is_current") == 1))
            
            # Filter unknown records
            if primary_key_column:
                target_records = target_records.filter((col(primary_key_column).cast('int') != -1))
            
            # Capture row counts for staging, target table, invalid records, duplicate records, and delete flagged records
            stage_count = stage_count if stage_count is not None else 0
            invalid_count = invalid_count if invalid_count is not None else 0
            duplicate_count = duplicate_count if duplicate_count is not None else 0
            delete_count = delete_count if delete_count is not None else 0
            target_count = target_records.count() if target_records is not None else 0

            # Define logging schema
            schema = StructType([
                StructField("table_name", StringType(), True),
                StructField("stage_count", IntegerType(), True),
                StructField("target_count", IntegerType(), True),
                StructField("invalid_count", IntegerType(), True),
                StructField("duplicate_count", IntegerType(), True),
                StructField("delete_count", IntegerType(), True)
            ])

            # Extract the table name without the schema prefix
            table_name_only = target_table_name.split('.')[-1]

            # Create DataFrame with the extracted table name
            df_validation = self.spark.createDataFrame([(table_name_only, stage_count, target_count, invalid_count, duplicate_count, delete_count)], schema)

            # Get current timestamp
            current_ts = current_timestamp()

            # Add timestamp column
            df_validation = df_validation.withColumn("log_timestamp", lit(current_ts))

            # Attempt to access the Delta table
            try:
                if write_method == 'catalog':
                    DeltaTable.forName(self.spark, log_table_name)
                elif write_method == 'path':
                    log_table_path = f'{log_storage_container_endpoint}/Tables/{log_table_name}'
                    DeltaTable.forPath(self.spark, log_table_path)
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Construct log table path
                if write_method == 'catalog':
                    create_table = f'CREATE TABLE {log_table_name}'
                elif write_method == 'path':
                    log_table_path = f'{log_storage_container_endpoint}/Tables/{log_table_name}'
                    create_table = f'CREATE TABLE delta.`{log_table_path}`'
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
                    
                # Create logging table
                self.spark.sql(f"""
                    {create_table} (
                        validation_key BIGINT,
                        table_name VARCHAR(150),
                        stage_count INT,
                        target_count INT,
                        invalid_count INT,
                        duplicate_count INT,
                        delete_count INT,
                        log_timestamp TIMESTAMP
                    ) USING delta
                """)
                self.logger.info(f"Log table {log_table_name} has been created.")

            # Define key column
            primary_key_column = "validation_key"

            # Capture dataframe columns
            columns = df_validation.columns

            # Create surrogate key column
            df_validation = df_validation.withColumn(
                primary_key_column, 
                abs(hash(concat_ws("_", lit(target_table_name), lit(str(current_ts))))).cast(LongType())
            ).select(primary_key_column, *columns)

            # Write to log table
            self.table_manager.write_delta_table(
                df_validation, 
                log_table_name, 
                log_storage_container_endpoint, 
                write_method, 
                'append', 
                'true'
            )

            self.logger.info(f"Validation log for table {target_table_name} has been successfully created.")
        except Exception as e:
            self.logger.error(f"Failed to create validation log for table {target_table_name}. Error: {str(e)}")
            raise e
        
    def data_validation_check(
            self, 
            df_stage: DataFrame, 
            target_table_name: str,
            target_storage_container_endpoint: str,
            composite_columns: List[str], 
            read_method: str = 'catalog',
            write_method: str = 'catalog',
            primary_key_column: Optional[str] = None, 
            dropped_validation_columns: Optional[List[str]] = None
        ) -> None:
        """
        Identify invalid, duplicate, and delete flagged records by identifying them,
        saving them to specified paths, and returning a filtered DataFrame along with counts of invalid, duplicate, and delete flagged records.

        :param df_stage: The staging DataFrame.
        :param target_table_name: The name of the table being processed and the target table to check for delete flagged records.
        :param target_storage_container_endpoint: The endpoint for the storage account.
        :param composite_columns: List of columns to check for invalid values and duplicates and form the composite key.
        :param read_method: The method used to read the target table (catalog or path).
        :param write_method: The method used to write the log table (catalog or path).
        :param primary_key_column: The primary key column used for identifying records in the target table.
        :param dropped_validation_columns: List of columns to drop from the final DataFrame after validation.

        :return: A tuple containing the filtered DataFrame, count of invalid records, count of duplicate records, and count of delete flagged records.

        Example:
            data_validation_check(df_stage, "my_table", ["id", "name"], "mydatalake", "id", ["created_at", "updated_at"])

        This function performs the following steps:
        1. Gets the current date and time for the paths.
        2. Defines paths for invalid, duplicate, and delete flagged records.
        3. Checks each key column individually for -1 to flag invalid records.
        4. Separates invalid records and writes them to a CSV file.
        5. Groups by the key columns to identify duplicates and writes them to a CSV file.
        6. Identifies records in the target table that are flagged for deletion and writes them to a CSV file.
        7. Combines all validation DataFrames for writing to a Delta table.
        8. Writes combined validation DataFrames to a Delta table.
        9. Drops the specified validation columns from the final DataFrame and returns the final valid records.
        """
        try:
            # Get the current date and time for the paths
            now = datetime.datetime.now()

            # Extract the desired date components
            year = now.strftime('%Y')
            month = now.strftime('%m')
            day = now.strftime('%d')
            timestamp = now.strftime('%H%M%S')

            # Define paths
            base_path = f'{target_storage_container_endpoint}/Files/Data_Validation/{target_table_name}/{year}/{month}/{day}'
            invalid_records_path = f'{base_path}/invalid_records/{timestamp}.csv'
            duplicate_records_path = f'{base_path}/duplicate_records/{timestamp}.csv'
            delete_records_path = f'{base_path}/delete_flagged_records/{timestamp}.csv'

            # Check each key column individually for -1
            invalid_condition = col(composite_columns[0]) == -1
            for column in composite_columns[1:]:
                invalid_condition = invalid_condition | (col(column) == -1)

            # Add a column to the DataFrame to flag invalid records
            df_stage = df_stage.withColumn('is_invalid', when(invalid_condition, True).otherwise(False))

            # Separate the invalid records
            invalid_records = df_stage.filter(col('is_invalid') == True).drop('is_invalid')
            valid_records = df_stage.filter(col('is_invalid') == False).drop('is_invalid')

            # Get the count of invalid records
            invalid_records_count = invalid_records.count()

            # Check for invalid records and write them to a CSV file
            if invalid_records_count > 0:
                invalid_records = invalid_records.withColumn('composite_key', concat_ws('_', *composite_columns))
                        
                # Identify timestamp and date columns
                timestamp_columns = [field.name for field in invalid_records.schema.fields if isinstance(field.dataType, (TimestampType))]

                # Convert identified columns to string
                for column in timestamp_columns:
                    invalid_records = invalid_records.withColumn(column, col(column).cast('string'))

                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

                # Write invalid records to CSV
                invalid_records_df = invalid_records.toPandas()
                invalid_records_df.to_csv(invalid_records_path, index=False)
                self.logger.info(f'Invalid records have been saved to {invalid_records_path}.')

                # Re-enable Arrow for further processing
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

            # Cache the valid records for efficient duplicate handling
            valid_records.cache()

            # Group by the key columns and count the occurrences to identify duplicates
            df_composite = valid_records.select(*composite_columns)
            df_duplicates = df_composite.groupBy(composite_columns).count().filter(col('count') > 1)

            # Join with the valid records to get the duplicate records
            duplicate_records = valid_records.join(df_duplicates, on=composite_columns, how='inner').drop('count')

            # Get the count of duplicate records
            duplicate_records_count = duplicate_records.count()

            # Check for duplicate records and write them to a CSV file
            if duplicate_records_count > 0:
                duplicate_records = duplicate_records.withColumn('composite_key', concat_ws('_', *composite_columns))
                
                # Identify timestamp and date columns
                timestamp_columns = [field.name for field in duplicate_records.schema.fields if isinstance(field.dataType, (TimestampType))]

                # Convert identified columns to string
                for column in timestamp_columns:
                    duplicate_records = duplicate_records.withColumn(column, col(column).cast('string'))

                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                
                # Write duplicate records to CSV
                duplicate_records_df = duplicate_records.toPandas()
                duplicate_records_df.to_csv(duplicate_records_path, index=False)
                self.logger.info(f'Duplicate records have been saved to {duplicate_records_path}.')

                # Re-enable Arrow for further processing
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

            # Filter out the duplicate records from the valid records
            final_valid_records = valid_records.join(df_duplicates, on=composite_columns, how='left_anti')
            valid_records_count = final_valid_records.count()

            # Create composite key for both DataFrames
            final_valid_records = final_valid_records.withColumn('composite_key', concat_ws('_', *composite_columns))

            # Set delete records DataFrame to None
            delete_records_count = 0

            # If target table doesn't exist skip delete flagged records handling
            try:
                DeltaTable.forName(self.spark, target_table_name)
                
                # Identify records in the target table that are flagged for deletion
                df_target = self.table_manager.read_delta_table(
                    target_table_name, 
                    target_storage_container_endpoint, 
                    read_method
                )

                # Filter out unknown records
                if primary_key_column:
                    df_target = df_target.filter(col(primary_key_column) != -1)

                df_target = df_target.withColumn('composite_key', concat_ws('_', *composite_columns))

                # Identify records in the target table that are flagged for deletion
                delete_records = df_target.join(
                    final_valid_records,
                    on='composite_key',
                    how='left_anti'
                )

                # Get the count of records flagged for deletion
                delete_records_count = delete_records.count()
            
                if delete_records_count > 0:
                    # Identify timestamp and date columns
                    timestamp_columns = [field.name for field in delete_records.schema.fields if isinstance(field.dataType, (TimestampType))]

                    # Convert identified columns to string
                    for column in timestamp_columns:
                        delete_records = delete_records.withColumn(column, col(column).cast('string'))

                    # Temporarily disble Arrow for writing to CSV
                    self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                    
                    # Write delete flagged records to CSV
                    df_delete_records = delete_records.toPandas()
                    df_delete_records.to_csv(delete_records_path, index=False)
                    self.logger.info(f'Records flagged for deletion have been saved to {delete_records_path}.')

                    # Re-enable Arrow for further processing
                    self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            except Exception as e:
                    self.logger.info(f"Target table {target_table_name} does not exist. Skipping delete flagged records handling.")
                    delete_records = None

            # Add record_type column to all validation DataFrames
            df_invalid = invalid_records.withColumn('record_type', lit('invalid')) if invalid_records_count > 0 else None
            df_duplicate = duplicate_records.withColumn('record_type', lit('duplicate')) if duplicate_records_count > 0 else None
            df_delete = delete_records.withColumn('record_type', lit('delete')) if delete_records_count > 0 else None

            # Combine all validation DataFrames for writing to a Delta table
            df_combined = None

            # Combine all validation DataFrames
            for df in [df_invalid, df_duplicate, df_delete]:
                if df is not None:
                    if df_combined is None:
                        df_combined = df
                    else:
                        df_combined = df_combined.unionByName(df, allowMissingColumns=True)

            # Extract the base name of the target table without the schema
            validation_target_table = f'{target_table_name}_validation'

            # Write combined validation DataFrames to a Delta table
            if df_combined is not None:
                self.table_manager.write_delta_table(
                    df_combined, 
                    validation_target_table, 
                    target_storage_container_endpoint, 
                    write_method, 
                    'overwrite', 
                    'true'
                )

            # Drop the specified validation columns from the final DataFrame
            if dropped_validation_columns:
                final_valid_records = final_valid_records.drop(*dropped_validation_columns)
            final_valid_records = final_valid_records.drop('composite_key')

            return final_valid_records, invalid_records, duplicate_records, delete_records, valid_records_count, invalid_records_count, duplicate_records_count, delete_records_count

        except Exception as e:
            self.logger.error(f'Failed to handle invalid, duplicate, and delete flagged records. Error: {str(e)}')
            raise e
    
    def hard_delete_records(
            self,
            target_table_name: str,
            primary_key_column: str,
            write_method: str = 'catalog',
            target_table_storage_container_endpoint: Optional[str] = None,
            df_delete=None
        ):
        """
        Perform hard deletes from the target table for records identified as delete.

        :param target_table_name: The name of the target table.
        :param primary_key_column: The primary key column used for identifying records in the target table.
        :param write_method: The method used to write the log table (catalog or path).
        :param target_table_storage_container_endpoint: The storage container endpoint of the target table.
        :param df_delete: DataFrame of records flagged for deletion.

        Example:
            hard_delete_records('schema.my_table', 'id', 'path', 'target_table_storage_container_endpoint', df_delete)

        This function performs the following steps:
        1. Checks if df_delete is None or empty.
        2. Converts the DataFrame of delete flagged records to a list of primary keys.
        3. Constructs the delete condition based on primary keys.
        4. Constructs the path of the target table if using the 'path' write method.
        5. Executes the delete statement on the target table.
        """
        try:
            # Check if df_delete is None or empty
            if df_delete is None or df_delete.count() == 0:
                self.logger.info("No records flagged for deletion. Skipping hard delete.")
                return
            
            # Convert DataFrame of delete flagged records to a list of primary keys
            delete_keys = df_delete.select(primary_key_column).distinct().collect()
            delete_keys = [row[primary_key_column] for row in delete_keys]

            # Perform hard delete on target table based on primary keys
            condition = " OR ".join([f"{primary_key_column} = '{key}'" for key in delete_keys])

            # Construct log table path
            if write_method == 'catalog':
                # Build and execute the delete statement
                self.spark.sql(f"DELETE FROM {target_table_name} WHERE {condition}")
            elif write_method == 'path':
                # Construct the absolute path of the target table
                target_table_path = f'{target_table_storage_container_endpoint}/Tables/{target_table_name}'
                
                # Build and execute the delete statement
                self.spark.sql(f"DELETE FROM delta.`{target_table_path}` WHERE {condition}")
            else:
                raise ValueError(f'Invalid write method: {write_method}')

            self.logger.info(f"Successfully performed hard deletes on {target_table_name} for records flagged as delete.")
        except Exception as e:
            self.logger.error(f"Failed to perform hard deletes on {target_table_name}. Error: {str(e)}")
            raise e
        
    def soft_delete_records(
            self,
            target_table_name: str,
            primary_key_column: str,
            read_method: str = 'catalog',
            target_table_storage_container_endpoint: Optional[str] = None,
            df_delete=None
            ):
        """
        Perform soft deletes from the target table for records identified as delete flagged by setting the is_deleted column to True.
        If the is_deleted column does not exist, it will be added to the target table.

        :param target_table_storage_container_endpoint: The storage container endpoint of the target table.
        :param target_table_name: The name of the target table.
        :param primary_key_column: The primary key column used for identifying records in the target table.
        :param df_delete: DataFrame of records flagged for deletion.

        Example:
            soft_delete_records("storage_container_endpoint", "my_table", "id", df_delete)

        This function performs the following steps:
        1. Checks if df_delete is None or empty.
        2. Converts the DataFrame of delete flagged records to a list of primary keys.
        3. Reads the Delta table based on the provided read method.
        4. Checks if the is_deleted column exists in the target table.
        5. Adds the is_deleted column to the target table if it does not exist.
        6. Creates a condition for updating the is_deleted column.
        7. Updates the target table to set is_deleted to True for the identified records.
        """
        try:
            # Check if df_delete is None or empty
            if df_delete is None or df_delete.count() == 0:
                self.logger.info("No records flagged for deletion. Skipping soft delete.")
                return

            # Convert DataFrame of delete flagged records to a list of primary keys
            delete_keys = df_delete.select(primary_key_column).distinct().collect()
            delete_keys = [row[primary_key_column] for row in delete_keys]

            # Perform soft delete on target table based on primary keys
            delta_table = self.table_manager.read_delta_table(
                target_table_name, 
                target_table_storage_container_endpoint, 
                read_method
            )

            # Check if the is_deleted column exists
            if 'is_deleted' not in delta_table.columns:
                # Add is_deleted column to the target table with default value False
                delta_table.update(
                    condition="true",
                    set={"is_deleted": lit(False)}
                )
                self.logger.info(f"Added is_deleted column to {target_table_name}.")

            # Create condition for updating the is_deleted column
            condition = " OR ".join([f"{primary_key_column} = '{key}'" for key in delete_keys])

            # Update the target table to set is_deleted to True for the identified records
            delta_table.update(
                condition=condition,
                set={"is_deleted": lit(True)}
            )

            self.logger.info(f"Successfully performed soft deletes on {target_table_name} for records flagged as delete.")
        except Exception as e:
            self.logger.error(f"Failed to perform soft deletes on {target_table_name}. Error: {str(e)}")
            raise e