from .upsert_strategy import UpsertStrategy
from pyspark.sql.functions import lit
from pyspark.sql import Row
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
    
    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default'):
        """
        Performs an upsert operation (merge) on a Delta table based on the provided configuration.
        This function supports SCD2, deletes, and auditing with specified columns.
        
        Args:
            config (dict): Configuration for upserting a Delta table including 'table_name', 'dataframe',
                        'match_keys', 'delete_unmatched', 'skey_column', and 'nkey_column'. Expects audit columns in the dataframe.
            write_method (str): The method to use for saving the table ('default' or 'abfss').
            storage_container_endpoint (str): Endpoint if using 'abfss' write method.
            
        Raises:
            Exception: If any error occurs during the upsert operation.
        """
        table_name = config['table_name']
        df_source = config['dataframe']
        match_keys = config['match_keys']
        skey_column = config.get('skey_column')
        nkey_column = config.get('nkey_column')
        current_ts = lit(datetime.datetime.now())

        try:
            # Generate keys using the unified transformation method
            if self.transformer:
                df_source = self.transformer.stage_dataframe_with_key(df_source, df_source.columns, nkey_column, match_keys, False)
                df_source = self.transformer.stage_dataframe_with_key(df_source, df_source.columns, skey_column, match_keys, True)
        except Exception as e:
            self.logger.error(f"Failed to generate keys for table {table_name}: {e}")
            raise

        try:
            # Attempt to access or create the Delta table
            try:
                deltaTable = DeltaTable.forName(self.spark, table_name)
            except Exception:
                self.logger.info(f"Table {table_name} does not exist. Creating it.")
                
                # Insert unknown record, unless it's a date dimension
                if 'date' not in table_name.lower():
                    df_unknown = self.insert_unknown_record(df_source)

                # Union df_unknown and df_source
                df_source = df_source.union(df_unknown) \
                            .withColumn("inserted_date_time", current_ts) \
                            .withColumn("updated_date_time", current_ts) \
                            .withColumn("effective_from_date_time", lit(datetime.datetime(1901, 1, 1, 23, 59, 59))) \
                            .withColumn("effective_to_date_time", lit(datetime.datetime(9999, 12, 31, 23, 59, 59))) \
                            .withColumn("is_current", lit(True)) \
                            .withColumn("is_deleted", lit(False))

                if write_method == 'default':
                    df_source.write.format("delta").saveAsTable(table_name)
                elif write_method == 'abfss':
                    if not storage_container_endpoint:
                        raise ValueError("storage_container_endpoint must be provided with 'abfss' write method")
                    df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}")
                print(f"Dimension upsert for {table_name} complete.")
                return            

            # Enhance df_source with necessary audit and SCD2 fields
            df_source = (df_source
                        .withColumn("inserted_date_time", current_ts)
                        .withColumn("updated_date_time", current_ts)
                        .withColumn("effective_from_date_time", current_ts)
                        .withColumn("effective_to_date_time", lit(datetime.datetime(9999, 12, 31, 23, 59, 59)))
                        .withColumn("is_current", lit(True))
                        .withColumn("is_deleted", lit(False)))
            
            # Generate excluded column list
            excluded_columns = match_keys + [
                "inserted_date_time",
                "updated_date_time",
                "effective_from_date_time",
                "effective_to_date_time",
                "is_current",
                "is_deleted"]
            if skey_column:  
                excluded_columns.append(skey_column)
            if nkey_column:  
                excluded_columns.append(nkey_column)

            # Get non-match key fields excluding audit fields
            non_match_keys = [col for col in df_source.columns if col not in excluded_columns]

            # Create a condition to check if any non-match key field has changed
            update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_match_keys])

            # Define match condition for the MERGE operation
            match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys]) + " AND target.is_current = true"

            # Prepare expressions for existing records and configure the merge operation
            expire_expr = {
                "is_current": lit(False),
                "effective_to_date_time": current_ts,
                "updated_date_time": current_ts
            }

            # Configure the merge operation to expire existing records where a non-key field has changed
            merge_operation = deltaTable.alias("target") \
                            .merge(df_source.alias("source"), match_condition) \
                            .whenMatchedUpdate(condition=update_condition, set=expire_expr)

            # Execute the merge operation
            merge_operation.execute()

            # Configure the merge operation to insert new records
            merge_operation = deltaTable.alias("target") \
                            .merge(df_source.alias("source"), match_condition) \
                            .whenNotMatchedInsertAll()

            # Execute the merge operation
            merge_operation.execute()

            print(f"Dimension upsert for {table_name} complete.")
            
        except Exception as e:
            self.logger.error(f"An error occurred in upsert_to_delta_table for table {table_name}: {e}")
            raise