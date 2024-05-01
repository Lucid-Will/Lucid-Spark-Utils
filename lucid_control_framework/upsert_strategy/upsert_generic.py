from .upsert_strategy import UpsertStrategy
from pyspark.sql.functions import lit
from delta.tables import DeltaTable
import datetime

class UpsertGeneric(UpsertStrategy):

 def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default'):
     """
     Performs an upsert operation (merge) on a Delta table based on the provided configuration.
     
     Args:
         config (dict): Configuration for upserting a Delta table including 'table_name', 'dataframe',
                     'match_keys', and optionally 'delete_unmatched'.
         write_method (str): The method to use for saving the table. Can be either 'default' or 'abfss'.
         storage_container_endpoint (str): Required if save_method is 'abfss'.
         delete_unmatched (bool): Indicates whether records not present in the source DataFrame should be deleted.
         
     Raises:
         Exception: If the target Delta table does not exist and cannot be created.
     """ 
     table_name = config['table_name']
     df_source = config['dataframe']
     match_keys = config['match_keys']
     nkey_column = config.get('nkey_column')
     current_ts = lit(datetime.datetime.now())

     try:
         # Determine if all data columns are part of the natural key
         all_columns_are_match_keys = set(match_keys) == set(df_source.columns)

         # If a transformer is provided, generate surrogate and natural keys
         if self.transformer and nkey_column:
             # Use key_column as the new_column and match_keys as match_key_columns
             df_source = self.transformer.stage_dataframe_with_natural_key(df_source, df_source.columns, nkey_column, match_keys)
         
     except Exception:
         self.logger.info(f"Creation of key fields for table {table_name} failed.")
         raise

     try:
         # Attempt to access or create the Delta table
         try:
             deltaTable = DeltaTable.forName(self.spark, table_name)
         except Exception:
             self.logger.info(f"Table {table_name} does not exist. Creating it.")

             # Add audit columns
             df_source = df_source \
                     .withColumn("inserted_date_time", current_ts) \
                     .withColumn("updated_date_time", current_ts)

             if write_method == 'default':
                 df_source.write.format("delta").saveAsTable(table_name)

             elif write_method == 'abfss':
                 if not storage_container_endpoint:
                     raise ValueError("storage_container_endpoint must be provided with 'abfss' write method")
                 df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}")

             print(f"Upsert for {table_name} complete.")
             return            

         # Enhance df_source with necessary audit fields
         df_source = (df_source
                     .withColumn("inserted_date_time", current_ts)
                     .withColumn("updated_date_time", current_ts))
         
         # Define match condition for the MERGE operation
         match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])

         # Begin conditional merge
         if not all_columns_are_match_keys:
             # Generate excluded column list
             excluded_columns = match_keys + ["inserted_date_time", "updated_date_time"]
             if nkey_column:  
                 excluded_columns.append(nkey_column)

             # Get non-match key fields excluding audit fields
             non_match_keys = [col for col in df_source.columns if col not in excluded_columns]
             
             # Create a condition to check if any non-match key field has changed
             update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_match_keys])

             # Configure the merge operation to update existing records and insert new records
             merge_operation = deltaTable.alias("target") \
                 .merge(df_source.alias("source"), match_condition) \
                 .whenMatchedUpdate(condition=update_condition, 
                                 set={field: f"source.{field}" for field in non_match_keys} | {"updated_date_time": "source.updated_date_time"}) \
                 .whenNotMatchedInsertAll()
         else:
             # Alternate merge logic: Insert new rows where there's no match, do nothing for matches.
             merge_operation = deltaTable.alias("target") \
                 .merge(
                     source = df_source.alias("source"), 
                     condition = match_condition
                 ) \
                 .whenNotMatchedInsertAll()
         
         # Execute the merge operation
         merge_operation.execute()

         print(f"Dimension upsert for {table_name} complete.")
         
     except Exception as e:
         self.logger.error(f"An error occurred in upsert_to_delta_table for table {table_name}: {e}")
         raise