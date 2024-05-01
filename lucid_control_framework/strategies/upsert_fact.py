from pyspark.sql.functions import lit
from delta.tables import DeltaTable
import datetime

class UpsertFact(UpsertStrategy):

    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default'):
        """
        Performs an upsert operation on a Delta fact table based on the provided configuration.
        
        Args:
            config (dict): Configuration for upserting a fact table including 'table_name', 'dataframe',
                        and 'match_keys'. Assumes audit columns are relevant but less complex.
            write_method (str): The method to use for saving the table ('default' or 'abfss').
            storage_container_endpoint (str): Endpoint if using 'abfss' write method.
            key_column (str): The name of the surrogate key column. If provided, surrogate keys will be generated.
        """
        table_name = config['table_name']
        df_source = config['dataframe']
        match_keys = config['match_keys']
        skey_column = config.get('skey_column')
        nkey_column = config.get('nkey_column')
        current_ts = lit(datetime.datetime.now())

        try:
            # If a transformer is provided, generate surrogate and natural keys
            if self.transformer and nkey_column:
                # Use key_column as the new_column and match_keys as match_key_columns
                df_source = self.transformer.stage_dataframe_with_natural_key(df_source, df_source.columns, nkey_column, match_keys)
                
            # If a transformer is provided, generate surrogate and natural keys
            if self.transformer and skey_column:
                # Use key_column as the new_column and match_keys as match_key_columns
                df_source = self.transformer.stage_dataframe_with_surrogate_key(df_source, df_source.columns, skey_column, match_keys)
        except Exception:
            self.logger.info(f"Creation of key fields for table {table_name} failed.")
            raise
            
        try:
            # Enhance df_source with necessary audit fields
            df_source = (df_source
                        .withColumn("inserted_date_time", current_ts)
                        .withColumn("updated_date_time", current_ts))

            # Try to access the Delta table, create if it does not exist
            try:
                deltaTable = DeltaTable.forName(self.spark, table_name)
            except Exception:
                self.logger.info(f"Fact table {table_name} does not exist. Creating it.")
                
                # Choose the write method based on the provided arguments
                if write_method == 'default':
                    df_source.write.format("delta").saveAsTable(table_name)

                elif write_method == 'abfss':
                    if not storage_container_endpoint:
                        raise ValueError("storage_container_endpoint is required for 'abfss' write method")
                    df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}")

                print(f"Fact upsert for {table_name} complete.")
                return

            # Define match condition for the MERGE operation
            match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])

            # Check delete logic
            has_is_deleted = 'is_deleted' in df_source.columns
            
            # Generate excluded column list
            excluded_columns = match_keys + ["inserted_date_time", "updated_date_time"]
            if skey_column:
                excluded_columns.append(skey_column)
            if nkey_column:
                excluded_columns.append(nkey_column)
            if has_is_deleted:
                excluded_columns.append("is_deleted")

            # Get non-match key fields excluding audit fields
            non_match_keys = [col for col in df_source.columns if col not in excluded_columns]

            # Create a condition to check if any non-match key field has changed
            update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_match_keys])

            # Configure the merge operation to update existing records and insert new records
            merge_operation = deltaTable.alias("target") \
                .merge(df_source.alias("source"), match_condition)

            # Determine if deletes are required
            if has_is_deleted:
                merge_operation = merge_operation.whenMatchedDelete(condition="source.is_deleted = 'true'")

            # Configure conditions based on the existence of 'is_deleted'
            if has_is_deleted:
                update_condition = f"source.is_deleted <> 'true' AND ({update_condition})"
            else:
                update_condition = update_condition

            merge_operation = merge_operation.whenMatchedUpdate(
                condition=update_condition,
                set={field: f"source.{field}" for field in non_match_keys} | {"updated_date_time": "source.updated_date_time"}) \
                .whenNotMatchedInsertAll()

            # Execute the merge operation
            merge_operation.execute()
                
            print(f"Fact upsert for {table_name} complete.")
        except Exception as e:
            self.logger.error(f"An error occurred in upsert_to_delta_table for table {table_name}: {e}")
            raise