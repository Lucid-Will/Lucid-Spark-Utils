#!/usr/bin/env python
# coding: utf-8

# ## Lucid_UpsertStrategy
# 
# 
# 

# In[ ]:


import datetime
from decimal import Decimal
from pyspark.sql import Row
from pyspark.sql.functions import lit
from concurrent.futures import ThreadPoolExecutor, as_completed
from delta.tables import DeltaTable
import os


# In[ ]:


class UpsertStrategy:

    def __init__(self, spark, logger, transform_manager = None):
        self.spark = spark
        self.logger = logger

        self.transformer = transform_manager

    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default'):
        raise NotImplementedError("Subclass must implement abstract method")


# In[ ]:


class UpsertToFactTable(UpsertStrategy):

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

            # Generate excluded column list
            excluded_columns = match_keys + ["inserted_date_time", "updated_date_time"]
            if skey_column:  
                excluded_columns.append(skey_column)
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

            # Execute the merge operation
            merge_operation.execute()
                
            print(f"Fact upsert for {table_name} complete.")
        except Exception as e:
            self.logger.error(f"An error occurred in upsert_to_delta_table for table {table_name}: {e}")
            raise


# In[ ]:


class UpsertSCD1Strategy(UpsertStrategy):
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
    
    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
        """
        Performs an upsert operation (merge) on a Delta table based on the provided configuration.
        This function supports SCD1, deletes, and auditing with specified columns.
        
        Args:
            config (dict): Configuration for upserting a Delta table including 'table_name', 'dataframe',
                        and 'match_keys'. Expects audit columns in the dataframe.
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
            # Determine if all data columns are part of the natural key
            all_columns_are_match_keys = set(match_keys) == set(df_source.columns)

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
            # Attempt to access or create the Delta table
            try:
                deltaTable = DeltaTable.forName(self.spark, table_name)
            except Exception:
                self.logger.info(f"Table {table_name} does not exist. Creating it.")

                # Insert unknown record, unless it's a date dimension
                if 'date' not in table_name.lower():
                    df_unknown = self.insert_unknown_record(df_source)

                    # Union df_unknown and df_source
                    df_source = df_source.union(df_unknown)
                
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

                print(f"Dimension upsert for {table_name} complete.")
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
                if skey_column:  
                    excluded_columns.append(skey_column)
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


# In[ ]:


class UpsertSCD2Strategy(UpsertStrategy):

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
    
    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
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


# In[ ]:


class UpsertGeneric(UpsertStrategy):

    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default'):
        """
        Performs an upsert operation (merge) on a Delta table based on the provided configuration.
        
        Args:
            config (dict): Configuration for upserting a Delta table including 'table_name', 'dataframe',
                        'match_keys', and optionally 'delete_unmatched'.
            write_method (str): The method to use for saving the table. Can be either 'default' or 'abfss'.
            storage_container_endpoint (str): Required if save_method is 'abfss'.
            
        Raises:
            Exception: If the target Delta table does not exist and cannot be created.
        """
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            match_keys = config['match_keys']

            # Define match condition for the MERGE operation
            match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])

            # Attempt to access or create the Delta table
            try:
                deltaTable = DeltaTable.forName(self.spark, table_name)
            except Exception:
                self.logger.info(f"Table {table_name} does not exist. Creating it.")
                if write_method == 'default':
                    df_source.write.format("delta").saveAsTable(table_name)
                elif write_method == 'abfss':
                    if storage_container_endpoint is None:
                        raise ValueError("storage_container_endpoint must be provided if save_method is 'abfss'")
                    df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}")
                deltaTable = DeltaTable.forName(self.spark, table_name)

            # Enhance df_source with audit columns
            current_ts = lit(datetime.datetime.now())
            df_source = (df_source.
                            withColumn("inserted_date_time", current_ts)
                            .withColumn("updated_date_time", current_ts))

            # Define update expressions excluding match keys
            update_expr = {col: f"source.{col}" for col in df_source.columns if col not in match_keys}

            # Initialize the merge operation
            merge_operation = deltaTable.alias("target") \
                .merge(df_source.alias("source"), match_condition) \
                .whenMatchedUpdate(set=update_expr) \
                .whenNotMatchedInsertAll()

            # Execute the merge operation
            merge_operation.execute()

        except Exception as e:
            self.logger.error(f"An error occurred while upserting into the Delta table {table_name}: {e}")
            raise


# In[ ]:


class ConcurrentUpsertHandler:
    """
    This class handles concurrent upsert operations on multiple tables.
    """

    def __init__(self, spark, logger, transform_manager=None):
        """
        Initializes the handler with a Spark session, a logger, and an optional transform manager.
        """
        self.spark = spark
        self.logger = logger
        self.transform_manager = transform_manager

    def upsert_data_concurrently(self, table_configs, storage_container_endpoint=None, write_method='default'):
        """
        Performs upsert operations concurrently on multiple tables based on the provided configurations.
        """
        def upsert_table(config):
            """
            Performs an upsert operation on a single table based on the provided configuration.
            """
            # Map of upsert types to strategy classes
            strategy_map = {
                'fact': UpsertToFactTable(self.spark, self.logger, self.transform_manager),
                'scd2': UpsertSCD2Strategy(self.spark, self.logger, self.transform_manager),
                'scd1': UpsertSCD1Strategy(self.spark, self.logger, self.transform_manager),
                'generic': UpsertGeneric(self.spark, self.logger)
            }

            # Get the strategy for the upsert type specified in the config, or use the generic strategy by default
            strategy = strategy_map.get(config.get('upsert_type', 'generic'), UpsertGeneric(self.spark, self.logger))

            # Perform the upsert operation
            strategy.upsert_to_table(config, storage_container_endpoint, write_method)

            # Log a message indicating that the upsert operation is complete
            self.logger.info(f"Upsert for {config['table_name']} completed.")

        # Create a ThreadPoolExecutor and submit the upsert_table function for each table config
        with ThreadPoolExecutor(max_workers=min(len(table_configs), (os.cpu_count() or 1) * 5)) as executor:
            futures = [executor.submit(upsert_table, config) for config in table_configs]

            # Wait for all futures to complete
            for future in as_completed(futures):
                future.result()

