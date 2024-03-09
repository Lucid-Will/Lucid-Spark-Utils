#!/usr/bin/env python
# coding: utf-8

# ## Lucid_UpsertStrategy
# 
# 
# 

# In[ ]:


import logging
import datetime
from decimal import Decimal
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import current_timestamp, lit
from concurrent.futures import ThreadPoolExecutor, as_completed
from delta.tables import DeltaTable
import os


# In[ ]:


class UpsertStrategy:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
        raise NotImplementedError("Subclass must implement abstract method")


# In[ ]:


class UpsertToFactTable(UpsertStrategy):

    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
        """
        Performs an upsert operation on a Delta fact table based on the provided configuration.
        
        Args:
            config (dict): Configuration for upserting a fact table including 'table_name', 'dataframe',
                        and 'match_keys'. Assumes audit columns are relevant but less complex.
            write_method (str): The method to use for saving the table ('default' or 'abfss').
            storage_container_endpoint (str): Endpoint if using 'abfss' write method.
        """
        try:
            table_name = config['table_name']
            df_source = config['dataframe']
            match_keys = config['match_keys']

            # Define match condition for the MERGE operation based on the unique identifiers or keys
            match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])

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
                deltaTable = DeltaTable.forName(self.spark, table_name)

            # Enhance df_source with audit columns
            current_ts = current_timestamp()
            df_source = df_source.withColumn("inserted_date_time", current_ts).withColumn("updated_date_time", current_ts)

            # Define update expression for upsert operation
            update_expr = {col: f"source.{col}" for col in df_source.columns if col not in match_keys}

            # Configure the merge operation for fact table upserts
            merge_operation = deltaTable.alias("target") \
                .merge(df_source.alias("source"), match_condition) \
                .whenMatchedUpdate(set=update_expr) \
                .whenNotMatchedInsertAll()

            # Execute the merge operation
            merge_operation.execute()
        except Exception as e:
            self.logger.error(f"An error occurred in upsert_to_delta_table for table {table_name}: {e}")
            raise


# In[ ]:


class UpsertSCD1Strategy(UpsertStrategy):
    def insert_unknown_record(self, df_source, table_name):
        """
        Inserts an unknown record into the source dataframe.
        """
        unknown_record = {col: None for col in df_source.columns}
        unknown_record.update({
            "inserted_date_time": datetime.datetime.now(),
            "updated_date_time": datetime.datetime.now(),
        })

        # Capture key field
        key_field = df_source.columns[0]
        
        # Set the key field based on the table name
        if 'date' in table_name.lower():
            unknown_record[key_field] = 19010101
        else:
            unknown_record[key_field] = -1

        return df_source.union(spark.createDataFrame([unknown_record]))
    
    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
        table_name = config['table_name']
        df_source = config['dataframe']
        match_keys = config['match_keys']

        try:
            # Enhance df_source with necessary audit fields
            current_ts = lit(datetime.datetime.now())
            df_source = (df_source
                        .withColumn("inserted_date_time", current_ts)
                        .withColumn("updated_date_time", current_ts))

            # Insert unknown record, unless it's a date dimension
            if 'date' not in table_name.lower():
                df_source = self.insert_unknown_record(df_source, table_name)

            # Attempt to access or create the Delta table
            try:
                deltaTable = DeltaTable.forName(self.spark, table_name)
            except Exception:
                self.logger.info(f"Table {table_name} does not exist. Creating it.")
                if write_method == 'default':
                    df_source.write.format("delta").saveAsTable(table_name)
                elif write_method == 'abfss':
                    if not storage_container_endpoint:
                        raise ValueError("storage_container_endpoint must be provided with 'abfss' write method")
                    df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}")
                return            

            # Define match condition for the MERGE operation
            match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])

            # Configure the merge operation to update existing records
            merge_operation = deltaTable.alias("target") \
                            .merge(df_source.alias("source"), match_condition) \
                            .whenMatchedUpdateAll()

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
                        'match_keys', and 'delete_unmatched'. Expects audit columns in the dataframe.
            write_method (str): The method to use for saving the table ('default' or 'abfss').
            storage_container_endpoint (str): Endpoint if using 'abfss' write method.
            
        Raises:
            Exception: If any error occurs during the upsert operation.
        """
        table_name = config['table_name']
        df_source = config['dataframe']
        match_keys = config['match_keys']
        current_ts = lit(datetime.datetime.now())

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
                            .withColumn("effective_from_date_time", current_ts) \
                            .withColumn("effective_to_date_time", lit(datetime.datetime(9999, 12, 31, 23, 59, 59))) \
                            .withColumn("is_current", lit(True)) \
                            .withColumn("is_deleted", lit(False))

                if write_method == 'default':
                    df_source.write.format("delta").saveAsTable(table_name)
                elif write_method == 'abfss':
                    if not storage_container_endpoint:
                        raise ValueError("storage_container_endpoint must be provided with 'abfss' write method")
                    df_source.write.format("delta").save(f"{storage_container_endpoint}/Tables/{table_name}")
                return            

            # Enhance df_source with necessary audit and SCD2 fields
            df_source = (df_source
                        .withColumn("inserted_date_time", current_ts)
                        .withColumn("updated_date_time", current_ts)
                        .withColumn("effective_from_date_time", current_ts)
                        .withColumn("effective_to_date_time", lit(datetime.datetime(9999, 12, 31, 23, 59, 59)))
                        .withColumn("is_current", lit(True))
                        .withColumn("is_deleted", lit(False)))
            
            # Get non-match key fields excluding audit and SCD2 fields
            non_match_keys = [col for col in df_source.columns if col not in match_keys + [
                "inserted_date_time",
                "updated_date_time",
                "effective_from_date_time",
                "effective_to_date_time",
                "is_current",
                "is_deleted"
            ]]

            # Create a condition to check if any non-match key field has changed
            update_condition = " OR ".join([f"target.{field} <> source.{field}" for field in non_match_keys])

            # Define match condition for the MERGE operation
            match_condition = " AND ".join([f"target.{k} = source.{k}" for k in match_keys])

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

            # Handle soft deletes if required
            if delete_unmatched:
                delete_expr = {
                    "is_deleted": lit(True),
                    "is_current": lit(False),
                    "updated_date_time": current_ts
                }
                merge_operation = deltaTable.alias("target") \
                                .merge(df_source.alias("source"), match_condition) \
                                .whenNotMatchedByTargetUpdate(set=delete_expr)

                merge_operation.execute()
            print(f"Dimension upsert for {table_name} complete.")
            
        except Exception as e:
            self.logger.error(f"An error occurred in upsert_to_delta_table for table {table_name}: {e}")
            raise


# In[ ]:


class UpsertGeneric(UpsertStrategy):

    def upsert_to_table(self, config, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
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
            df_source = df_source.withColumn("inserted_date_time", current_ts).withColumn("updated_date_time", current_ts)

            # Define update expressions excluding match keys
            update_expr = {col: f"source.{col}" for col in df_source.columns if col not in match_keys}

            # Initialize the merge operation
            merge_operation = deltaTable.alias("target") \
                .merge(df_source.alias("source"), match_condition) \
                .whenMatchedUpdate(set=update_expr) \
                .whenNotMatchedInsertAll()

            # Conditionally add delete operation for unmatched source rows
            if delete_unmatched:
                merge_operation = merge_operation.whenNotMatchedBySourceDelete()

            # Execute the merge operation
            merge_operation.execute()

        except Exception as e:
            self.logger.error(f"An error occurred while upserting into the Delta table {table_name}: {e}")
            raise


# In[ ]:


class ConcurrentUpsertHandler:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def upsert_data_concurrently(self, table_configs, storage_container_endpoint=None, write_method='default', delete_unmatched=False):
        def upsert_table(config):
            strategy_map = {
                'fact': UpsertToFactTable(self.spark, self.logger),
                'scd2': UpsertSCD2Strategy(self.spark, self.logger),
                'scd1': UpsertSCD1Strategy(self.spark, self.logger),
                'generic': UpsertGeneric(self.spark, self.logger)
            }
            strategy = strategy_map.get(config.get('upsert_type', 'generic'), UpsertGeneric(self.spark, self.logger))
            strategy.upsert_to_table(config, storage_container_endpoint, write_method, delete_unmatched)
            self.logger.info(f"Upsert for {config['table_name']} completed.")

        with ThreadPoolExecutor(max_workers=min(len(table_configs), (os.cpu_count() or 1) * 5)) as executor:
            futures = [executor.submit(upsert_table, config) for config in table_configs]
            for future in as_completed(futures):
                future.result()

