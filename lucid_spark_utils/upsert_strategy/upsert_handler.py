from . import UpsertFact, UpsertSCD2, UpsertSCD1, UpsertGeneric
from lucid_spark_utils.transformation_manager import TransformationManager
from lucid_spark_utils.delta_table_manager import DeltaTableManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
import logging
import os

class UpsertHandler:
    """
    This class handles concurrent upsert operations on multiple tables.
    """

    def __init__(self, spark, logger=None):
        """
        Initializes the handler with a Spark session, a logger, a file manager, and an optional transform manager.

        :param spark: The Spark session.
        :param logger: The logger. If not provided, a default logger will be used.
        """
        self.spark = spark
        self.logger = logger if logger else logging.getLogger(__name__)
        self.transform_manager = TransformationManager(self.spark, self.logger)
        self.table_manager = DeltaTableManager(self.spark, self.logger)

        # Create a single instance of each strategy class
        self.strategy_map = {
            'fact': UpsertFact(self.spark, self.logger, self.transform_manager, self.table_manager),
            'scd2': UpsertSCD2(self.spark, self.logger, self.transform_manager, self.table_manager),
            'scd1': UpsertSCD1(self.spark, self.logger, self.transform_manager, self.table_manager),
            'generic': UpsertGeneric(self.spark, self.logger, self.transform_manager, self.table_manager)
        }

    def upsert_data_concurrently(
        self, 
        table_configs: List[Dict[str, str]], 
        storage_container_endpoint: Optional[str] = None, 
        write_method: str = 'catalog'
    ) -> None:
        """
        Performs upsert operations concurrently on multiple tables based on the provided configurations.

        :param table_configs: A list of dictionaries, each representing a table configuration. Each dictionary should include:
            'table_name': The name of the table to upsert.
            'upsert_type': The type of upsert operation to perform. Options are 'scd1' and 'scd2'.
            'primary_key': A list of columns that form the primary key.
            'composite_columns': A list of columns to use for creating a composite key.
        :param storage_container_endpoint: The endpoint of the storage container if using 'path' write method. This is optional and defaults to None.
        :param write_method: The method to use for saving the table. This should be either 'path' or 'catalog'. Defaults to 'catalog'.

        :return: None. The function performs upsert operations on the tables concurrently.

        Raises:
            Exception: If any error occurs during the upsert operation.

        Example:
        table_configs = [
            {
                'table_name': 'table1',
                'upsert_type': 'scd2',
                'primary_key': ['id'],
                'composite_columns': ['id', 'name', 'age']
            },
            {
                'table_name': 'table2',
                'upsert_type': 'scd1',
                'primary_key': ['id'],
                'composite_columns': ['id', 'name', 'age']
            }
        ]
        upsert_data_concurrently(table_configs)

        This function performs the following steps:
        1. For each table configuration, it gets the strategy for the specified upsert type or uses the 'scd1' strategy by default.
        2. It logs a message indicating that the upsert operation is starting.
        3. It performs the upsert operation.
        4. It logs a message indicating that the upsert operation is complete.
        5. If any error occurs during the upsert operation, it logs an error message.
        6. It sets the thread pool size based on the number of tables and available CPUs.
        7. It creates a ThreadPoolExecutor and submits the upsert_table function for each table configuration.
        8. It waits for all futures to complete.
        """
        def upsert_table(config: Dict):
            """
            Calls the appropriate upsert strategy based on the provided configuration and performs an upsert operation on a single table.

            :param config: The table configuration. It should include the following keys:
                'table_name': The name of the table to upsert.
                'upsert_type': The type of upsert operation to perform. Options are 'scd1' and 'scd2'.
                'primary_key': A list of columns that form the primary key.
                'composite_columns': A list of columns to use for creating a composite key.

            :return: None. The function performs an upsert operation on the table.

            Raises:
                Exception: If any error occurs during the upsert operation.

            Example:
            config = {
                'table_name': 'table1',
                'upsert_type': 'scd2',
                'primary_key': ['id'],
                'composite_columns': ['id', 'name', 'age']
            }
            upsert_table(config)

            This function performs the following steps:
            1. It gets the appropriate upsert strategy based on the 'upsert_type' specified in the config, or uses the 'scd1' strategy by default.
            2. It logs a message indicating that the upsert operation is starting.
            3. It calls the upsert strategy to perform the upsert operation.
            4. It logs a message indicating that the upsert operation is complete.
            5. If any error occurs during the upsert operation, it logs an error message.
            """
            # Get the strategy for the upsert type specified in the config, or use the scd1 strategy by default
            strategy = self.strategy_map.get(config.get('upsert_type', 'scd1'))

            # Log a message indicating that the upsert operation is starting
            self.logger.info(f"Starting upsert for {config['table_name']}.")

            try:
                # Perform the upsert operation
                strategy.upsert_to_table(config, storage_container_endpoint, write_method)

                # Log a message indicating that the upsert operation is complete
                self.logger.info(f"Upsert for {config['table_name']} completed.")
            except Exception as e:  # Consider catching more specific exceptions
                self.logger.error(f"Upsert for {config['table_name']} failed with error: {e}")

        # Set thread pool size based on the number of tables and available CPUs
        max_workers = min(len(table_configs), (os.cpu_count() or 1) * 5)
        
        # Create a ThreadPoolExecutor and submit the upsert_table function for each table config
        with ThreadPoolExecutor(max_workers) as executor:  # Consider making the pool size configurable
            futures = [executor.submit(upsert_table, config) for config in table_configs]

            # Wait for all futures to complete
            for future in as_completed(futures):
                future.result()