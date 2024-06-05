from . import UpsertFact, UpsertSCD2, UpsertSCD1, UpsertGeneric
from lucid_control_framework.transformation_manager import TransformationManager
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
        Initializes the handler with a Spark session, a logger, and an optional transform manager.

        :param spark: The Spark session.
        :param logger: The logger. If not provided, a default logger will be used.
        """
        self.spark = spark
        self.logger = logger if logger else logging.getLogger(__name__)
        self.transform_manager = TransformationManager(self.spark, self.logger)

        # Create a single instance of each strategy class
        self.strategy_map = {
            'fact': UpsertFact(self.spark, self.logger, self.transform_manager),
            'scd2': UpsertSCD2(self.spark, self.logger, self.transform_manager),
            'scd1': UpsertSCD1(self.spark, self.logger, self.transform_manager),
            'generic': UpsertGeneric(self.spark, self.logger, self.transform_manager)
        }

    def upsert_data_concurrently(self, table_configs: List[Dict], storage_container_endpoint: Optional[str] = None, write_method: str = 'path'):
        """
        Performs upsert operations concurrently on multiple tables based on the provided configurations.

        :param table_configs: A list of table configurations.
        :param storage_container_endpoint: The storage container endpoint (optional).
        :param write_method: The write method (default is 'path').
        :param delete_unmatched: Whether to delete unmatched records (default is False).

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
        """
        def upsert_table(config: Dict):
            """
            Performs an upsert operation on a single table based on the provided configuration.

            :param config: The table configuration.
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