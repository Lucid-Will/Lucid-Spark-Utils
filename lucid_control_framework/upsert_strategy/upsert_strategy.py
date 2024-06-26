from abc import ABC, abstractmethod
from typing import Optional

class UpsertStrategy(ABC):
    """
    Base class for upsert strategies.

    This class should be subclassed by any class that implements a specific upsert strategy.
    """

    def __init__(self, spark, logger: 'Logger', transform_manager: Optional['TransformManager'] = None, table_manager: Optional['DeltaTableManager'] = None):
        """
        Initialize the UpsertStrategy.

        :param spark: The SparkSession object.
        :param logger: The logger object.
        :param transform_manager: The TransformManager object. Default is None.
        :param table_manager: The DeltaTableManager object. Default is None.
        """

        self.spark = spark
        self.logger = logger
        self.transformer = transform_manager
        self.table_manager = table_manager

    @abstractmethod
    def upsert_to_table(self, config: dict, storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None:
        """
        Upsert data to a table.

        This is an abstract method that must be overridden by subclasses.

        :param config: The table configuration. It should include the following keys:
            'table_name': The name of the table to upsert.
            'dataframe': The dataframe to upsert into the table.
            'composite_columns': A list of columns to use for creating a composite key.
            'primary_key_column': The column that forms the primary key.
        :param storage_container_endpoint: The endpoint of the storage container if using 'path' write method.
        :param write_method: The method to use for saving the table ('path' or 'catalog').

        :raises NotImplementedError: If the method is not overridden by a subclass.
        """

        raise NotImplementedError("Subclass must implement abstract method")