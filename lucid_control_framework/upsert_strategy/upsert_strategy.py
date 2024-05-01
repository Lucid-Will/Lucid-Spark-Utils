from abc import ABC, abstractmethod
from typing import Optional
import logging

class UpsertStrategy(ABC):
    """
    Base class for upsert strategies.

    This class should be subclassed by any class that implements a specific upsert strategy.
    """

    def __init__(self, spark, logger: 'logging', transform_manager):
        """
        Initialize the UpsertStrategy.

        :param spark: The SparkSession object.
        :param logger: The logger object.
        :param transform_manager: The TransformManager object. Default is None.
        """
        self.spark = spark
        self.logger = logger
        self.transformer = transform_manager

    @abstractmethod
    def upsert_to_table(self, config: dict, storage_container_endpoint: Optional[str] = None, write_method: str = 'default') -> None:
        """
        Upsert data to a table.

        This is an abstract method that must be overridden by subclasses.

        :param config: The configuration dictionary.
        :param storage_container_endpoint: The storage container endpoint. Default is None.
        :param write_method: The write method. Default is 'default'.
        :raises NotImplementedError: If the method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclass must implement abstract method")