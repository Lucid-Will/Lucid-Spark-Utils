# Import the required libraries
import logging
from typing import Optional
import sempy.fabric as fabric
from azure.core.exceptions import AzureError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

class FabricResourceManager:
    """
    The `FabricResourceManager` class is responsible for collecting and logging details related to 
    Microsoft Fabric and converting it into Spark DataFrames. It provides methods to transform and 
    stage fabric system details such as workspace, capacity, and artifact dataframes.
    """

    def __init__(self):
        """
        Initializes the `FabricResourceManager` instance.
        
        Sets up a logger with the name of the current module and sets its level to INFO.
        Initializes a SparkSession with the app name "AzureFabricManager".
        Attempts to initialize a `FabricRestClient`. If this fails, it logs the error and re-raises the exception.
        
        Raises:
            Exception: If there's a problem initializing the `FabricRestClient`.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Initialize Spark session
        self.spark = SparkSession.builder.getOrCreate()
        
        # Initialize the FabricRestClient
        try:
            self.fabric_client = fabric.FabricRestClient()
        except Exception as e:
            self.logger.error(f"An error occurred while initializing the FabricRestClient: {e}")
            raise

    def get_fabric_workspace_dataframe(self, additional_filters: Optional[str] = None) -> DataFrame:
        """
        Fetches workspace data from Azure Fabric, applying optional filtering, and returns it as a Spark DataFrame.
        The schema is explicitly defined.
        Args:
            additional_filters (Optional[str], optional): Additional OData filter expressions to refine the fetched workspaces.
        Returns:
            pyspark.sql.DataFrame: Spark DataFrame containing the workspace data, with an inferred schema.
        Raises:
            AzureError: If there's a problem with the Azure Fabric service.
        """
        default_filter = "type ne 'AdminInsights'"
        filter_expression = f"({default_filter}) and ({additional_filters})" if additional_filters else default_filter
        
        # Define the schema for the workspace DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("is_read_only", BooleanType(), True),
            StructField("is_on_dedicated_capacity", BooleanType(), True),
            StructField("capacity_id", StringType(), True),
            StructField("default_dataset_storage_format", StringType(), True),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
        ])
        
        try:
            # Direct fetch, assuming this returns a list of dictionaries
            workspaces_data = self.fabric_client.list_workspaces(filter=filter_expression)
            
            # Convert directly to Spark DataFrame with inferred schema
            df_fabric_workspace_spark = self.spark.createDataFrame(workspaces_data, schema=schema)

            return df_fabric_workspace_spark
        except AzureError as e:
            self.logger.error(f"An Azure service error occurred in get_fabric_workspace_dataframe: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_fabric_workspace_dataframe: {e}")
            raise

    def get_fabric_capacity_dataframe(self, filter_expression: Optional[str] = None) -> DataFrame:
        """
        Fetches capacity data from Azure Fabric, applying an optional filter expression, and returns it as a Spark DataFrame.
        The schema is explicitly defined.
        Args:
            filter_expression (Optional[str], optional): The OData filter expression to apply when fetching capacities.
        Returns:
            pyspark.sql.DataFrame: Spark DataFrame containing the capacity data, with an inferred schema.
        Raises:
            AzureError: If there's a problem with the Azure Fabric service.
        """

        # Define the schema for the capacity DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("region", StringType(), True),
            StructField("state", StringType(), True),
        ])
        
        try:
            # Directly fetch capacity data into a format that Spark can handle (assuming list of dictionaries)
            capacities_data = self.fabric_client.list_capacities(filter_expression=filter_expression) if filter_expression else self.fabric_client.list_capacities()
            
            # Convert directly to Spark DataFrame with inferred schema
            df_fabric_capacity_spark = self.spark.createDataFrame(capacities_data, schema=schema)

            return df_fabric_capacity_spark
        except AzureError as e:
            self.logger.error(f"An Azure service error occurred in get_fabric_capacity_dataframe: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_fabric_capacity_dataframe: {e}")
            raise

    def get_fabric_item_dataframe(self, filter_expression: Optional[str] = None) -> DataFrame:
        """
        Fetches and prepares item data from Azure Fabric with an optional filter, returning it as a Spark DataFrame.
        The schema is inferred based on the data structure of the input.
        Args:
            filter_expression (Optional[str], optional): The filter expression to apply when fetching item data.
        Returns:
            pyspark.sql.DataFrame: Spark DataFrame containing the filtered item data, with an inferred schema.
        Raises:
            AzureError: If there's a problem with the Azure Fabric service.
        """

        # Define the schema for the item DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("type", StringType(), True),
            StructField("workspace_id", StringType(), True),
        ])
        
        try:
            # Fetch item data with an optional filter
            items_data = self.fabric_client.list_items(filter_expression=filter_expression) if filter_expression else self.fabric_client.list_items()
            
            # Convert to Spark DataFrame with inferred schema
            df_fabric_item_spark = self.spark.createDataFrame(items_data, schema=schema)

            return df_fabric_item_spark
        except AzureError as e:
            self.logger.error(f"An Azure service error occurred in get_fabric_item_dataframe: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_fabric_item_dataframe: {e}")
            raise

    def transform_and_stage_fabric_system_details(self, tenant_id: str) -> DataFrame:
        """
        Fetches, prepares, transforms, and stages Azure Fabric system details for workspaces, capacities, items, and locations, 
        creating Spark DataFrames optimized for upsert operations into Delta tables.
        Returns:
            pyspark.sql.DataFrame: DataFrame containing detailed and transformed information about the Azure Fabric system, 
            including workspaces, capacities, items, and locations, ready for upsert.
        """
        try:
            # Fetch details and convert to DataFrames
            df_workspace = self.get_fabric_workspace_dataframe()
            df_capacity = self.get_fabric_capacity_dataframe()
            df_item = self.get_fabric_item_dataframe()

            # Register the DataFrames as temporary views so they can be used in Spark SQL
            df_workspace.createOrReplaceTempView("workspaces")
            df_capacity.createOrReplaceTempView("capacities")
            df_item.createOrReplaceTempView("items")

            # Perform the joins and transformations using Spark SQL
            df_fabric_system_master = self.spark.sql(f"""
                WITH item_details AS (
                    SELECT
                        i.type,
                        i.id,
                        i.display_name,
                        CASE 
                            WHEN LOWER(i.display_name) LIKE '%raw%' THEN 'Raw'
                            WHEN LOWER(i.display_name) LIKE '%bronze%' THEN 'Bronze'
                            WHEN LOWER(i.display_name) LIKE '%silver%' THEN 'Silver'
                            WHEN LOWER(i.display_name) LIKE '%gold%' THEN 'Gold'
                            WHEN LOWER(i.display_name) LIKE '%control%' THEN 'Control'
                            ELSE 'Other'
                        END AS storage_container_name,
                        CASE WHEN i.type = 'Lakehouse' THEN CONCAT('abfss://', w.id, '@onelake.dfs.fabric.microsoft.com/', i.id) ELSE '' END AS storage_container_endpoint
                    FROM 
                        items i
                )
                SELECT
                    {tenant_id} AS tenant_id,
                    c.region AS region_display_name,
                    w.id AS orchestration_system_id,
                    w.name AS orchestration_system_name,
                    w.id AS storage_system_id,
                    w.name AS storage_system_name,
                    'Fabric Workspace' AS system_type_name,
                    CASE WHEN id.type = 'Notebook' THEN id.type ELSE NULL END AS orchestration_type_name,
                    CASE WHEN id.type IN ('Lakehouse', 'Warehouse') THEN id.type ELSE NULL END AS storage_type_name,
                    CASE WHEN id.type = 'Notebook' THEN id.id ELSE NULL END AS orchestration_object_id,
                    CASE WHEN id.type IN ('Lakehouse', 'Warehouse') AND id.display_name NOT IN ('DataflowStagingLakehouse', 'DataflowStagingWarehouse') THEN id.id ELSE NULL END AS storage_object_id,
                    CASE WHEN id.type = 'Notebook' THEN id.display_name ELSE NULL END AS orchestration_object_name,
                    CASE WHEN id.type IN ('Lakehouse', 'Warehouse') AND id.display_name NOT IN ('DataflowStagingLakehouse', 'DataflowStagingWarehouse') THEN id.display_name ELSE NULL END AS storage_object_name,
                    id.storage_container_name,
                    id.storage_container_endpoint
                FROM 
                    workspaces w
                INNER JOIN 
                    capacities c ON LOWER(w.capacity_id) = LOWER(c.id)
                INNER JOIN 
                    item_details id ON LOWER(w.id) = LOWER(id.workspace_id)
            """, {"tenant_id": self.tenant_id})

            return df_fabric_system_master
        except Exception as e:
            # Log an error message and re-throw the exception if any other exception occurs
            logging.error(f"An unexpected error occurred in transform_and_stage_fabric_system_details: {e}")
            raise