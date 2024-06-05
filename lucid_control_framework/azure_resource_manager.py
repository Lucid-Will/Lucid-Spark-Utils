import logging
from azure.mgmt.resource import SubscriptionClient, ResourceManagementClient
from azure.mgmt.keyvault import KeyVaultManagementClient
from azure.keyvault.secrets import SecretClient
from azure.synapse.artifacts import ArtifactsClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import AzureError
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession, DataFrame
from functools import reduce
from typing import Dict, Any, List, Optional, Tuple
from .utility_manager import UtilityManager

class AzureResourceManager:
    """
    The AzureResourceManager class is responsible for collecting and logging details related to Azure resources.

    :param tenant_id: The Azure tenant ID.
    :param client_id: The Azure client ID.
    :param client_secret: The Azure client secret.

    These credentials are used to create a ClientSecretCredential object for Azure operations.

    Example:
        azure_resource_manager = AzureResourceManager(tenant_id="your_tenant_id", client_id="your_client_id", client_secret="your_client_secret")
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str) -> None:
        """
        Initializes the AzureResourceManager with the provided Azure credentials.

        :param tenant_id: The Azure tenant ID.
        :param client_id: The Azure client ID.
        :param client_secret: The Azure client secret.

        :raises ValueError: If any of the Azure credentials are not provided.
        :raises Exception: If there's a problem initializing the AzureResourceManager.
        """
        # If tenant_id, client_id, or client_secret are not provided, raise an exception
        if not tenant_id or not client_id or not client_secret:
            raise ValueError("Missing necessary credentials. Please provide tenant_id, client_id, and client_secret.")
    
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Initialize Spark session
        self.spark = SparkSession.builder.getOrCreate()
        
        try:
            # Store the tenant_id
            self.tenant_id = tenant_id

            # Create a ClientSecretCredential object
            self.credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )

            # Initialize Azure clients as None to be set on first use
            self._subscription_client: Optional[Any] = None
            self._resource_clients: Dict[str, Any] = {}
            self._keyvault_clients: Dict[str, Any] = {}

            
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in __init__: {e}")
            raise

    @property
    def subscription_client(self) -> SubscriptionClient:
        """
        Returns a SubscriptionClient object. If the object has not been created yet, it is created using the provided Azure credentials.

        :return: A SubscriptionClient object.
        :raises Exception: If Azure credentials are not provided.

        Example:
            subscription_client = azure_resource_manager.subscription_client()
        """
        try:
            # If _subscription_client is not set yet, create a SubscriptionClient object
            if not self._subscription_client:
                self._subscription_client = SubscriptionClient(self.credential)
            # Return the SubscriptionClient object
            return self._subscription_client
        except AzureError as e:
            self.logger.error(f"An Azure error occurred in subscription_client: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in subscription_client: {e}")
            raise
    
    def get_resource_client(self, subscription_id: str) -> ResourceManagementClient:
        """
        Returns a ResourceManagementClient object for the given subscription_id.
        If the object has not been created yet, it is created using the provided Azure credentials.

        :param subscription_id: The subscription ID to create the ResourceManagementClient for.
        :return: A ResourceManagementClient object.
        :raises Exception: If Azure credentials are not provided.

        Example:
            resource_management_client = azure_resource_manager.resource_management_client("your_subscription_id")
        """
        try:
            # If a ResourceManagementClient for the given subscription_id is not set yet, create one
            if subscription_id not in self._resource_clients:
                self._resource_clients[subscription_id] = ResourceManagementClient(self.credential, subscription_id)
            # Return the ResourceManagementClient object for the given subscription_id
            return self._resource_clients[subscription_id]
        except AzureError as e:
            self.logger.error(f"An Azure error occurred in get_resource_client: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_resource_client: {e}")
            raise
    
    def get_keyvault_client(self, subscription_id: str) -> KeyVaultManagementClient:
        """
        Returns a KeyVaultManagementClient object for the given subscription_id.
        If the object has not been created yet, it is created using the provided Azure credentials.

        :param subscription_id: The subscription ID to create the KeyVaultManagementClient for.
        :return: A KeyVaultManagementClient object.
        :raises Exception: If Azure credentials are not provided.

        Example:
            key_vault_management_client = azure_resource_manager.key_vault_management_client("your_subscription_id")
        """
        try:
            # If a KeyVaultManagementClient for the given subscription_id is not set yet, create one
            if subscription_id not in self._keyvault_clients:
                self._keyvault_clients[subscription_id] = KeyVaultManagementClient(self.credential, subscription_id)
            # Return the KeyVaultManagementClient object for the given subscription_id
            return self._keyvault_clients[subscription_id]
        except AzureError as e:
            self.logger.error(f"An Azure error occurred in get_keyvault_client: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_keyvault_client: {e}")
            raise

    def get_subscriptions_dataframe(self) -> DataFrame:
        """
        Fetches all Azure subscriptions and returns them in a Spark DataFrame with an explicitly defined schema.

        :return: DataFrame containing subscription details.
        :raises AzureError: If there's a problem with the Azure service.

        Example:
            subscriptions_df = azure_resource_manager.fetch_subscriptions()
        """
        schema = StructType([
            StructField("authorization_source", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("id", StringType(), True),
            StructField("state", StringType(), True),
            StructField("subscription_id", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("tenant_id", StringType(), True)
        ])
    
        def fetch_subscriptions():
            """
            Fetches all Azure subscriptions.

            :return: List of all Azure subscriptions.
            :raises AzureError: If there's a problem with the Azure service.
            """
            try:
                return list(self.subscription_client.subscriptions.list())
            except AzureError as e:
                self.logger.error(f"An Azure error occurred in fetch_subscriptions: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in fetch_subscriptions: {e}")
                raise
    
        def process_subscription(subscription: Dict[str, Any]):
            """
            Processes a subscription to extract and return subscription details.

            :param subscription: A dictionary containing subscription details.
            :return: Dictionary containing processed subscription details.

            Example:
                processed_subscription = azure_resource_manager.process_subscription(subscription)
            """
            try:
                return UtilityManager.flatten_properties(subscription)
            except AzureError as e:
                self.logger.error(f"An Azure error occurred while processing subscription {subscription['id']}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An error occurred while processing subscription {subscription['id']}: {e}")
                raise
    
        subscriptions_data = [process_subscription(sub) for sub in fetch_subscriptions()]
        return self.spark.createDataFrame(subscriptions_data, schema=schema)

    def get_resource_group_dataframe(self) -> DataFrame:
        """
        Fetches all resource groups across all subscriptions and returns them in a Spark DataFrame with an explicitly defined schema.

        :return: DataFrame containing resource group details.
        :raises AzureError: If there's a problem with the Azure service.

        Example:
            resource_groups_df = azure_resource_manager.fetch_all_resource_groups()
        """
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("managed_by", StringType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("tenant_id", StringType(), True),
            StructField("subscription_id", StringType(), True),
            StructField("tags", StringType(), True)
        ])
    
        def fetch_resource_groups():
            """
            Fetches all resource groups across all subscriptions.
            
            :return: List[Tuple]: List of tuples where each tuple contains a resource group and its associated subscription ID.
            """
            try:
                return [(rg, sub.subscription_id) for sub in self.subscription_client.subscriptions.list()
                        for rg in self.get_resource_client(sub.subscription_id).resource_groups.list()]
            except AzureError as e:
                self.logger.error(f"An Azure error occurred in fetch_resource_groups: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in fetch_resource_groups: {e}")
                raise
    
        def process_resource_group(resource_group_tuple: Tuple[Any, str]):
            """
            Processes a resource group tuple to extract and return resource group details.

            :param resource_group_tuple: A tuple containing a resource group and its associated subscription ID.
            :return: Dictionary containing resource group details.

            Example:
                processed_resource_group = azure_resource_manager.process_resource_group_tuple(resource_group_tuple)
            """
            try:
                resource_group, subscription_id = resource_group_tuple
                rg_details = UtilityManager.flatten_properties(resource_group)
                rg_details.update({"subscription_id": subscription_id, "tenant_id": self.tenant_id})
                return rg_details
            except AzureError as e:
                self.logger.error(f"An Azure error occurred in process_resource_group: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An error occurred in process_resource_group: {e}")
                raise
    
        resource_groups_data = [process_resource_group(rg) for rg in fetch_resource_groups()]
        return self.spark.createDataFrame(resource_groups_data, schema=schema)

    def get_resource_dataframe(self) -> DataFrame:
        """
        Fetches all resources across all resource groups and subscriptions and returns them in a Spark DataFrame with an explicitly defined schema.

        :return: DataFrame containing resource details.
        :raises AzureError: If there's a problem with the Azure service.

        Example:
            resources_df = azure_resource_manager.fetch_all_resources()
        """
        # Define the schema for the DataFrame
        schema = StructType([
            StructField("changed_time", StringType(), True),
            StructField("created_time", StringType(), True),
            StructField("extended_location", StringType(), True),
            StructField("id", StringType(), True),
            StructField("identity", StringType(), True),
            StructField("kind", StringType(), True),
            StructField("location", StringType(), True),
            StructField("managed_by", StringType(), True),
            StructField("name", StringType(), True),
            StructField("plan", StringType(), True),
            StructField("properties", StringType(), True),
            StructField("provisioning_state", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("type", StringType(), True),
            StructField("tenant_id", StringType(), True),
            StructField("subscription_id", StringType(), True),
            StructField("resource_group_name", StringType(), True),
            StructField("sku", StringType(), True)
        ])

        def fetch_resources() -> List[Tuple[Any, str, str]]:
            """
            Fetches all resources across all resource groups and subscriptions.

            :return: A list of tuples, each containing a resource, a subscription ID, and a resource group name.
            :raises Exception: If there's a problem fetching the resources.

            Example:
                resources = azure_resource_manager.fetch_resources()
            """
            try:
                return [(res, sub.subscription_id, rg.name) for sub in self.subscription_client.subscriptions.list()
                        for rg in self.get_resource_client(sub.subscription_id).resource_groups.list()
                        for res in self.get_resource_client(sub.subscription_id).resources.list_by_resource_group(rg.name)]
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in fetch_resources: {e}")
                raise

        def process_resource(resource_tuple: Tuple[Any, str, str]) -> Dict[str, Any]:
            """
            Processes a resource tuple and returns a dictionary with the resource details.

            :param resource_tuple: A tuple containing a resource, a subscription ID, and a resource group name.
            :return: A dictionary with the resource details.
            :raises Exception: If there's a problem processing the resource.

            Example:
                processed_resource = azure_resource_manager.process_resource(resource_tuple)
            """
            try:
                resource, subscription_id, resource_group_name = resource_tuple
                resource_details = UtilityManager.flatten_properties(resource)
                resource_details.update({"subscription_id": subscription_id, "tenant_id": self.tenant_id, "resource_group_name": resource_group_name})
                return resource_details
            except Exception as e:
                self.logger.error(f"An error occurred while processing resource: {e}")
                raise

        # Fetch the resource data
        resources = fetch_resources()

        # Process the resource data
        resources_data = [process_resource(res) for res in resources]

        # Convert the resource data to a Spark DataFrame using the defined schema
        return self.spark.createDataFrame(resources_data, schema=schema)

    def get_keyvaults_and_secrets_dataframe(self) -> DataFrame:
        """
        Fetches all key vaults and secret names across all subscriptions and returns them in a Spark DataFrame with an explicitly defined schema.

        :return: DataFrame containing key vault and secret names.
        :raises AzureError: If there's a problem with the Azure service.

        Example:
            key_vaults_and_secrets_df = azure_resource_manager.fetch_all_key_vaults_and_secrets()
        """
        schema = StructType([
            StructField("key_vault_name", StringType(), True),
            StructField("key_vault_secret_name", StringType(), True),
            StructField("key_vault_url", StringType(), True),
            StructField("tenant_id", StringType(), True),
            StructField("subscription_id", StringType(), True)
        ])
    
        def fetch_keyvaults_and_secrets():
            """
            Fetches all key vaults and secret names across all subscriptions.
            Returns:
                list: List of tuples containing vault name, secret name, vault URL, tenant_id, and subscription_id.
            """
            try:
                results = []
                for sub in self.subscription_client.subscriptions.list():
                    resource_client = ResourceManagementClient(self.credential, sub.subscription_id)
                    keyvault_client = self.get_keyvault_client(sub.subscription_id)
                    for resource_group in resource_client.resource_groups.list():
                        for vault in keyvault_client.vaults.list_by_resource_group(resource_group.name):
                            secret_client = SecretClient(vault_url=vault.properties.vault_uri, credential=self.credential)
                            for secret in secret_client.list_properties_of_secrets():
                                results.append((vault.name, secret.name, vault.properties.vault_uri, self.tenant_id, sub.subscription_id))
                return results
            except AzureError as e:
                self.logger.error(f"An Azure error occurred in fetch_keyvaults_and_secrets: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in fetch_keyvaults_and_secrets: {e}")
                raise
    
        key_vault_data = fetch_keyvaults_and_secrets()
    
        return self.spark.createDataFrame(data=key_vault_data, schema=schema)

    def get_locations_dataframe(self) -> DataFrame:
        """
        Fetches all Azure locations (regions) and returns them in a Spark DataFrame with an explicitly defined schema.

        :return: DataFrame containing location details.
        :raises AzureError: If there's a problem with the Azure service.

        Example:
            locations_df = azure_resource_manager.get_locations_dataframe()
        """
        # Define the schema for the DataFrame
        schema = StructType([
            StructField("region_name", StringType(), True),
            StructField("region_display_name", StringType(), True),
            StructField("regional_display_name", StringType(), True),
        ])
    
        def fetch_locations():
            """
            Fetches all Azure locations for the first subscription.

            :return: List of all Azure locations.
            :raises AzureError: If there's a problem with the Azure service.
            :raises Exception: If no subscriptions are found.

            Example:
                locations = fetch_locations()
            """
            try:
                # Get the first subscription
                subscription = next(self.subscription_client.subscriptions.list(), None)
                # Raise an exception if no subscriptions are found
                if not subscription:
                    raise Exception("No subscriptions found.")
                
                # Fetch all locations in the subscription
                return list(self.subscription_client.subscriptions.list_locations(subscription.subscription_id))
            except AzureError as e:
                self.logger.error(f"An Azure error occurred in fetch_locations: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in fetch_locations: {e}")
                raise
    
        def process_locations(locations):
            """
            Processes a list of locations to extract and return location details.

            :param locations: A list of Azure locations.
            :return: List of dictionaries containing location details.

            Example:
                processed_locations = azure_resource_manager.process_locations(locations)
            """
            return [
                {
                    'region_name': location.name,
                    'region_display_name': location.display_name,
                    'regional_display_name': location.regional_display_name
                }
                for location in locations
            ]
    
        # Fetch the location data
        locations = fetch_locations()
        
        # Process the location data
        locations_data = process_locations(locations)
        
        # Convert the location data to a Spark DataFrame using the defined schema
        return self.spark.createDataFrame(locations_data, schema=schema)

    def get_pipelines_dataframe(self, workspace_name: str) -> DataFrame:
        """
        Fetches Synapse Analytics pipelines for a specified workspace and returns them in a Spark DataFrame with an explicitly defined schema.

        :param workspace_name: The name of the Synapse workspace to fetch pipelines from.
        :return: Spark DataFrame containing pipeline details.
        :raises AzureError: If there's a problem with the Azure service.

        Example:
            pipelines_df = azure_resource_manager.get_pipelines_dataframe("your_workspace_name")
        """
        # Define the schema for the DataFrame
        schema = StructType([
            StructField("workspace_name", StringType(), True),
            StructField("additional_properties", StringType(), True),
            StructField("concurrency", StringType(), True),
            StructField("description", StringType(), True),
            StructField("etag", StringType(), True),
            StructField("folder", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("run_dimensions", StringType(), True),
            StructField("type", StringType(), True),
            StructField("variables", StringType(), True),
        ])
    
        def fetch_pipelines():
            """
            Fetches all pipelines in the specified Synapse workspace.

            :return: List of all pipelines in the workspace.
            :raises AzureError: If there's a problem with the Azure service.

            Example:
                pipelines = fetch_pipelines()
            """
            try:
                # Construct the workspace URL
                workspace_url = f"https://{workspace_name}.dev.azuresynapse.net"
                # Create an ArtifactsClient instance
                artifacts_client = ArtifactsClient(credential=self.credential, endpoint=workspace_url)
                # Fetch all pipelines in the workspace
                return list(artifacts_client.pipeline.get_pipelines_by_workspace())
            except AzureError as e:
                self.logger.error(f"An Azure error occurred in fetch_pipelines: {e}")
                raise
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in fetch_pipelines: {e}")
                raise
    
        def process_pipelines(pipelines):
            """
            Processes a list of pipelines to extract and return pipeline details.

            :param pipelines: A list of pipelines.
            :return: List of dictionaries containing pipeline details.

            Example:
                processed_pipelines = process_pipelines(pipelines)
            """
            return [
                {"workspace_name": workspace_name, **UtilityManager.flatten_properties(pipeline)}
                for pipeline in pipelines
            ]
    
        # Fetch the pipeline data
        pipelines = fetch_pipelines()
        
        # Process the pipeline data
        pipelines_data = process_pipelines(pipelines)
        
        # Convert the pipeline data to a Spark DataFrame using the defined schema
        return self.spark.createDataFrame(pipelines_data, schema=schema)
    
    def transform_and_stage_azure_system_details(self) -> DataFrame:
        """
        Fetches, prepares, transforms, and stages Azure details for subscriptions, resource groups, resources, locations, 
        and pipelines, creating Spark DataFrames optimized for upsert operations into Delta tables.

        :return: DataFrame containing detailed and transformed information about the Azure system, 
                including subscriptions, resource groups, resources, locations, and pipelines, ready for upsert.

        Example:
            azure_details_df = azure_resource_manager.fetch_and_prepare_azure_details()
        """
        try:
            # Fetch details and convert to DataFrames using the AzureResourceManager methods
            df_subscriptions = self.azure_resource_manager.get_subscriptions_dataframe()
            df_resource_groups = self.azure_resource_manager.get_resource_group_dataframe()
            df_resources = self.azure_resource_manager.get_resource_dataframe()
            df_locations = self.azure_resource_manager.get_locations_dataframe()
            df_keyvaults_and_secrets = self.azure_resource_manager.get_keyvaults_and_secrets_dataframe()

            # Filter for Synapse workspaces
            df_synapse_workspaces = df_resources.filter(df_resources["type"] == 'Microsoft.Synapse/workspaces')

            # Initialize an empty list for pipelines DataFrames
            df_pipelines_list = []

            # Fetch pipelines for each workspace and add to the list
            workspace_names = df_synapse_workspaces.select('name').rdd.flatMap(lambda x: x).collect()
            for name in workspace_names:
                df_pipelines_list.append(self.azure_resource_manager.get_pipelines_dataframe(name))

            # Concatenate all pipelines DataFrames
            df_pipelines = reduce(DataFrame.unionAll, df_pipelines_list)

            # Register the DataFrames as temporary views so they can be used in Spark SQL
            df_subscriptions.createOrReplaceTempView("subscriptions")
            df_resource_groups.createOrReplaceTempView("resource_groups")
            df_synapse_workspaces.createOrReplaceTempView("synapse_workspaces")
            df_locations.createOrReplaceTempView("locations")
            df_pipelines.createOrReplaceTempView("pipelines")
            df_keyvaults_and_secrets.createOrReplaceTempView("keyvaults_and_secrets")

            # Perform the joins and transformations using Spark SQL
            df_azure_system_master = self.spark.sql("""
                SELECT
                    s.tenant_id,
                    s.subscription_id,
                    s.display_name AS subscription_name,
                    s.state AS subscription_state,
                    rg.name AS resource_group_name,
                    rgl.region_name AS resource_group_region_name,
                    rgl.region_display_name AS resource_group_region_display_name,
                    rgl.regional_display_name AS resource_group_regional_display_name,
                    sw.id AS orchestration_system_id,
                    sw.name AS orchestration_system_name,
                    CASE WHEN sw.type = 'Microsoft.Synapse/workspaces' THEN 'Synapse Analytics Workspace' ELSE NULL END AS system_type_name,
                    wsl.region_name AS resource_region_name,
                    wsl.region_display_name AS resource_region_display_name,
                    wsl.regional_display_name AS resource_regional_display_name,
                    p.id AS orchestration_object_id,
                    p.name AS orchestration_object_name,
                    CASE WHEN p.type = 'Microsoft.Synapse/workspaces/pipelines' THEN 'Pipeline' ELSE NULL END AS orchestration_type_name,
                    kvs.key_vault_name,
                    kvs.key_vault_url,
                    kvs.key_vault_secret_name
                FROM subscriptions s
                JOIN resource_groups rg
                    ON s.subscription_id = rg.subscription_id
                JOIN synapse_workspaces sw
                    ON sw.subscription_id = s.subscription_id AND sw.resource_group_name = rg.name
                LEFT JOIN locations rgl
                    ON rg.location = rgl.region_name
                LEFT JOIN locations wsl
                    ON sw.location = wsl.region_name
                LEFT JOIN pipelines p
                    ON sw.name = p.workspace_name
                LEFT JOIN keyvaults_and_secrets kvs
                    ON s.subscription_id = kvs.subscription_id
            """)

            # Write the transformed DataFrame to the Delta layer
            df_azure_system_master.write.format('delta').mode('overwrite').saveAsTable('Control.stage_azure_system_master')

            print('Azure system details have been successfully transformed and staged.')
            
            return df_azure_system_master
        
        except Exception as e:
            # Log the error message and re-raise the exception
            self.logger.error(f"An unexpected error occurred in transform_and_stage_azure_system_details: {e}")
            raise