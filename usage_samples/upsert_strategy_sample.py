#!/usr/bin/env python
# coding: utf-8

# ## Upsert Strategy
# 
# New notebook

# ## Upsert Strategy Module Documentation
# 
# ### Overview
# 
# The `upsert_strategy` module provides various classes and methods to handle upsert operations on Delta tables using different strategies. This module supports multiple types of tables including fact tables, Slowly Changing Dimension (SCD) Type 1, SCD Type 2, and generic tables. The module is designed to be extensible, allowing developers to implement their own upsert strategies by subclassing the `UpsertStrategy` base class.
# 
# ### Module Structure
# 
# #### Classes
# 
# 1. **UpsertStrategy**
# 2. **UpsertFact**
# 3. **UpsertSCD1**
# 4. **UpsertSCD2**
# 5. **UpsertGeneric**
# 6. **UpsertHandler**
# 
# ---
# 
# ### Class: UpsertStrategy
# 
# #### Description
# Base class for upsert strategies. This class should be subclassed by any class that implements a specific upsert strategy.
# 
# #### Methods
# 
# - `__init__(self, spark, logger: 'Logger', transform_manager: Optional['TransformManager'] = None, table_manager: Optional['DeltaTableManager'] = None)`
#     - Initialize the UpsertStrategy.
#     - **Parameters:**
#         - `spark`: The SparkSession object.
#         - `logger`: The logger object.
#         - `transform_manager`: The TransformManager object. Default is None.
#         - `table_manager`: The DeltaTableManager object. Default is None.
# 
# - `upsert_to_table(self, config: dict, storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None`
#     - Abstract method to upsert data to a table. Must be overridden by subclasses.
#     - **Parameters:**
#         - `config`: The table configuration.
#         - `storage_container_endpoint`: The storage container endpoint.
#         - `write_method`: The write method ('path' or 'catalog').
# 
# ---
# 
# ### Class: UpsertFact
# 
# #### Description
# This class implements the upsert strategy for fact tables. It inherits from the abstract base class `UpsertStrategy`.
# 
# #### Methods
# 
# - `add_insert_audit_columns(self, df: DataFrame) -> DataFrame`
#     - Adds audit columns to the source dataframe for upsert.
#     - **Parameters:**
#         - `df`: The source dataframe.
#     - **Returns:** The dataframe with added audit columns.
# 
# - `upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None`
#     - Performs an upsert operation on a Delta fact table based on the provided configuration.
#     - **Parameters:**
#         - `config`: Configuration for upserting a fact table.
#         - `storage_container_endpoint`: The endpoint of the storage container if using 'path' write method.
#         - `write_method`: The method to use for saving the table ('path' or 'catalog').
# 
# ---
# 
# ### Class: UpsertSCD1
# 
# #### Description
# This class implements the upsert strategy for SCD Type 1 tables. It inherits from the abstract base class `UpsertStrategy`.
# 
# #### Methods
# 
# - `insert_unknown_record(self, df_source)`
#     - Inserts an unknown record into the source dataframe.
#     - **Parameters:**
#         - `df_source`: The source dataframe.
#     - **Returns:** The source dataframe with the unknown record inserted.
# 
# - `add_insert_audit_columns(self, df: DataFrame) -> DataFrame`
#     - Adds audit columns to the source dataframe for upsert operations.
#     - **Parameters:**
#         - `df`: The source dataframe.
#     - **Returns:** The dataframe with added audit columns.
# 
# - `upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None`
#     - Performs an upsert operation on a Delta table based on the provided configuration.
#     - **Parameters:**
#         - `config`: Configuration for upserting a Delta table.
#         - `storage_container_endpoint`: The endpoint of the storage container if using 'path' write method.
#         - `write_method`: The method to use for saving the table ('path' or 'catalog').
# 
# ---
# 
# ### Class: UpsertSCD2
# 
# #### Description
# This class implements the upsert strategy for SCD Type 2 tables. It inherits from the abstract base class `UpsertStrategy`.
# 
# #### Methods
# 
# - `insert_unknown_record(self, df_source)`
#     - Inserts an unknown record into the source dataframe.
#     - **Parameters:**
#         - `df_source`: The source dataframe.
#     - **Returns:** The source dataframe with the unknown record inserted.
# 
# - `add_insert_audit_columns(self, df: DataFrame) -> DataFrame`
#     - Adds audit columns to the source dataframe for an initial load.
#     - **Parameters:**
#         - `df`: The source dataframe.
#     - **Returns:** The dataframe with added audit columns.
# 
# - `add_update_audit_columns(self, df: DataFrame) -> DataFrame`
#     - Adds audit columns to the source dataframe for an upsert operation.
#     - **Parameters:**
#         - `df`: The source dataframe.
#     - **Returns:** The dataframe with added audit columns.
# 
# - `upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None`
#     - Performs an upsert operation on a Delta table based on the provided configuration.
#     - **Parameters:**
#         - `config`: Configuration for upserting a Delta table.
#         - `storage_container_endpoint`: The endpoint of the storage container if using 'path' write method.
#         - `write_method`: The method to use for saving the table ('path' or 'catalog').
# 
# ---
# 
# ### Class: UpsertGeneric
# 
# #### Description
# This class implements a generic upsert strategy for Delta tables. It inherits from the abstract base class `UpsertStrategy`.
# 
# #### Methods
# 
# - `add_insert_audit_columns(self, df: DataFrame) -> DataFrame`
#     - Adds audit columns to the source dataframe for upsert.
#     - **Parameters:**
#         - `df`: The source dataframe.
#     - **Returns:** The dataframe with added audit columns.
# 
# - `upsert_to_table(self, config: Dict[str, Any], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None`
#     - Performs an upsert operation on a Delta table based on the provided configuration.
#     - **Parameters:**
#         - `config`: Configuration for upserting a Delta table.
#         - `storage_container_endpoint`: The endpoint of the storage container if using 'path' write method.
#         - `write_method`: The method to use for saving the table ('path' or 'catalog').
# 
# ---
# 
# ### Class: UpsertHandler
# 
# #### Description
# This class handles concurrent upsert operations on multiple tables.
# 
# #### Methods
# 
# - `__init__(self, spark, logger=None)`
#     - Initializes the handler with a Spark session, a logger, a file manager, and an optional transform manager.
#     - **Parameters:**
#         - `spark`: The Spark session.
#         - `logger`: The logger. If not provided, a default logger will be used.
# 
# - `upsert_data_concurrently(self, table_configs: List[Dict[str, str]], storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog') -> None`
#     - Performs upsert operations concurrently on multiple tables based on the provided configurations.
#     - **Parameters:**
#         - `table_configs`: A list of dictionaries, each representing a table configuration.
#         - `storage_container_endpoint`: The endpoint of the storage container if using 'path' write method.
#         - `write_method`: The method to use for saving the table ('path' or 'catalog').
#     - **Example:**
#         ```python
#         # Set source and target tables
#         source_table = 'bronze.packagetypes'
#         target_table = 'gold.dim_package_type'
# 
#         # Set target storage container endpoint
#         target_storage_container_endpoint = 'my_endpoint'
# 
#         # Set upsert match columns and surrogate key name
#         composite_columns = ['package_type_id']
#         primary_key_column = 'package_type_key'
# 
#         # Build stage dataframe
#         df_stage = spark.sql(f"""
#             SELECT 
#                 PackageTypeID       package_type_id
#                 ,PackageTypeName    package_type_name
#             FROM {source_table}
#         """)
# 
#         # Set upsert config
#         upsert_config = [
#             {
#                 "table_name": target_table,
#                 "dataframe": df_stage,
#                 "composite_columns": composite_columns,
#                 "upsert_type": "scd1",
#                 "primary_key_column": primary_key_column
#             }
#         ]
# 
#         # Perform upsert
#         utils.upsert_data_concurrently(upsert_config)
#         ```

# In[ ]:


# Install and initialize lucid-spark-utils
get_ipython().system('pip install "https://raw.githubusercontent.com/Lucid-Will/Lucid-Spark-Utils/main/dist/lucidsparkutils-1.0-py3-none-any.whl" --quiet 2>/dev/null')

import lucid_control_framework as lucid
utils = lucid.LucidUtils()


# In[ ]:


# Set source and target tables
source_table = 'bronze.packagetypes'
target_table = 'gold.dim_package_type'

# Set target storage container endpoint
target_storage_container_endpoint = 'my_endpoint'

# Set upsert match columns and surrogate key name
composite_columns = ['package_type_id']
primary_key_column = 'package_type_key'

# Build stage dataframe
df_stage = spark.sql(f"""
    SELECT 
        PackageTypeID       package_type_id
        ,PackageTypeName    package_type_name
    FROM {source_table}
""")

# Set upsert config
upsert_config = [
    {
        "table_name": target_table,
        "dataframe": df_stage,
        "composite_columns": composite_columns,
        "upsert_type": "scd1",
        "primary_key_column": primary_key_column
    }
]

# Perform upsert
utils.upsert_data_concurrently(upsert_config)


# In[ ]:


# Set source table
source_table = 'bronze.people'

# Set target table
target_table = 'gold.dim_salesperson'

# Set upsert match columns
composite_columns = ['salesperson_id']

# Set surrogate key name
primary_key_column = 'salesperson_key'

# Build stage dataframe
df_stage = spark.sql(f"""
    SELECT 
        PersonID            salesperson_id
        ,FullName           full_name
        ,EmailAddress       email_address
        ,PhoneNumber        phone_number
    FROM {source_table}
    WHERE IsSalesPerson = 1
""")

# Set upsert config
upsert_config = [
    {
        "table_name": target_table,
        "dataframe": df_stage,
        "composite_columns": composite_columns,
        "upsert_type": "scd2",
        "primary_key_column": primary_key_column
    }
]

# Perform upsert
utils.upsert_data_concurrently(upsert_config)


# In[ ]:


# Set target table
target_table = 'gold.fact_orders'

# Set upsert match columns
composite_columns = ['order_line_id']

# Set surrogate key name
primary_key_column = 'order_key'

# Build stage dataframe# Build stage
df_stage = spark.sql("""
    SELECT 
        OrderLineID     order_line_id
        ,COALESCE(s.salesperson_key, -1)    salesperson_key
        ,COALESCE(p.package_type_key, -1)   package_type_key
        ,COALESCE(st.stock_item_key, -1)    stock_item_key
        ,COALESCE(c.customer_key, -1)       customer_key
        ,Quantity       order_quantity
        ,UnitPrice      unit_price
        ,TaxRate        tax_rate
    FROM bronze.orderlines ol
    LEFT JOIN bronze.orders o ON o.orderid = ol.orderid
    LEFT JOIN gold.dim_salesperson s ON o.salespersonpersonid = s.salesperson_id
    LEFT JOIN gold.dim_package_type p ON ol.packagetypeid = p.package_type_id
    LEFT JOIN gold.dim_stock_item st ON ol.stockitemid = st.stock_item_id
    LEFT JOIN gold.dim_customer c ON o.customerid = c.customer_id
""")

# Set upsert config
upsert_config = [
    {
        "table_name": target_table,
        "dataframe": df_stage,
        "composite_columns": composite_columns,
        "upsert_type": "fact",
        "primary_key_column": primary_key_column
    }
]

# Perform upsert
utils.upsert_data_concurrently(upsert_config)

