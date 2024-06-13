## Transformation Manager Documentation

### Overview

The `TransformationManager` class provides methods for transforming data in Spark DataFrames. It includes methods for staging DataFrames with surrogate keys and executing multiple transformations concurrently.

### Module Structure

#### Classes

1. **TransformationManager**

---

### Class: TransformationManager

#### Description

This class is used for managing data transformations in Spark DataFrames. It includes methods for staging DataFrames with surrogate keys and executing multiple transformations concurrently.

#### Methods

- `__init__(self, spark, logger=None)`
    - Initialize the TransformationManager object.
    - **Parameters:**
        - `spark`: The SparkSession object.
        - `logger`: The logger object. If not provided, a default logger will be used.

- `stage_dataframe_with_keys(self, target_table: str, dataframe: DataFrame, primary_key_column: Optional[str] = None, composite_key_column: Optional[str] = None, composite_columns: Optional[List[str]] = None, read_method: str = 'catalog', target_table_storage_container_endpoint: Optional[str] = None) -> Optional[DataFrame]`
    - Transforms a DataFrame by adding a new column with an integer hash based on specified key columns. It also adds a surrogate key column with values starting from the maximum key in the target table plus one.
    - **Parameters:**
        - `target_table`: The target table to check for the maximum key.
        - `dataframe`: The source DataFrame.
        - `primary_key_column`: The name of the new surrogate key column to be added.
        - `composite_key_column`: The name of the new natural key column to be added.
        - `composite_columns`: List of columns to use for hash generation.
        - `read_method`: The method to use for reading the target table. Can be either 'path' or 'catalog'.
        - `target_table_storage_container_endpoint`: The storage container endpoint for the target table. Required if read_method is 'path'.
    - **Returns:** Transformed DataFrame with the new columns added, if specified.
    - **Raises:**
        - `ValueError`: If an invalid read method is provided.
        - `Exception`: If there's a problem transforming the DataFrame.
    - **Example:**
        ```python
        df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
        df_transformed = transformation_manager.stage_dataframe_with_keys("target_table", df, primary_key_column="skey", composite_key_column="nkey", composite_columns=["ID", "First Name"], read_method="catalog")
        ```

- `execute_transformations_concurrently(self, transformations: List[Tuple[Callable, Tuple]]) -> List`
    - Executes multiple DataFrame transformation tasks concurrently, improving performance on multi-core systems.
    - **Parameters:**
        - `transformations`: A list of tuples, where each tuple contains a transformation function and its corresponding arguments.
    - **Returns:** A list of results from each transformation task, executed concurrently.
    - **Raises:** `Exception`: If there's a problem executing the transformations concurrently.
    - **Example:**
        ```python
        transformations = [
            (transformation_manager.stage_dataframe_with_keys, ("target_table1", df1, "skey", "nkey", ["ID", "First Name"], "catalog")),
            (transformation_manager.stage_dataframe_with_keys, ("target_table2", df2, "skey", "nkey", ["ID", "First Name"], "catalog"))
        ]
        results = transformation_manager.execute_transformations_concurrently(transformations)
        ```