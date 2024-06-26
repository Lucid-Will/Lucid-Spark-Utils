
## Validation Module Documentation

### Overview

The `validation` module provides methods for validating data in Spark, including logging validation results and handling invalid, duplicate, and delete flagged records. This module is designed to streamline data validation processes and ensure data integrity.

### Module Structure

#### Classes

1. **Validation**

---

### Class: Validation

#### Description
This class is used for validating and testing data in Spark. It includes methods for logging table validation results.

#### Methods

- `__init__(self, spark, logger=None)`
    - Initialize the Validation object.
    - **Parameters:**
        - `spark`: The SparkSession object.
        - `logger`: The logger object. If not provided, a default logger will be used.

- `log_table_validation(self, target_table_name: str, log_table_name: str, read_method: str = 'catalog', write_method: str = 'catalog', target_storage_container_endpoint: Optional[str] = None, log_storage_container_endpoint: Optional[str] = None, primary_key_column: Optional[str] = None, stage_count: Optional[int] = None, invalid_count: Optional[int] = None, duplicate_count: Optional[int] = None, delete_count: Optional[int] = None) -> None`
    - Log the validation results for a table.
    - **Parameters:**
        - `target_table_name`: The name of the target table.
        - `log_table_name`: The name of the log table.
        - `read_method`: The method used to read the target table (catalog or path).
        - `write_method`: The method used to write the log table (catalog or path).
        - `target_storage_container_endpoint`: The storage container endpoint of the target table.
        - `log_storage_container_endpoint`: The storage container endpoint of the log table.
        - `primary_key_column`: The primary key column used for filtering.
        - `stage_count`: The count of rows in the staging DataFrame.
        - `invalid_count`: Count of invalid records.
        - `duplicate_count`: Count of duplicate records.
        - `delete_count`: Count of records flagged for deletion.
    - **Returns:** None. The function logs the validation results into the specified log table.
    - **Example:**
        ```python
        log_table_validation(
            "storage_container_endpoint", 
            "my_table", 
            "log_storage_container_endpoint", 
            "log_table", 
            "id", 
            100, 
            5, 
            10, 
            3
        )
        ```

- `data_validation_check(self, df_stage: DataFrame, target_table_name: str, target_storage_container_endpoint: str, composite_columns: List[str], read_method: str = 'catalog', write_method: str = 'catalog', primary_key_column: Optional[str] = None, dropped_validation_columns: Optional[List[str]] = None) -> None`
    - Identify invalid, duplicate, and delete flagged records by identifying them, saving them to specified paths, and returning a filtered DataFrame along with counts of invalid, duplicate, and delete flagged records.
    - **Parameters:**
        - `df_stage`: The staging DataFrame.
        - `target_table_name`: The name of the table being processed and the target table to check for delete flagged records.
        - `target_storage_container_endpoint`: The endpoint for the storage account.
        - `composite_columns`: List of columns to check for invalid values and duplicates and form the composite key.
        - `read_method`: The method used to read the target table (catalog or path).
        - `write_method`: The method used to write the log table (catalog or path).
        - `primary_key_column`: The primary key column used for identifying records in the target table.
        - `dropped_validation_columns`: List of columns to drop from the final DataFrame after validation.
    - **Returns:** A tuple containing the filtered DataFrame, count of invalid records, count of duplicate records, and count of delete flagged records.
    - **Example:**
        ```python
        data_validation_check(df_stage, "my_table", ["id", "name"], "mydatalake", "id", ["created_at", "updated_at"])
        ```

- `hard_delete_records(self, target_table_name: str, primary_key_column: str, write_method: str = 'catalog', target_table_storage_container_endpoint: Optional[str] = None, df_delete=None)`
    - Perform hard deletes from the target table for records identified as delete.
    - **Parameters:**
        - `target_table_name`: The name of the target table.
        - `primary_key_column`: The primary key column used for identifying records in the target table.
        - `write_method`: The method used to write the log table (catalog or path).
        - `target_table_storage_container_endpoint`: The storage container endpoint of the target table.
        - `df_delete`: DataFrame of records flagged for deletion.
    - **Returns:** None. The function performs hard deletes on the target table.
    - **Example:**
        ```python
        hard_delete_records('schema.my_table', 'id', 'path', 'target_table_storage_container_endpoint', df_delete)
        ```

- `soft_delete_records(self, target_table_name: str, primary_key_column: str, read_method: str = 'catalog', target_table_storage_container_endpoint: Optional[str] = None, df_delete=None)`
    - Perform soft deletes from the target table for records identified as delete flagged by setting the is_deleted column to True. If the is_deleted column does not exist, it will be added to the target table.
    - **Parameters:**
        - `target_table_name`: The name of the target table.
        - `primary_key_column`: The primary key column used for identifying records in the target table.
        - `read_method`: The method used to read the target table (catalog or path).
        - `target_table_storage_container_endpoint`: The storage container endpoint of the target table.
        - `df_delete`: DataFrame of records flagged for deletion.
    - **Returns:** None. The function performs soft deletes on the target table.
    - **Example:**
        ```python
        soft_delete_records("storage_container_endpoint", "my_table", "id", df_delete)
        ```