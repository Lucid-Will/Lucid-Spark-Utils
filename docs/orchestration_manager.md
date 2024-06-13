## Orchestration Manager Documentation

### Overview

The `OrchestrationManager` class manages the orchestration for data processing. It provides methods to load orchestration configurations, build a Directed Acyclic Graph (DAG) for data processing, and log the execution results.

### Module Structure

#### Classes

1. **OrchestrationManager**

---

### Class: OrchestrationManager

#### Description

This class is used for orchestrating data processing tasks. It includes methods for loading orchestration configurations into a Delta table, building and executing a DAG, and logging execution results.

#### Methods

- `__init__(self, spark, logger=None)`
    - Initialize the OrchestrationManager object.
    - **Parameters:**
        - `spark`: The SparkSession object.
        - `logger`: The logger object. If not provided, a default logger will be used.

- `load_orchestration_config(self, control_table_name: str, orchestration_config: list, write_method: str = 'catalog', control_storage_container_endpoint: Optional[str] = None)`
    - Load the orchestration configuration into a DataFrame and save it as a Delta table.
    - **Parameters:**
        - `control_table_name`: The name of the control table.
        - `orchestration_config`: A list of dictionaries representing the orchestration configuration.
        - `write_method`: The method to use for writing the table. Should be either 'catalog' or 'path'. Defaults to 'catalog'.
        - `control_storage_container_endpoint`: The endpoint of the storage container where the orchestration configuration is stored. Optional and defaults to None.
    - **Example:**
        ```python
        orchestration_config = [
            {
                'notebook_name': 'Notebook1',
                'notebook_path': '/path/to/notebook1',
                'dependencies': '["Notebook2", "Notebook3"]',
                'parameters': '{"param1": "value1", "param2": "value2"}',
                'timeout_per_cell_seconds': 600,
                'retry_attempts': 3,
                'interval_between_retry_attempt_seconds': 60,
                'active': 1,
                'process_group': 1
            },
            {
                'notebook_name': 'Notebook2',
                'notebook_path': '/path/to/notebook2',
                'dependencies': '["Notebook3"]',
                'parameters': '{"param1": "value1", "param2": "value2"}',
                'timeout_per_cell_seconds': 300,
                'retry_attempts': 2,
                'interval_between_retry_attempt_seconds': 30,
                'active': 1,
                'process_group': 1
            }
        ]
        orchestration_manager.load_orchestration_config('control_table', orchestration_config)
        ```

- `build_and_execute_dag(self, control_table_name: str, process_group: int, read_method: str = 'catalog', control_storage_container_endpoint: Optional[str] = None) -> dict`
    - Build a Directed Acyclic Graph (DAG) for data processing based on the orchestration configuration.
    - **Parameters:**
        - `control_table_name`: The name of the control table.
        - `process_group`: The load group to use for building the DAG.
        - `read_method`: The method to use for reading the table. Can be either 'path' or 'catalog'.
        - `control_storage_container_endpoint`: The endpoint of the storage container where the orchestration configuration is stored.
    - **Returns:** A dictionary representing the DAG.
    - **Example:**
        ```python
        process_group = 1
        dag = orchestration_manager.build_and_execute_dag('control_table', process_group)
        ```

- `log_orchestration_execution(self, log_table_name: str, execution_results: dict, write_method: str = 'catalog', control_storage_container_endpoint: Optional[str] = None)`
    - Log the execution results into a DataFrame and save it as a Delta table.
    - **Parameters:**
        - `log_table_name`: The name of the control table.
        - `execution_results`: A dictionary representing the execution results.
        - `write_method`: The method to use for writing the table. Can be either 'path' or 'catalog'.
        - `control_storage_container_endpoint`: The endpoint of the storage container where the orchestration log is stored.
    - **Example:**
        ```python
        execution_results = {
            'Notebook1': {'exception': None},
            'Notebook2': {'exception': 'Some error message'}
        }
        orchestration_manager.log_orchestration_execution('log_table', execution_results, 'catalog')
        ```