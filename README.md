# Lucid Spark Utils

Lucid Spark Utils is an open-source Python package developed to streamline and enhance the process of working with Spark DataFrames and Delta tables. It aims to provide a comprehensive set of utility functions that simplify common tasks and operations, improving efficiency and productivity.

## Features

- **Data Validation Manager**: Validates data in Spark DataFrames and Delta tables.
- **Delta Table Manager**: Manages Delta tables in Spark, simplifying the process of creating and updating Delta tables.
- **File Manager**: Streamlines file operations in Spark, making it easier to read, write, and manage files.
- **Orchestration Manager**: Manages the orchestration of tasks in Spark using DAGs.
- **Semantic Model Manager**: Manages semantic models activities such as semantic model refresh operations in Spark.
- **Transformation Manager**: Simplifies the process of transforming data with in Spark, making it easier to apply key fields to DataFrames before loading to Delta tables.
- **Upsert Strategy**: Provides a strategy for upserting data in Spark, simplifying loading patterns using SCD and Fact tables loads.
- **Utility Manager**: Provides a variety of utility functions for Spark, making it easier to perform common tasks and operations.

## Installation

Lucid Spark Utils can be installed directly on an engineering cluster or in-line in a notebook.

### On an Engineering Cluster

To install the package on an engineering cluster, you will need to install the `.whl` file on your Spark cluster. Once the `.whl` file is installed, you can use the `import` command:

```python
import lucid_spark_utils as lucid
utils = lucid.LucidUtils()
```

### In a Notebook

To install the package in-line in a notebook, you can use the `!pip install` command with the direct URL to the `.whl` file in the GitHub repository:

```python
!pip install "https://raw.githubusercontent.com/Lucid-Will/Lucid-Spark-Utils/main/dist/lucidsparkutils-1.0-py3-none-any.whl" --quiet 2>/dev/null
```
Once installed, you can use the `import` command:

```python
import lucid_spark_utils as lucid
utils = lucid.LucidUtils()
```

## Main Facade and Docstrings

Lucid Spark Utils is designed with a main facade that provides access to all the utility functions. Once you've imported the package, you can access any function directly from the main facade.

In Python, you can access the docstrings of functions and classes for more information about their usage. After importing a function or class, you can view its docstring by using the `help()` function:

```python
import lucid_control_framework as lucid
utils = lucid.LucidUtils()
help(utils)
```

## Documentation

Detailed documentation for each module in the Lucid Spark Utils library can be found in the `docs` directory of the GitHub repository. Each module has its own markdown file with a detailed explanation of its functionality, usage examples, and any additional notes or considerations.

Here are the direct links to the documentation for each module:

- [Data Validation Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/data_validation_manager.md)
- [Delta Table Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/delta_table_manager.md)
- [File Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/file_manager.md)
- [Orchestration Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/orchestration_manager.md)
- [Semantic Model Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/semantic_model_manager.md)
- [Transformation Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/transformation_manager.md)
- [Upsert Strategy](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/upsert_strategy.md)
- [Utility Manager](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/docs/utility_manager.md)

## Contributions

Contributions to Lucid Spark Utils are very welcome! If you have a feature request, bug report, or proposal, please open an issue on the GitHub repository. If you wish to contribute code, please fork the repository, make your changes, and submit a pull request.

## License

Lucid Spark Utils is licensed under the terms of the CC BY-NC license. See the [LICENSE](https://github.com/Lucid-Will/Lucid-Spark-Utils/blob/main/LICENSE) file for more details.
