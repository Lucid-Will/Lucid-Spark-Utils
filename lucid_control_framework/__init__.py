"""
lucid_control_framework
-----------------------

A Python library providing a consolidated toolset for data engineering and orchestration.

This package provides a facade that simplifies access to the underlying modules. 

Modules:
    lucid - Contains the LucidFacade class, which provides a simplified interface to the underlying modules

Note: The underlying modules are not directly accessible when importing this package. To use the functionality they provide, use the methods of the LucidFacade class.
"""

from .utils import LucidUtils

# Create an instance of LucidUtils
lucid = LucidUtils()

# Expose the instance of LucidUtils at the top level
__all__ = ['utils']