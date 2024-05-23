"""
This package provides classes for different upsert strategies.

Classes:
    - UpsertFact: For upserting fact tables. This class implements the logic for upserting data into fact tables.
    - UpsertSCD1: For upserting Slowly Changing Dimension Type 1 tables. This class implements the logic for upserting data into SCD Type 1 tables.
    - UpsertSCD2: For upserting Slowly Changing Dimension Type 2 tables. This class implements the logic for upserting data into SCD Type 2 tables.
    - UpsertStrategy: This is the base class for all upsert strategies. It provides the basic structure that all upsert strategies should follow.
"""

from .upsert_fact import UpsertFact
from .upsert_scd1 import UpsertSCD1
from .upsert_scd2 import UpsertSCD2
from .upsert_strategy import UpsertStrategy