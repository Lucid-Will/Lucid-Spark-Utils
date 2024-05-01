"""
This package provides classes for different upsert strategies.

Classes:
    - UpsertFact: For upserting fact tables.
    - UpsertSCD1: For upserting Slowly Changing Dimension Type 1 tables.
    - UpsertSCD2: For upserting Slowly Changing Dimension Type 2 tables.
    - UpsertGeneric: For upserting tables with a generic strategy.
"""

from .upsert_fact import UpsertFact
from .upsert_generic import UpsertGeneric
from .upsert_scd1 import UpsertSCD1
from .upsert_scd2 import UpsertSCD2