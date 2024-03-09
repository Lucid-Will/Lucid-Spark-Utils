#!/usr/bin/env python
# coding: utf-8

# ## Lucid_SparkSessionManager
# 
# 
# 

# In[ ]:


import logging
from pyspark.sql import SparkSession


# In[ ]:


class SparkSessionManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark = SparkSession.builder.appName("LucidSparkManager").getOrCreate()

