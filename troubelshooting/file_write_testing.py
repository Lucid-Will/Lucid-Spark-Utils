#!/usr/bin/env python
# coding: utf-8

# ## File Write Testing
# 
# New notebook

# In[ ]:


get_ipython().system('pip install /lakehouse/default/Files/lucidsparkutils-1.0.1-py3-none-any.whl --q')


# In[ ]:


import lucid_control_framework as lucid
utils = lucid.LucidUtils()


# In[ ]:


# Load sample df
df = spark.sql('select * from bronze.customers')

# df.show(5)


# In[ ]:


###### Doesn't work with .whl installation to envt ######

# Set vars
storage_container_endpoint = 'my_storage_endpoint'
file_format = 'csv'
file_name = 'customers_pandas_test1'

# Write file
utils.write_file(df, file_name, storage_container_endpoint, file_format)

