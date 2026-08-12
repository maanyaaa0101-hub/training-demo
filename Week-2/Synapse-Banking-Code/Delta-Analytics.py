#!/usr/bin/env python
# coding: utf-8

# ## Delta-Analytics
# 
# 
# 

# In[105]:


gold_path = "abfss://banking@banktraining.dfs.core.windows.net/gold"
analytics_path = "abfss://banking@banktraining.dfs.core.windows.net/analytics"


# In[106]:


dim_customers = spark.read.parquet(
    gold_path + "/dim_customers.parquet"
)

dim_accounts = spark.read.parquet(
    gold_path + "/dim_accounts.parquet"
)

dim_branches = spark.read.parquet(
    gold_path + "/dim_branches.parquet"
)

dim_date = spark.read.parquet(
    gold_path + "/dim_date.parquet"
)

fact_transactions = spark.read.parquet(
    gold_path + "/fact_transactions.parquet"
)


# In[107]:


mssparkutils.fs.mkdirs(analytics_path)

dim_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(analytics_path + "/dim_customers")

dim_accounts.write \
    .format("delta") \
    .mode("overwrite") \
    .save(analytics_path + "/dim_accounts")

dim_branches.write \
    .format("delta") \
    .mode("overwrite") \
    .save(analytics_path + "/dim_branches")

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .save(analytics_path + "/dim_date")

fact_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .save(analytics_path + "/fact_transactions")
    


# In[ ]:




