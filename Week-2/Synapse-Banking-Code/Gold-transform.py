#!/usr/bin/env python
# coding: utf-8

# ## Gold-transform
# 
# 
# 

# In[92]:


from pyspark.sql.functions import *


# **CUSTOMERS**

# In[93]:


silver_path = "abfss://banking@banktraining.dfs.core.windows.net/silver"
gold_path = "abfss://banking@banktraining.dfs.core.windows.net/gold"

customers_silver = spark.read.parquet(
    silver_path + "/customers_cleaned.parquet"
)

dim_customers = (
    customers_silver
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "customer_segment",
        "date_of_birth",
        "registration_date"
    )
    .dropDuplicates(["customer_id"])
)

temp_path = gold_path + "/_temp_dim_customers"

dim_customers.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            gold_path + "/dim_customers.parquet",
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# **ACCOUNTS**

# In[94]:


accounts_silver = spark.read.parquet(
    silver_path + "/accounts_cleaned.parquet"
)

dim_accounts = (
    accounts_silver
    .select(
        "account_id",
        "customer_id",
        "account_type",
        "account_status"
    )
    .dropDuplicates(["account_id"])
)

temp_path = gold_path + "/_temp_dim_accounts"

dim_accounts.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            gold_path + "/dim_accounts.parquet",
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# **BRANCHES**

# In[95]:


branches_silver = spark.read.parquet(
    silver_path + "/branches_cleaned.parquet"
)

dim_branches = (
    branches_silver
    .select(
        "branch_id",
        "branch_name",
        "city",
        "state"
    )
    .dropDuplicates(["branch_id"])
)

temp_path = gold_path + "/_temp_dim_branches"

dim_branches.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            gold_path + "/dim_branches.parquet",
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# **DATE DIMENSION FROM TRANSACTIONS**

# In[96]:


transactions_silver = spark.read.parquet(
    silver_path + "/transactions_cleaned.parquet"
)

dim_date = (
    transactions_silver
    .select("transaction_date")
    .filter(col("transaction_date").isNotNull())
    .dropDuplicates()
    .withColumnRenamed("transaction_date", "full_date")
    .withColumn(
        "date_key",
        date_format(col("full_date"), "yyyyMMdd").cast("int")
    )
    .withColumn("year", year("full_date"))
    .withColumn("month", month("full_date"))
    .withColumn("day", dayofmonth("full_date"))
    .withColumn(
        "month_name",
        date_format(col("full_date"), "MMMM")
    )
    .withColumn(
        "day_name",
        date_format(col("full_date"), "EEEE")
    )
)

temp_path = gold_path + "/_temp_dim_date"

dim_date.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            gold_path + "/dim_date.parquet",
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# **TRANSACTIONS**

# In[97]:


transactions_silver = spark.read.parquet(
    silver_path + "/transactions_cleaned.parquet"
)

fact_transactions = (
    transactions_silver
    .select(
        "transaction_id",
        "account_id",
        "transaction_date",
        "transaction_type",
        "payment_method",
        "amount"
    )
    .withColumn(
        "date_key",
        date_format(
            col("transaction_date"),
            "yyyyMMdd"
        ).cast("int")
    )
)

temp_path = gold_path + "/_temp_fact_transactions"

fact_transactions.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            gold_path + "/fact_transactions.parquet",
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# In[ ]:




