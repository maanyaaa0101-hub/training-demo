#!/usr/bin/env python
# coding: utf-8

# ## Data-quality-gold
# 
# 
# 

# In[7]:


from pyspark.sql.functions import *


# In[8]:


gold_path = "abfss://banking@banktraining.dfs.core.windows.net/gold"

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

print("Gold data loaded successfully")


# In[9]:


dq_failures = []

customer_duplicates = (
    dim_customers
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

account_duplicates = (
    dim_accounts
    .groupBy("account_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

branch_duplicates = (
    dim_branches
    .groupBy("branch_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

date_duplicates = (
    dim_date
    .groupBy("date_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

transaction_duplicates = (
    fact_transactions
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

customer_nulls = dim_customers.filter(
    col("customer_id").isNull()
).count()

account_nulls = dim_accounts.filter(
    col("account_id").isNull()
).count()

branch_nulls = dim_branches.filter(
    col("branch_id").isNull()
).count()

date_key_nulls = dim_date.filter(
    col("date_key").isNull()
).count()

transaction_id_nulls = fact_transactions.filter(
    col("transaction_id").isNull()
).count()

account_id_nulls = fact_transactions.filter(
    col("account_id").isNull()
).count()

date_key_fact_nulls = fact_transactions.filter(
    col("date_key").isNull()
).count()

if customer_duplicates > 0:
    dq_failures.append(
        f"dim_customers: {customer_duplicates} duplicate customer_id values"
    )

if account_duplicates > 0:
    dq_failures.append(
        f"dim_accounts: {account_duplicates} duplicate account_id values"
    )

if branch_duplicates > 0:
    dq_failures.append(
        f"dim_branches: {branch_duplicates} duplicate branch_id values"
    )

if date_duplicates > 0:
    dq_failures.append(
        f"dim_date: {date_duplicates} duplicate date_key values"
    )

if transaction_duplicates > 0:
    dq_failures.append(
        f"fact_transactions: {transaction_duplicates} duplicate transaction_id values"
    )

if customer_nulls > 0:
    dq_failures.append(
        f"dim_customers: {customer_nulls} NULL customer_id values"
    )

if account_nulls > 0:
    dq_failures.append(
        f"dim_accounts: {account_nulls} NULL account_id values"
    )

if branch_nulls > 0:
    dq_failures.append(
        f"dim_branches: {branch_nulls} NULL branch_id values"
    )

if date_key_nulls > 0:
    dq_failures.append(
        f"dim_date: {date_key_nulls} NULL date_key values"
    )

if transaction_id_nulls > 0:
    dq_failures.append(
        f"fact_transactions: {transaction_id_nulls} NULL transaction_id values"
    )

if account_id_nulls > 0:
    dq_failures.append(
        f"fact_transactions: {account_id_nulls} NULL account_id values"
    )

if date_key_fact_nulls > 0:
    dq_failures.append(
        f"fact_transactions: {date_key_fact_nulls} NULL date_key values"
    )

if dq_failures:
    print("GOLD DATA QUALITY VALIDATION FAILED")
    print("------------------------------------")

    for failure in dq_failures:
        print(failure)

    raise Exception(
        "Pipeline stopped because Gold data quality validation failed."
    )

print("GOLD DATA QUALITY VALIDATION PASSED")


# In[ ]:




