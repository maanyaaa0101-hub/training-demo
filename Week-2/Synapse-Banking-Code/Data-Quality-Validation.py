#!/usr/bin/env python
# coding: utf-8

# ## Data-Quality-Validation
# 
# 
# 

# In[98]:


from pyspark.sql.functions import *


# In[99]:


silver_path = "abfss://banking@banktraining.dfs.core.windows.net/silver"

customers_silver = spark.read.parquet(
    silver_path + "/customers_cleaned.parquet"
)

accounts_silver = spark.read.parquet(
    silver_path + "/accounts_cleaned.parquet"
)

branches_silver = spark.read.parquet(
    silver_path + "/branches_cleaned.parquet"
)

transactions_silver = spark.read.parquet(
    silver_path + "/transactions_cleaned.parquet"
)


# In[100]:


print("Customers:", customers_silver.count())
print("Accounts:", accounts_silver.count())
print("Branches:", branches_silver.count())
print("Transactions:", transactions_silver.count())


# In[104]:


dq_failures = []

customer_nulls = customers_silver.filter(
    col("customer_id").isNull()
).count()

customer_duplicates = (
    customers_silver
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

account_id_nulls = accounts_silver.filter(
    col("account_id").isNull()
).count()

account_customer_nulls = accounts_silver.filter(
    col("customer_id").isNull()
).count()

account_duplicates = (
    accounts_silver
    .groupBy("account_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

branch_id_nulls = branches_silver.filter(
    col("branch_id").isNull()
).count()

branch_duplicates = (
    branches_silver
    .groupBy("branch_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

transaction_id_nulls = transactions_silver.filter(
    col("transaction_id").isNull()
).count()

transaction_account_nulls = transactions_silver.filter(
    col("account_id").isNull()
).count()

transaction_date_nulls = transactions_silver.filter(
    col("transaction_date").isNull()
).count()

transaction_duplicates = (
    transactions_silver
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if customer_nulls > 0:
    dq_failures.append(
        f"Customers: {customer_nulls} NULL customer_id values"
    )

if customer_duplicates > 0:
    dq_failures.append(
        f"Customers: {customer_duplicates} duplicate customer_id values"
    )

if account_id_nulls > 0:
    dq_failures.append(
        f"Accounts: {account_id_nulls} NULL account_id values"
    )

if account_customer_nulls > 0:
    dq_failures.append(
        f"Accounts: {account_customer_nulls} NULL customer_id values"
    )

if account_duplicates > 0:
    dq_failures.append(
        f"Accounts: {account_duplicates} duplicate account_id values"
    )

if branch_id_nulls > 0:
    dq_failures.append(
        f"Branches: {branch_id_nulls} NULL branch_id values"
    )

if branch_duplicates > 0:
    dq_failures.append(
        f"Branches: {branch_duplicates} duplicate branch_id values"
    )

if transaction_id_nulls > 0:
    dq_failures.append(
        f"Transactions: {transaction_id_nulls} NULL transaction_id values"
    )

if transaction_account_nulls > 0:
    dq_failures.append(
        f"Transactions: {transaction_account_nulls} NULL account_id values"
    )

if transaction_date_nulls > 0:
    dq_failures.append(
        f"Transactions: {transaction_date_nulls} NULL transaction_date values"
    )

if transaction_duplicates > 0:
    dq_failures.append(
        f"Transactions: {transaction_duplicates} duplicate transaction_id values"
    )

if dq_failures:
    print("DATA QUALITY VALIDATION FAILED")
    print("--------------------------------")
    for failure in dq_failures:
        print(failure)
    raise Exception("Pipeline stopped because data quality validation failed.")

print("DATA QUALITY VALIDATION PASSED")


# In[ ]:




