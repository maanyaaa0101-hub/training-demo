#!/usr/bin/env python
# coding: utf-8

# ## Silver-transform
# 
# 
# 

# In[86]:


from pyspark.sql.functions import *


customers_path = "abfss://banking@banktraining.dfs.core.windows.net/bronze/customers.json"

silver_path = "abfss://banking@banktraining.dfs.core.windows.net/silver"



customers_df = (
    spark.read
    .option("multiLine", True)
    .json(customers_path)
)


customers_silver = (
    customers_df

    # Remove completely duplicated records
    .dropDuplicates()

    # Clean text columns
    .withColumn("customer_id", upper(trim(col("customer_id"))))
    .withColumn("first_name", upper(trim(col("first_name"))))
    .withColumn("last_name", upper(trim(col("last_name"))))
    .withColumn("email", upper(trim(col("email"))))
    .withColumn("city", upper(trim(col("city"))))
    .withColumn("state", upper(trim(col("state"))))
    .withColumn("customer_segment", upper(trim(col("customer_segment"))))

    # Convert JSON string dates into DATE
    .withColumn("date_of_birth", to_date(col("date_of_birth")))
    .withColumn("registration_date", to_date(col("registration_date")))

    # Basic required-field validation
    .filter(col("customer_id").isNotNull())
)


customers_silver.show(5)

customers_silver.printSchema()

#creating and uploading parquet file to folder

temp_path = silver_path + "/_temp_customers"

customers_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)


files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            silver_path + "/customers_cleaned.parquet"
            False,
            True
        )


mssparkutils.fs.rm(temp_path, True)



# **ACCOUNTS**

# In[87]:


accounts_path = "abfss://banking@banktraining.dfs.core.windows.net/bronze/accounts.csv"

silver_path = "abfss://banking@banktraining.dfs.core.windows.net/silver"

accounts_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(accounts_path)
)


accounts_silver = (
    accounts_df

    # Remove duplicate records
    .dropDuplicates()

    # Clean text columns
    .withColumn("account_id", upper(trim(col("account_id"))))
    .withColumn("customer_id", upper(trim(col("customer_id"))))
    .withColumn("account_type", upper(trim(col("account_type"))))
    .withColumn("account_status", upper(trim(col("account_status"))))

    # Required-field validation
    .filter(col("account_id").isNotNull())
    .filter(col("customer_id").isNotNull())
)


accounts_silver.show(5)

accounts_silver.printSchema()


temp_path = silver_path + "/_temp_accounts"

accounts_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)



files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            silver_path + "/accounts_cleaned.parquet"
            False,
            True
        )


mssparkutils.fs.rm(temp_path, True)


# In[88]:


branches_path = "abfss://banking@banktraining.dfs.core.windows.net/bronze/branches.csv"

branches_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(branches_path)
)

branches_silver = (
    branches_df
    .dropDuplicates()
    .withColumn("branch_id", upper(trim(col("branch_id"))))
    .withColumn("branch_name", upper(trim(col("branch_name"))))
    .withColumn("city", upper(trim(col("city"))))
    .withColumn("state", upper(trim(col("state"))))
    .filter(col("branch_id").isNotNull())
)

silver_path = "abfss://banking@banktraining.dfs.core.windows.net/silver"
temp_path = silver_path + "/_temp_branches"

branches_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            silver_path + "/branches_cleaned.parquet"
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# In[91]:


transactions_path = "abfss://banking@banktraining.dfs.core.windows.net/bronze/transactions.csv"

transactions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(transactions_path)
)

transactions_silver = (
    transactions_df
    .dropDuplicates()
    .withColumn("transaction_id", upper(trim(col("transaction_id"))))
    .withColumn("account_id", upper(trim(col("account_id"))))
    .withColumn("transaction_type", upper(trim(col("transaction_type"))))
    .withColumn("payment_method", upper(trim(col("payment_method"))))
    .withColumn("transaction_date", to_date(col("transaction_date")))
    .filter(col("transaction_id").isNotNull())
    .filter(col("account_id").isNotNull())
)

silver_path = "abfss://banking@banktraining.dfs.core.windows.net/silver"
temp_path = silver_path + "/_temp_transactions"

transactions_silver.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(temp_path)

files = mssparkutils.fs.ls(temp_path)

for file in files:
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(
            file.path,
            silver_path + "/transactions_cleaned.parquet",
            False,
            True
        )

mssparkutils.fs.rm(temp_path, True)


# In[ ]:




