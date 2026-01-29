# Databricks notebook source
!pip install kaggle

# COMMAND ----------

import os

os.environ['KAGGLE_USERNAME']= "YOUR_USER_NAME"
os.environ['KAGGLE_KEY']="YOUR_KAGGLE_KEY"

# COMMAND ----------

spark.sql(
    """
    create schema if not exists workspace.codebasics
    """
)

# COMMAND ----------

spark.sql("""
create volume if not exists workspace.codebasics.customer_spending
""")

# COMMAND ----------

# Download the Kaggle dataset to the specified volume and unzip
# !kaggle datasets download -d mountboy/online-store-customer-transactions-1m-rows -p /Volumes/workspace/codebasics/customer_spending --unzip

# COMMAND ----------

df = spark.read.table("codebasics.customer_spending_1_m")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Content
# MAGIC There are 11 features.
# MAGIC
# MAGIC - Transaction_date - Transaction date
# MAGIC - Transaction_ID - This is a unique transaction id
# MAGIC - Gender - Customer Gender
# MAGIC - Age - Customer Age
# MAGIC - Marital_status - Marital status about customer
# MAGIC - State_names - Customer location of State.
# MAGIC - Segment - Customer membership
# MAGIC - Employees_status - Customer employment status
# MAGIC - Payment_method - Payment method used by customer
# MAGIC - Referral - Customer coming from referral link or not
# MAGIC - Amount_spent - Amount spent by customer per transaction

# COMMAND ----------

df.count(),len(df.columns)

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F

display(df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]))

# COMMAND ----------

display(df.groupBy("State_names").count().orderBy(F.desc("count")))

# COMMAND ----------

spark.sql("""
          create volume if not exists workspace.codebasics.bronze
""")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/Volumes/workspace/codebasics/bronze/customer_spending")

# COMMAND ----------

display(spark.read.load("/Volumes/workspace/codebasics/bronze/customer_spending"))

# COMMAND ----------

