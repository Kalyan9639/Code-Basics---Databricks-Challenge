# Databricks notebook source
!pip install kaggle

# COMMAND ----------

# Configure Kaggle Credentials
import os

os.environ['KAGGLE_USERNAME']='kalyansaiprasad'
os.environ['KAGGLE_KEY']='0bbb6fe7dfd4961679dcd514b2027e6d'

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### <u>Spark or PySpark</u>:
# MAGIC
# MAGIC *Spark is a distributed data processing engine designed for big data analytics.It enables fast computation on large datasets by distributing tasks across multiple nodes in a cluster. Spark is commonly used for data engineering, data science, and machine learning workflows. In this code, we use Spark to execute SQL commands for managing schemas and tables in a scalable way within Databricks.*
# MAGIC

# COMMAND ----------

# create database schema
spark.sql("""
create schema if not exists workspace.ecommerce          
"""
)

# COMMAND ----------

# create volume for data storage
spark.sql(
    """
    create volume if not exists workspace.ecommerce.ecommerce_data
    """
)

# COMMAND ----------

# change the directory to the volume directory
cd /Volumes/workspace/ecommerce/ecommerce_data

# COMMAND ----------

# download the dataset in this directory
!kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store

# COMMAND ----------

# MAGIC %md
# MAGIC **Extracting the downloaded dataset**

# COMMAND ----------

cd /Volumes/workspace/ecommerce/ecommerce_data

# COMMAND ----------

!unzip -o ecommerce-behavior-data-from-multi-category-store.zip

# COMMAND ----------

!ls -lh

# COMMAND ----------

# MAGIC %md
# MAGIC **Deleting the zip file**

# COMMAND ----------

cd /Volumes/workspace/ecommerce/ecommerce_data

# COMMAND ----------

rm -f ecommerce-behavior-data-from-multi-category-store.zip

# COMMAND ----------

ls -lh

# COMMAND ----------

# restarting the python environment
%restart_python

# COMMAND ----------

df_n = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")
df_n.head()

# COMMAND ----------

# display the dataset
display(df_n.select(df_n.columns))

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")
df.head()

# COMMAND ----------

# display the dataset
display(df.select(df.columns[:5]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Why Pandas `shape()` is simpler and faster than Spark?
# MAGIC
# MAGIC In PySpark, DataFrames do not have a `shape` attribute like pandas DataFrames. To get the number of rows, use .count(). To get the number of columns, use len(df.columns). This is because Spark DataFrames are distributed and do not store all data in memory like pandas.

# COMMAND ----------

num_rows_df = df.count()
num_cols_df = len(df.columns)
num_rows_df_n = df_n.count()
num_cols_df_n = len(df_n.columns)

# Display the shape as (num_rows, num_columns) for both DataFrames
print("df shape:", (num_rows_df, num_cols_df))
print("df_n shape:", (num_rows_df_n, num_cols_df_n))

# COMMAND ----------

type(df)

# COMMAND ----------

# display dataset statistics and schema
print(f"October 2019 - Total Events: {df.count():,}")
print("\n" + "="*60)
print("SCHEMA:")
print("="*60)
df.printSchema()

# COMMAND ----------

# display sample data
print("\n" + "="*60)
print("SAMPLE DATA (First 5 rows):")
print("="*60)
df.show(5, truncate=False)

# COMMAND ----------

