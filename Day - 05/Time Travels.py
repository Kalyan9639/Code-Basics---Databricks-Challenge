# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructField, StructType

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstname", StringType(), True),
  StructField("middlename", StringType(), True),
  StructField("lastname", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthdate", TimestampType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

df = spark.read.format("csv").option("header",True).schema(schema).load("/Volumes/workspace/default/my-volume/export.csv")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating the salary of a person in the table:
# MAGIC
# MAGIC In order to perform any modification, we have to use deltatable

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

delta_table = DeltaTable.forName(spark,'default.people_10m')

# update pennie's salary
delta_table.update("firstname = 'Pennie'",{'salary':'70000'})

display(delta_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Since delta_table can't be viewed directly, i am first saving the table to the variable in which the change has been made. Then, I will display the data record which is updated

# COMMAND ----------

df_delta = spark.read.table("default.people_10m")

# COMMAND ----------

df_delta.filter(df_delta.firstname=='Pennie').show()

# COMMAND ----------

display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert a new record to the table:

# COMMAND ----------

from datetime import datetime

new_data = [(1001,"Kalyan","Sai","Prasad","M",datetime(2006, 10, 12),"123-45-6789",99999999)]
new_df = spark.createDataFrame(new_data,schema=df_delta.schema)
new_df.write.mode("append").saveAsTable("workspace.default.people_10m")

# COMMAND ----------

if new_df.printSchema()==df_delta.printSchema():
    print("True")

# COMMAND ----------

display(df_delta)

# COMMAND ----------

df_delta.filter(df_delta.id==1001).show()

# COMMAND ----------

display(delta_table.history())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travelling:
# MAGIC **1. Accessing the previous version of the table**

# COMMAND ----------

df_v07 = spark.read.format("delta").option("versionAsOf",7).table("workspace.default.people_10m")

display(df_v07)

# COMMAND ----------

df_v07.filter(df_v07.id == 1001).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Time Travelling using TimeStamp**:
# MAGIC
# MAGIC Rolling back to the original dataset i.e., first version where Pennie salary is not updated to 70000

# COMMAND ----------

display(spark.read.format("delta").option("timestampAsOf", "2026-01-13").table('default.people_10m'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Important Note:
# MAGIC
# MAGIC 1. Use `.table()` when you want to read a Unity Catalog managed or external table by its catalog-qualified name, such as "workspace.default.people_10m". This is the recommended approach for Unity Catalog tables, especially when using features like time travel or schema enforcement.
# MAGIC 2. Use `.load()` when you want to read data from an absolute file path, such as a location in a volume or external storage. This is useful for reading raw files (Parquet, CSV, Delta, etc.) that are not registered as tables in Unity Catalog.
# MAGIC 3. For Unity Catalog tables: use `.table("catalog.schema.table")`.
# MAGIC 4. For files in volumes or external locations: use `.load("/Volumes/catalog/schema/volume/path")` or `.load("abspath").`
# MAGIC
# MAGIC **For writing:**
# MAGIC - Use `.saveAsTable()` to write DataFrames to Unity Catalog tables.
# MAGIC - Use `.save()` to write DataFrames to a file path (such as a volume).
# MAGIC
# MAGIC Volumes are for file management and direct file access. Tables are for structured, governed data access with Delta Lake features

# COMMAND ----------

