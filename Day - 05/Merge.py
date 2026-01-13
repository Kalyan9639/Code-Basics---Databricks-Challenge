# Databricks notebook source
# MAGIC %md
# MAGIC # 1. What is a Merge (Upsert) Operation?
# MAGIC A merge operation in Delta Lake allows you to combine updates and insertions into an existing Delta table. This is commonly referred to as an "upsert" (update + insert). You use the DeltaTable.merge method in Python/Scala, or the MERGE INTO statement in SQL. The operation updates rows where a match is found and inserts new rows where no match exists
# MAGIC
# MAGIC # 2. How to perform Incremental Merge (Upsert)
# MAGIC Incremental merge means you only merge new or changed data (incremental data) into your target Delta table, rather than reprocessing the entire dataset. This is a common pattern in ETL pipelines.
# MAGIC
# MAGIC # 3. Incremental Merge Workflow
# MAGIC - **Identify new or changed data** (incremental data) since the last merge.
# MAGIC - **Prepare your source DataFrame or table** with only the incremental data.
# MAGIC - **Perform the merge** using the syntax above.
# MAGIC - **Optionally partition your table** (e.g., by date) to optimize queries and merges, especially if duplicates are only possible for recent data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/ecommerce/ecommerce_data

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Performing Incremental Merge:

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")
df.write.format("delta").mode("overwrite").save("/Volumes/workspace/ecommerce/ecommerce_data/delta/2019-Nov")

# COMMAND ----------

from delta.tables import *

delta_table = DeltaTable.forPath(spark, '/Volumes/workspace/ecommerce/ecommerce_data/delta/2019-Nov')
sourcedf = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")

# COMMAND ----------

# display(delta_table)   # can't display a deltatable object.
display(sourcedf)

# COMMAND ----------

delta_table.alias('target').merge(
    sourcedf.alias('source'), "target.user_id=source.user_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

