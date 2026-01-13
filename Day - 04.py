# Databricks notebook source
# MAGIC %md
# MAGIC # Delta and Delta Lake Tutorial:
# MAGIC
# MAGIC ### 1. Delta Lake
# MAGIC Delta Lake is an open-source storage layer that turns that pile into an organized, high-tech library. It stores your data as Parquet files but adds a Transaction Log (the _delta_log folder). This log tracks every single change made to the table, making it reliable and fast.
# MAGIC
# MAGIC ### 2. Delta Table
# MAGIC It is a special kind of table based on the Delta Lake open source project. Here’s what makes a Delta table unique:
# MAGIC
# MAGIC 1. Storage: A Delta table stores data as a directory of files (usually in Parquet format) on cloud object storage, not as a single file like a CSV.
# MAGIC 2. ACID Transactions: Delta tables support ACID (Atomicity, Consistency, Isolation, Durability) transactions, which means you can reliably read and write data without corruption or partial updates.
# MAGIC 3. Metadata and Versioning: Delta tables keep a transaction log that tracks all changes, so you can query previous versions of the data (time travel) and see the history of updates.
# MAGIC 4. Integration: Delta tables are fully compatible with SQL and Apache Spark APIs, so you can use them just like regular tables in your queries and data processing workflows.
# MAGIC
# MAGIC **Note (Default Table Type)**: 
# MAGIC In Databricks, all tables are Delta tables by default unless you specify otherwise.
# MAGIC
# MAGIC So, a Delta table is much more powerful than a CSV file and offers more reliability and features than a basic SQL table, especially for big data and cloud environments.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACID Transactions:
# MAGIC In traditional data lakes, if a write job failed halfway, you ended up with "half-written" or corrupted data. Delta Lake fixes this with ACID:
# MAGIC
# MAGIC - Atomicity: "All or Nothing." A transaction either completes fully or doesn't happen at all. No partial data!
# MAGIC - Consistency: Data is always valid and uncorrupted before and after a change.
# MAGIC - Isolation: Multiple people can read and write at the same time without seeing "in-progress" messy data.
# MAGIC - Durability: Once a change is committed, it’s permanent, even if the system crashes.

# COMMAND ----------

df = spark.read.csv('/Volumes/workspace/default/my-volume/export.csv')
df.head()

# COMMAND ----------

display(df)

# COMMAND ----------

df.count(),len(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining The Schema:
# MAGIC Defining a schema when reading a CSV file ensures that each column has the correct data type and name, which improves data quality and reduces errors during processing. If you do not specify a schema, Spark will infer column types, which can lead to incorrect types (e.g., treating numbers as strings or misinterpreting dates). Explicit schemas also help with downstream tasks like writing to tables, joining, and aggregating data, as the structure is predictable and consistent
# MAGIC
# MAGIC You can read a CSV without a schema, but for production workflows or when data types matter, specifying a schema is recommended.

# COMMAND ----------

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

schema

# COMMAND ----------

type(schema)

# COMMAND ----------

df = spark.read.format("csv").option("header",True).schema(schema).load("/Volumes/workspace/default/my-volume/export.csv")

display(df)

# COMMAND ----------

# Create the table if it does not exist. Otherwise, replace the existing table.
df.writeTo("workspace.default.people_10m").createOrReplace()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert To A Table:
# MAGIC The following example takes data from the source table and merges it into the target Delta table. When there is a matching row in both tables, Delta Lake updates the data column using the given expression. When there is no matching row, Delta Lake adds a new row. This operation is known as an **upsert**.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from datetime import date

# Define schema for CSV import
csv_schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstname", StringType(), True),
  StructField("middlename", StringType(), True),
  StructField("lastname", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthdate", TimestampType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

# Read CSV into DataFrame
df = spark.read.format("csv").option("header", True).schema(csv_schema).load(
  "/Volumes/workspace/default/my-volume/export.csv"
)

display(df)

# Create or replace Delta table
df.writeTo("workspace.default.people_10m").createOrReplace()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Delta Tables and Test Schema Enforcement

# COMMAND ----------

# Define schema for updates
update_schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstName", StringType(), True),
  StructField("middleName", StringType(), True),
  StructField("lastName", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthDate", DateType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

# Prepare update data
update_data = [
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', date.fromisoformat('1992-09-17'), '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', date.fromisoformat('1984-05-22'), '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', date.fromisoformat('1968-07-22'), '988-61-6247', 90000),
  (20000001, 'John', '', 'Doe', 'M', date.fromisoformat('1978-01-14'), '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F', date.fromisoformat('1982-10-29'), '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', date.fromisoformat('1981-06-25'), '567-89-0123', 89900)
]

# Create DataFrame for updates and register as temp view
people_10m_updates = spark.createDataFrame(update_data, update_schema)
people_10m_updates.createOrReplaceTempView("people_10m_updates")

from delta.tables import DeltaTable

# Perform upsert (merge) into Delta table
deltaTable = DeltaTable.forName(spark, "workspace.default.people_10m")

(
  deltaTable.alias("people_10m")
    .merge(
      people_10m_updates.alias("people_10m_updates"),
      "people_10m.id = people_10m_updates.id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explanation:
# MAGIC
# MAGIC In Databricks, every table you create is a Delta table by default unless you specify otherwise. Delta tables use the Delta Lake format, which provides powerful features like ACID transactions, schema enforcement, and time travel for your data
# MAGIC
# MAGIC **Why use DeltaTable and its methods?**
# MAGIC
# MAGIC The DeltaTable class lets you run advanced operations on Delta tables, such as upserts (merge), deletes, and updates, directly from PySpark.
# MAGIC The method `forName(spark, "workspace.default.people_10m")` loads an existing Delta table so you can use these advanced features.
# MAGIC The `.merge()` method allows you to update existing rows and insert new ones in a single step, which is not possible with regular Spark DataFrames.
# MAGIC
# MAGIC **In simple terms:**
# MAGIC
# MAGIC Delta tables are special tables in Databricks that make your data reliable and easy to manage.
# MAGIC The DeltaTable class is a tool that helps you do powerful things with these tables, like updating and inserting data efficiently.
# MAGIC You use `forName()` to tell Databricks which table you want to work with.
# MAGIC
# MAGIC > Delta tables make your data management much easier and safer in Databricks. The DeltaTable class and its methods are just ways to use these features with Python code.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Read and Display Table:

# COMMAND ----------

display(spark.read.table("default.people_10m"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update A Table:

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, 'default.people_10m')

deltaTable.update(
  condition="gender='F'",
  set={'gender':"'Female'"}
)

display(spark.read.table('default.people_10m'))

# COMMAND ----------

display(spark.read.table('default.people_10m').groupBy("gender").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete A Table:
# MAGIC
# MAGIC You can delete a delta table using the below code
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import *
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC deltaTable = DeltaTable.forName(spark, "main.default.people_10m")
# MAGIC deltaTable.delete("birthDate < '1955-01-01'")
# MAGIC deltaTable.delete(col('birthDate') < '1960-01-01')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Display the history :

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, 'default.people_10m')

display(deltaTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC # Query an earlier version of the table (time travel)
# MAGIC
# MAGIC Delta Lake time travel allows you to query an older snapshot of a Delta table.
# MAGIC
# MAGIC To query an older version of a table, specify the table's version or timestamp. For example, to query version 0 or timestamp 2024-05-15T22:43:15.000+00:00Z from the preceding history, use the following:

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, 'default.people_10m')
deltahistory = deltaTable.history()

display(deltahistory.where("version == 0"))

# COMMAND ----------

