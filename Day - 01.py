# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Why Databricks vs. Pandas/Hadoop?
# MAGIC
# MAGIC Think of these as different modes of transportation for your data:
# MAGIC 1. <u>*Pandas*</u> (The Bicycle 🚲): Perfect for small data that fits in your computer's RAM. If you try to load a 100GB file into Pandas on a standard laptop, it will crash.
# MAGIC 2. <u>*Hadoop*</u> (The Cargo Ship 🚢): Designed for massive data, but it's slow. It writes data to a hard drive after every single step (MapReduce), which creates a lot of "friction."
# MAGIC 3. <u>*Databricks/Spark*</u> (The High-Speed Train 🚅): It processes data in-memory. It is much faster than Hadoop and, unlike Pandas, it is distributed. This means it can split a massive task across 100 different computers (a cluster) to get it done in seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 2. Lakehouse Architecture Basics
# MAGIC In the past, companies had to choose between a Data Lake (cheap, messy storage for files like your video/audio logs) and a Data Warehouse (organized, fast, but expensive storage for tables).
# MAGIC
# MAGIC The Lakehouse combines them. It uses Delta Lake, an open-source storage layer that brings "order to the chaos." It allows you to store raw files cheaply while still being able to query them with lightning-fast SQL as if they were in a structured database. 🏗️
# MAGIC
# MAGIC
# MAGIC Databricks recommends organizing your Lakehouse into three layers. This ensures your data gets cleaner as it moves through the system. The most common way to build a Lakehouse is using the Medallion Architecture. This is a series of data "layers" that refine information as it flows through the system:
# MAGIC 1. **Bronze (Raw)** 🥉: This is the entry point. We land data here exactly as it is from the source (APIs, logs, or databases). We don't change anything yet; it’s a permanent record of what arrived.
# MAGIC 2. **Silver (Validated/Cleaned)** 🥈: Here, we clean the data. We remove duplicates, fix null values, and ensure dates and numbers are in the right format. It’s "query-ready" for data scientists.
# MAGIC 3. **Gold (Curated/Business-Level)** 🥇: This is the final product. Data is aggregated and shaped for specific business needs, like a table showing "Monthly Revenue by Region" or "Daily Video Summaries."

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 3. Databricks Workspace Structure
# MAGIC The workspace is where your team actually works. It consists of:
# MAGIC
# MAGIC 1. Compute (Clusters) ⚙️: The actual "brains" or virtual machines that do the processing.
# MAGIC 2. Notebooks 📝: Interactive documents (like Jupyter) where you write Python, SQL, or Scala.
# MAGIC 3. Catalog 📁: A central library (Unity Catalog) that tracks all your tables, files, and who has permission to see them.
# MAGIC 4. Workflows ⏰: A scheduler that runs your code automatically every day or hour.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 4. Industry Use Cases
# MAGIC 1. Netflix 🍿: Uses Databricks to process billions of "play" events to refine their recommendation algorithms in real-time.
# MAGIC 2. Shell 🐚: Uses it for Predictive Maintenance—analyzing sensor data from thousands of sensors on oil rigs to predict a part will break before it actually does.
# MAGIC 3. Comcast 📺: Uses it to analyze petabytes of telemetry data from set-top boxes to improve voice-remote accuracy.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⭐ Some Common Terminologies: 
# MAGIC
# MAGIC ### Data Warehouse vs. Data Lake vs. Data Lakehouse
# MAGIC 1. *Data Warehouse* 📦: Think of this as a bottled water factory. Everything is perfectly cleaned, bottled, and labeled before it even enters the building. It’s great for reports, but it’s expensive and doesn't handle "raw" things like videos or images well.
# MAGIC 2. *Data Lake* 🌊: This is like a giant reservoir. You can dump any water in there—rainwater, river water, even some mud. It’s very cheap to store massive amounts, but it can get messy (a "data swamp") because there’s no organization.
# MAGIC 3. *Data Lakehouse* 🏠: This is the best of both. It’s a reservoir with a built-in filtration plant. You get the cheap storage of the lake but the high-quality, organized "bottled" tables of the warehouse.
# MAGIC
# MAGIC ### Delta Table
# MAGIC In a normal "lake," data is just a bunch of files (like CSVs or Parquet). If two people try to edit the same file at once, it breaks. A Delta Table is a "smart" table that lives on your lake. It adds ACID Transactions—which is just a fancy way of saying it ensures your data never gets corrupted, even if a process crashes halfway through. It also lets you do "Time Travel" to see what the data looked like yesterday. 🕒

# COMMAND ----------

data = [('samsung','15000'),('realme','35000'),('apple','100000'),('oneplus','25000')]
df = spark.createDataFrame(data,['product','price'])

# COMMAND ----------

display(df.select(df.columns))

# COMMAND ----------

# display the number of rows
df.count()

# COMMAND ----------

df.show(3)

# COMMAND ----------

df.filter(df['price']>50000).show()

# COMMAND ----------

df.filter(df['price']<50000).display()

# COMMAND ----------

