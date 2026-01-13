# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Spark Fundamentals:
# MAGIC Understanding Apache Spark is the most critical step because it is the "engine" that powers everything inside Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spark Architecture:
# MAGIC Think of Spark like a construction site:
# MAGIC - The Driver (The Architect): This is the master node. It sits in the "office," analyzes the code you wrote, and plans how to finish the job. It doesn't do the heavy lifting itself; it delegates tasks.
# MAGIC - The Executors (The Workers): These are the worker nodes. They live on different machines (clusters). They receive tasks from the Driver, process the data, and send the results back.
# MAGIC - The DAG (Directed Acyclic Graph): This is the Blueprint. When you write Spark code, Spark doesn't run it immediately. Instead, it builds a logical graph (DAG) of all the steps needed to get the result. It calculates the most efficient way to finish the work before starting.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 2. DataFrames vs. RDDs(Resilient Distributed Data):
# MAGIC In the early days of Spark, we used RDDs. Now, we almost exclusively use DataFrames.
# MAGIC
# MAGIC #### RDD (Resilient Distributed Dataset):
# MAGIC - The Foundation: RDDs were the original building block of Spark. They represent an immutable, distributed collection of objects.
# MAGIC - Low-Level Control: Because they are "low-level," they give you maximum control over exactly how data is processed, but you have to write a lot of manual code.
# MAGIC - No Schema: Spark has no idea what is inside an RDD (it’s just "objects" to Spark). Therefore, it cannot optimize the data processing for you.
# MAGIC - Strictly Functional: You use functions like .map(), .filter(), and .reduce() to manipulate them.
# MAGIC
# MAGIC #### DataFrame:
# MAGIC - The Modern Standard: DataFrames are built on top of RDDs but are organized into named columns, much like a table in a relational database.
# MAGIC - Catalyst Optimizer: This is the "magic" of DataFrames. When you write code, the Spark engine (Catalyst) looks at your request and rearranges the steps to make it run as fast as possible.
# MAGIC - Ease of Use: You can use SQL queries or "domain-specific" methods like .select() or .groupBy(), which are much more intuitive than RDD functions.
# MAGIC - Memory Efficiency: Because Spark knows the "schema" (the data types of the columns), it stores the data in a highly compressed, binary format.
# MAGIC
# MAGIC | Feature | RDD (Resilient Distributed Dataset) | DataFrame |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **Abstractions** | Low-level API | High-level API |
# MAGIC | **Data Representation** | Distributed collection of Java/Python objects | Distributed collection of Rows with a Schema |
# MAGIC | **Optimization** | Manual optimization (User must be an expert) | Automatic optimization via Catalyst & Tungsten |
# MAGIC | **Performance** | Slower (requires more overhead) | Faster (highly optimized and compressed) |
# MAGIC | **Ease of Use** | Difficult; requires complex functional programming | Easy; uses SQL-like syntax or pure SQL |
# MAGIC | **Type Safety** | Type-safe (in Scala/Java) | Untyped (checked at runtime) |
# MAGIC | **When to use?** | Only for low-level legacy code or custom objects | **Always** for modern Data Engineering and Analysis |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 3. Lazy Evaluation: "I'll do it when I have to"
# MAGIC This is a unique concept in Spark. Spark categorizes your code into two types:
# MAGIC - Transformations: (e.g., `filter`, `select`, `groupBy`). Spark does nothing when you run these. It just adds them to the DAG (the blueprint).
# MAGIC - Actions: (e.g., `show()`, `count()`, `write()`). Spark only starts working when you call an Action.
# MAGIC
# MAGIC **Why?** Imagine you tell Spark to: 
# MAGIC 1. Load 1TB of data, 
# MAGIC 2. Filter for only "New York," 
# MAGIC 3. Show the first 5 rows.
# MAGIC
# MAGIC If Spark weren't "lazy," it would load all 1TB first (slow!). Because it is lazy, it looks at the whole plan, realizes you only need 5 rows from NY, and only loads exactly what is necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 4. Notebook Magic Commands
# MAGIC In Databricks, a notebook is usually set to one default language (like Python). Magic Commands allow you to switch languages or interact with the file system within the same notebook.
# MAGIC
# MAGIC - `%sql`: Allows you to write pure SQL queries in a Python notebook.
# MAGIC - `%python`: Allows you to write Python code (if your notebook is set to SQL).
# MAGIC - `%fs`: Short for "File System." It lets you look at the Databricks File System (DBFS) to see your files.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/ecommerce

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note:
# MAGIC I got the above error because I tried to display all the folders/files present in the directory 'ecommerce'. But, Databricks spark only accepts the format `/Volumes/<catalog>/<schema>/<volume>`

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/ecommerce/ecommerce_data

# COMMAND ----------

# Default language is Python
df = spark.read.csv("/databricks-datasets/workspace/ecommerce/ecommerce_data/2019-Oct.csv", header=True)
df.createOrReplaceTempView("my_data") # Make it available to SQL

# Use Magic Command to switch to SQL
%sql
SELECT * FROM my_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note:
# MAGIC I tried to run the above cell but i got the error because, in databricks, **you can try only one magic command in a cell. If you want to use different magic command, you have to use a new cell.**

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv", header=True)
df.createOrReplaceTempView("my_data") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Tasks to be performed:

# COMMAND ----------

# uploaded a sample ecommerce dataset to the workspace catalog, default schema
df = spark.read.csv("workspace.default")
df.head()

# COMMAND ----------

