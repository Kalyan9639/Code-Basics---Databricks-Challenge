# Databricks notebook source
# MAGIC %md
# MAGIC ## Pre-installed Modules In Jupyter Notebooks For Python:
# MAGIC Databricks notebooks for Python come with several pre-installed libraries, especially in the Databricks Apps environment. Some of the default Python libraries include:
# MAGIC
# MAGIC - `databricks-sql-connector`
# MAGIC - `databricks-sdk`
# MAGIC - `mlflow-skinny`
# MAGIC - `gradio`
# MAGIC - `streamlit`
# MAGIC - `shiny`
# MAGIC - `dash`
# MAGIC - `flask`
# MAGIC - `fastapi`
# MAGIC - `uvicorn[standard]`
# MAGIC - `gunicorn`
# MAGIC - `huggingface-hub`
# MAGIC - `dash-ag-grid`
# MAGIC - `dash-mantine-components`
# MAGIC - `dash-bootstrap-components`
# MAGIC - `plotly`
# MAGIC - `plotly-resampler`
# MAGIC
# MAGIC You do not need to install these unless you require a different version
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Day - 03
# MAGIC ## 1. PySpark vs. Pandas
# MAGIC Think of Pandas as a high-performance sports car (perfect for small-scale data that fits in your RAM). Think of PySpark as a freight train (built to carry massive loads across multiple tracks/nodes).
# MAGIC
# MAGIC | Feature        | Pandas                        | PySpark                          |
# MAGIC |---------------|------------------------------|----------------------------------|
# MAGIC | Execution     | Eager (Runs immediately)      | Lazy (Builds a plan first)       |
# MAGIC | Scaling       | Vertical (needs a bigger computer) | Horizontal (adds more computers) |
# MAGIC | Syntax        | df.head()                     | df.show()                        |
# MAGIC | Data Recovery | "If it crashes, data is lost."| Resilient; can rebuild lost parts.|

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Joins (The Relationship Builder)
# MAGIC Joins allow you to combine two different tables based on a common "Key" (like product_id).
# MAGIC
# MAGIC 1. _Inner Join_: Only keeps rows where the ID exists in both tables.
# MAGIC 2. _Left Join_: Keeps all rows from the left table, even if there’s no match on the right (missing values become null).
# MAGIC 3. _Right Join_: Keeps all rows from the right table.
# MAGIC 4. _Outer Join_: Keeps everything from both, filling null where there's no match.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Window Functions (The Advanced Analytics Tool)
# MAGIC This is where beginners become pros. A Window Function performs a calculation across a set of rows that are related to the current row. Unlike groupBy, which collapses rows into a single summary, Window functions keep all rows intact but add an extra column of information (like a rank or a running total) alongside them.
# MAGIC
# MAGIC **How it works: The "WindowSpec"**:
# MAGIC To use a window, you must define its Specification (WindowSpec) using three parts:
# MAGIC
# MAGIC 1. `partitionBy`: Groups your data (e.g., group by "Category").
# MAGIC 2. `orderBy`: Sets the sequence within those groups (e.g., sort by "Date").
# MAGIC 3. `rowsBetween` (Optional): Defines a moving frame (e.g., "the last 3 rows plus the current row").
# MAGIC
# MAGIC **Core Functions to Master:**
# MAGIC 1. Ranking: `row_number()` (1, 2, 3), `rank()` (1, 2, 2, 4 — leaves gaps), and `dense_rank()` (1, 2, 2, 3 — no gaps).
# MAGIC 2. Analytic: `lead()` (peeks at the next row) and `lag()` (peeks at the previous row).
# MAGIC 3. Aggregate: `sum()`, `avg()`, `max()` used over a window to create running totals or moving averages.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. User-Defined Functions (UDFs)
# MAGIC Spark provides hundreds of built-in functions (like `upper()`, `date_add()`, etc.). A UDF is a custom Python function you write yourself when Spark's built-in tools aren't enough for your specific business logic.
# MAGIC
# MAGIC **The Two Types of UDFs**
# MAGIC 1. Standard UDF: Processes data one row at a time. It is easy to write but can be slow because Spark has to move data back and forth between its JVM engine and the Python environment.
# MAGIC
# MAGIC 2. Pandas UDF (Vectorized): Much faster! It uses Apache Arrow to send chunks (batches) of data to Python, allowing you to use high-speed Pandas/NumPy logic on distributed data.
# MAGIC
# MAGIC #### Example Syntax:
# MAGIC > Using Standard UDF
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC
# MAGIC # 1. Define a regular Python function
# MAGIC def category_label(price):
# MAGIC     if price > 1000: return "Premium"
# MAGIC     return "Standard"
# MAGIC
# MAGIC # 2. Register it as a Spark UDF
# MAGIC price_udf = udf(category_label, StringType())
# MAGIC
# MAGIC # 3. Use it in a DataFrame
# MAGIC df.withColumn("price_tier", price_udf(df["price"]))
# MAGIC ```
# MAGIC
# MAGIC > Using Pandas UDF
# MAGIC
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC from pyspark.sql.types import DoubleType
# MAGIC
# MAGIC # 1. Define the function with a Type Hint (Series in -> Series out)
# MAGIC @pandas_udf(DoubleType())
# MAGIC def apply_tax_vectorized(prices: pd.Series) -> pd.Series:
# MAGIC     # This logic happens in Pandas/C, not slow Python loops!
# MAGIC     return prices * 1.15 
# MAGIC
# MAGIC # 2. Apply it to your DataFrame
# MAGIC df_with_tax = df.withColumn("total_price", apply_tax_vectorized(df["price"]))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Practical Implementation:

# COMMAND ----------

df = spark.table("workspace.default.industry_pricing_optimization_final")
display(df)

# COMMAND ----------

# get the shape of the dataframe
df.count(),len(df.columns)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/ecommerce/ecommerce_data/

# COMMAND ----------

df1 = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv",header=True,inferSchema=True)
df2 = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df2)

# COMMAND ----------

df1['event_type'].unique()

# COMMAND ----------

display(df1.select("event_type").distinct())

# COMMAND ----------

df1_new = df1.limit(30000)
df2_new = df2.limit(30000)

# COMMAND ----------

len(df1_new.columns)

# COMMAND ----------

df1_new.count(),df2_new.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing Inner Join: 

# COMMAND ----------

df_joined = df1_new.join(df2_new,on="product_id",how="inner")

display(df_joined)

# COMMAND ----------

df_joined.count(),len(df_joined.columns)

# COMMAND ----------

display(df1_new.select("category_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Splitting the category_code :

# COMMAND ----------

from pyspark.sql.functions import split

display(df1_new.withColumn("category_code_first", split("category_code", "\.")[0]))

# COMMAND ----------

# DBTITLE 1,Calculating Mode
display(df1_new.groupBy("category_code").count().orderBy("count", ascending=False).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The reason why I was getting 6 lakh rows even though the rows were 30k each:
# MAGIC
# MAGIC If there are duplicate values in the join column in either DataFrame, the join will produce a Cartesian product for those matches, which can significantly increase the number of rows in the result
# MAGIC

# COMMAND ----------

display(df1_new.groupBy("product_id").count().orderBy("count", ascending=False).limit(10))
display(df2_new.groupBy("product_id").count().orderBy("count", ascending=False).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Goal: Find the Top 2 most expensive items purchased by each user.

# COMMAND ----------

df1_new.columns

# COMMAND ----------

display(df1_new.repartition("user_id").orderBy("price",ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Obsservation:**
# MAGIC
# MAGIC The top 2 most expensive items purchased were "Electronic Clocks"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Calculating the user with the highest spending and assingning them rank-1
# MAGIC
# MAGIC
# MAGIC ### calculating rank and row_number:
# MAGIC To understand about the rank and row_number, here is a small example:
# MAGIC
# MAGIC Suppose you have this data:
# MAGIC
# MAGIC |user_id	| price |
# MAGIC |---|---|
# MAGIC | 1	| 100 |
# MAGIC | 1 |	90 |
# MAGIC | 1 |	90 |
# MAGIC | 2 |	80 |
# MAGIC | 2 |	70 |
# MAGIC
# MAGIC If you use a window partitioned by `user_id` and ordered by `price` descending:
# MAGIC
# MAGIC - row_number for `user_id`=1: 100→1, 90→2, 90→3
# MAGIC - rank for `user_id`=1: 100→1, 90→2, 90→2
# MAGIC
# MAGIC So, for `user_id`=1, the two rows with price 90 get the same rank (2), but different row numbers (2 and 3).
# MAGIC
# MAGIC > You can use these functions to identify the top-N items per group, handle duplicates, or filter for specific ranks.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,rank

window_spec = Window.partitionBy("user_id").orderBy(df1_new["price"].desc())
df1_new_with_rownum = df1_new.withColumn("row_number", row_number().over(window_spec))
df1_new_with_rank = df1_new.withColumn("rank", rank().over(window_spec))

display(df1_new_with_rownum)
display(df1_new_with_rank)

# COMMAND ----------

display(df1_new_with_rank.groupBy("rank").count())

# COMMAND ----------

display(df1_new_with_rank.filter((df1_new_with_rank['rank']==1)|(df1_new_with_rank['rank']==2)).groupBy("rank").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Standard & Pandas UDF's:

# COMMAND ----------

display(df1.orderBy("price",ascending=False).select('price'))

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def cat_label(price):
  if price>1250: return 'Premium'
  return 'Standard'

# register the user-defined function with udf so that it can be used
price_udf = udf(cat_label, StringType())
df1_with_priec_tier = df1.withColumn("price_tier",price_udf(df1.price))

display(df1_with_priec_tier.select(["price","price_tier"]))

# COMMAND ----------

# display(df1_with_priec_tier.orderBy("price_tier").select(["price","price_tier"]))

# COMMAND ----------

