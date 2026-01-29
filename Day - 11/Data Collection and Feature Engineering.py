# Databricks notebook source
# MAGIC %md
# MAGIC # Note:
# MAGIC
# MAGIC The dataset that i am going to use in this session is from Kaggle. You can download it from the link given below.
# MAGIC
# MAGIC #### To Download the Dataset: [Click Here](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")
display(df)

# COMMAND ----------

print(f'Size of the dataset is: ({df.count()},{len(df.columns)})')

# COMMAND ----------

from pyspark.sql.functions import col
# Checking for null values
for i in df.columns:
    print("="*10,i,"="*10)
    print(df.filter(col(i).isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Changing the column names**

# COMMAND ----------

# Collect the first row as the new header
new_header = df.first()

# Remove the first row from the DataFrame
df_no_header = df.filter(
    "NOT (" + " AND ".join([
        f"{col} = '{val}'" if val is not None else f"{col} IS NULL"
        for col, val in zip(df.columns, new_header)
    ]) + ")"
)

# Rename columns using the first row values
df_renamed = df_no_header.toDF(*[str(x) for x in new_header])

display(df_renamed)

# COMMAND ----------

df_renamed = df_renamed.dropna(subset=['user_session'])
df_renamed.filter(col("user_session").isNull()).limit(1).count() > 0

# COMMAND ----------

from pyspark.sql.functions import split


display(df_renamed.withColumn('category',split('category_code',"\\.")[0]).withColumn("product",split("category_code", "\\.")[1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Splitting the `category_code` to 2 columns

# COMMAND ----------

df_split = df_renamed.withColumn('category',split('category_code',"\\.")[0]).withColumn("product",split("category_code", "\\.")[1])

display(df_split)

# COMMAND ----------

display((df_split.select('category').distinct()))
display((df_split.select('product').distinct()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating dictionary using `brand` to full the null values in `product`

# COMMAND ----------

from pyspark.sql.functions import collect_list

display(df_split.groupBy("brand").agg(collect_list('product').alias('products')))

# COMMAND ----------

display(df_split.groupBy('brand','product').count())

# this returns a DataFrame where each row represents a unique combination of brand and product, along with the count of how many times that combination appears in your data.

# COMMAND ----------

from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Count occurrences of each product per brand
product_counts = df_split.groupBy("brand", "product").count()

# Window to rank products by count per brand
window = Window.partitionBy("brand").orderBy(col("count").desc())

# Get the mode (most frequent product) per brand
mode_df = product_counts.withColumn(
    "rank", row_number().over(window)
).filter(col("rank") == 1).select("brand", "product")

# Collect as dictionary
brand_mode_dict = {row["brand"]: row["product"] for row in mode_df.collect()}

# COMMAND ----------

brand_mode_dict['global']  # you can check the output from the search option in above dataframe output by typing "global"

# COMMAND ----------

from pyspark.sql.functions import create_map, coalesce, lit, col

mapping_expr = create_map(
    *[item for pair in brand_mode_dict.items() for item in (lit(pair[0]), lit(pair[1]))]
)

df_prod_filled = df_split.withColumn(
    "product",
    coalesce(
        col("product"),
        mapping_expr[col("brand")]
    )
)

display(df_prod_filled)

# COMMAND ----------

display(
    df_prod_filled
    .filter(col("product").isNull()).count()
)

# COMMAND ----------

df_prod_filled.filter(
    col("brand").isNull() & col("product").isNotNull()
).count()

# COMMAND ----------

