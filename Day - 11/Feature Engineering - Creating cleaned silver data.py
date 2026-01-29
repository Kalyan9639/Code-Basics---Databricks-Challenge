# Databricks notebook source
# MAGIC %md
# MAGIC # Note:
# MAGIC
# MAGIC The dataset that i am going to use in this session is from Kaggle. You can download it from the link given below.
# MAGIC
# MAGIC To Download the Dataset: [Click Here](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

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

# MAGIC %md
# MAGIC ### Splitting the `category_code` to 2 columns

# COMMAND ----------

from pyspark.sql.functions import split


display(df_renamed.withColumn('category',split('category_code',"\\.")[0]).withColumn("product",split("category_code", "\\.")[1]))

# COMMAND ----------

df_split = df_renamed.withColumn('category',split('category_code',"\\.")[0]).withColumn("product",split("category_code", "\\.")[1])

display(df_split)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, count, first, desc

def fill_with_mode(df, columns):
    for column in columns:
        mode_value = (
            df.groupBy(column)
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
            .filter(col(column).isNotNull())
            .select(column)
            .first()[0]
        )
        df = df.fillna({column: mode_value})
    return df

df_filled = fill_with_mode(df_split, ["brand", "category", "product"])
display(df_filled)

# COMMAND ----------

for i in ['brand','product','category']:
    print(
    df_filled
    .filter(col(i).isNull()).count())

# COMMAND ----------

df_filled.select('event_type').distinct().show()

# COMMAND ----------

cols_to_drop = ['product_id','category_id','category_code','user_session']

df_final = df_filled.drop(*cols_to_drop)
display(df_final)

# COMMAND ----------

# Create the 'silver' volume in the 'ecommerce' catalog if it doesn't exist
spark.sql("""
CREATE VOLUME IF NOT EXISTS ecommerce.silver
""")

df_final.write.format("delta").mode("overwrite").save("/Volumes/workspace/ecommerce/silver/ecom_dataset")

# COMMAND ----------

display(spark.read.load("/Volumes/workspace/ecommerce/silver/ecom_dataset"))

# COMMAND ----------

