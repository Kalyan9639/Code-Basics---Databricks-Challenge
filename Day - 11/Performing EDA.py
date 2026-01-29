# Databricks notebook source
df = spark.read.load("/Volumes/workspace/ecommerce/silver/ecom_dataset")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count(),len(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Things to find out using EDA:
# MAGIC - How many people purchased the product vs added to cart vs just viewed
# MAGIC - Which product is mostly purchased
# MAGIC - Which brand product is mostly purchased
# MAGIC - which category is mostly viewed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC This code uses Spark's TargetEncoder to encode the categorical columns category, product, and brand into new columns category_target, product_target, and brand_target.
# MAGIC
# MAGIC It does this by replacing each category value with the mean of the target variable (is_purchase_cart, which is binary) for that category. For example, if "brand" = "Apple" and 60% of "Apple" rows have is_purchase_cart = 1, then "Apple" will be encoded as 0.6 in brand_target. This encoding captures the relationship between each category and the likelihood of purchase/cart action
# MAGIC

# COMMAND ----------

# from pyspark.ml.feature import TargetEncoder

# encoder = TargetEncoder(
#     inputCols=["category", "product", "brand"],
#     outputCols=["category_target", "product_target", "brand_target"],
#     labelCol="is_purchase_cart",
#     targetType="binary"
# )
# model = encoder.fit(df_analysis)
# df_encoded = model.transform(df_analysis)
# display(df_encoded)