# Databricks notebook source
df1 = spark.read.load("/Volumes/workspace/codebasics/silver/df_filled")
df2 = spark.read.load("/Volumes/workspace/codebasics/silver/final_df")

display(df1)
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Distribution of Customer Spending Tiers (0=Normal, 1=Premium, 2=Elite)

# COMMAND ----------

display(df2.groupBy("value_segment").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 💰 Average Spendings: Weekdays vs Weekends

# COMMAND ----------

df_sa = df2.select(
    "Transaction_ID",
    "is_weekend").join(df1.select("Transaction_ID","Amount_spent"),on="Transaction_ID")
display(df_sa)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Age Density Across Spending Tiers

# COMMAND ----------

display(df2.select('Age',"value_segment"))

# COMMAND ----------

