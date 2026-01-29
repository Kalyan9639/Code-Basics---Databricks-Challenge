# Databricks notebook source
df = spark.read.load("/Volumes/workspace/codebasics/silver/df_filled")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

for i in df.columns:
    if i in ['Transaction_ID','Transaction_date','Age','Amount_spent']: continue
    display(df.groupBy(i).count())

# COMMAND ----------

df.count()

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer, TargetEncoder

# One-hot encode low-cardinality features
low_card_cols = [
    "Gender", "Marital_status", "Segment", "Employees_status", "Payment_method"
]
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep")
    for col in low_card_cols
]
encoders = [
    OneHotEncoder(inputCol=col + "_idx", outputCol=col + "_vec")
    for col in low_card_cols
]

# Index high-cardinality feature for target encoding
state_indexer = StringIndexer(
    inputCol="State_names",
    outputCol="State_names_idx",
    handleInvalid="keep"
)

# Target encode high-cardinality feature
target_encoder = TargetEncoder(
    inputCols=["State_names_idx"],
    outputCols=["State_names_target"],
    labelCol="Amount_spent",
    targetType="continuous"
)

# Fit and transform (example for pipeline)
from pyspark.ml import Pipeline
pipeline = Pipeline(
    stages=indexers + encoders + [state_indexer, target_encoder]
)
model = pipeline.fit(df)
df_fe = model.transform(df)

display(df_fe)

# COMMAND ----------

df_fe.columns

# COMMAND ----------

df1 = df_fe.drop("Gender","Marital_status","Segment","Employees_status","Payment_method","State_names",
               "Gender_idx","Marital_status_idx","Segment_idx","Employees_status_idx","Payment_method_idx",
               "State_names_idx"            
)

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import dayofweek, when

df1 = df1.withColumn(
    "is_weekend",
    when(dayofweek("Transaction_date").isin([6, 7]), 1).otherwise(0)
)

display(df1)

# COMMAND ----------

df1 = df1.drop("Transaction_date")
display(df1)

# COMMAND ----------

df1.describe().show()

# COMMAND ----------

display(df1.filter(df1.Amount_spent > 2000).count())

# COMMAND ----------

from pyspark.sql.functions import when

df1 = df1.withColumn(
    "value_segment",
    when(F.col("Amount_spent") >= 2000, 2)
    .when((F.col("Amount_spent") >= 1000) & (F.col("Amount_spent") < 2000), 1)
    .otherwise(0)
)

display(df1)

# COMMAND ----------

final_df = df1.drop("Amount_spent")
display(final_df)

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/codebasics/silver/final_df")

# COMMAND ----------

# spark.sql("""
# create volume if not exists codebasics.gold          
# """)

# COMMAND ----------

