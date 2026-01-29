# Databricks notebook source
import mlflow

# Find the run ID and use the artifact URI
model_uri = "runs:/e6627384abb243bb82450fccbf4bc0cd/random_forest_model"
rf_model = mlflow.spark.load_model(
    model_uri, dfs_tmpdir="/Volumes/workspace/codebasics/gold/mlflow_staging"
)

# COMMAND ----------

train_df = spark.read.load("/Volumes/workspace/codebasics/gold/train_df")

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.regression import RandomForestRegressionModel

rf_stage = None
for stage in rf_model.stages:
    if isinstance(stage, (RandomForestClassificationModel, RandomForestRegressionModel)):
        rf_stage = stage
        break

if rf_stage is None:
    raise ValueError("No RandomForest model found in pipeline stages.")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Checking Feature Importance:

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Extract Feature Importance scores
importances = rf_stage.featureImportances

# 2. Extract Feature Names from VectorAssembler Metadata
# This shows high technical proficiency with Spark
attrs = train_df.schema["features"].metadata["ml_attr"]["attrs"]
feature_names = []

# Collect names from numeric and binary attribute groups
for group in attrs.values():
    for attr in group:
        feature_names.append((attr["idx"], attr["name"]))

# Sort names to match the importance vector index
feature_names.sort(key=lambda x: x[0])
sorted_names = [x[1] for x in feature_names]

# 3. Create a DataFrame for analysis
importance_df = pd.DataFrame({
    "Feature": sorted_names,
    "Importance": importances.toArray()
}).sort_values(by="Importance", ascending=False)

# 4. Display the Top Business Drivers
print("--- Top 10 Features Driving Customer Spending ---")
print(importance_df.head(10))

# 5. Visualize (Save this for your presentation!)
plt.figure(figsize=(10, 6))
sns.barplot(x="Importance", y="Feature", data=importance_df, palette="magma")
plt.title("Business Insights: What Factors Predict High-Value Customers?")
plt.xlabel("Impact Score (Importance)")
plt.ylabel("Feature Name")
plt.tight_layout()
plt.savefig("/Volumes/workspace/codebasics/gold/feature_importance.png")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🪙 Preparing the Business-Ready (Gold) Data
# MAGIC We select the key columns and extract the `Max Probability` to show how confident the AI is.

# COMMAND ----------

test_df = spark.read.load("/Volumes/workspace/codebasics/gold/test_df")

# COMMAND ----------

predictions = rf_stage.transform(test_df)
display(predictions)

# COMMAND ----------

predictions.columns

# COMMAND ----------

from pyspark.sql import functions as F

gold_df = predictions.select(
    "value_segment", 
    "prediction", 
    "probability"
)

# 2. Add Business Logic: Extract Confidence Score
# The probability column is a vector. We extract the value of the chosen prediction.
def get_confidence(prob_vec, pred):
    return float(prob_vec[int(pred)])

from pyspark.sql.types import DoubleType
udf_confidence = F.udf(get_confidence, DoubleType())

gold_df = gold_df.withColumn("AI_Confidence", udf_confidence("probability", "prediction"))

# 3. Create a 'Success' flag for the dashboard
gold_df = gold_df.withColumn(
    "Is_Correct", 
    F.when(F.col("value_segment") == F.col("prediction"), "Correct").otherwise("Incorrect")
)

# 4. Save to Unity Catalog
gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.codebasics.gold_model_insights")

print("Gold Table Created with Confidence Scores!")

# COMMAND ----------

display(spark.read.table("codebasics.gold_model_insights"))

# COMMAND ----------

