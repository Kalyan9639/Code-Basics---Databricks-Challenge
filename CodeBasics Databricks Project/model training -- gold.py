# Databricks notebook source
df = spark.read.load("/Volumes/workspace/codebasics/silver/final_df")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

import pyspark.sql.functions as F

# This forces Spark to see State_names_target as a continuous number, not a category
# df = df.withColumn("State_names_target", F.col("State_names_target").alias("State_names_target", metadata={}))
df = df.withColumn(
    "State_names_target", 
    F.col("State_names_target").cast("double").alias("State_names_target", metadata={})
)

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
import mlflow
import mlflow.spark
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

feature_cols = [
    "Age", "Referral", "is_weekend", "State_names_target", 
    "Gender_vec", "Marital_status_vec", "Segment_vec",       
    "Employees_status_vec", "Payment_method_vec"
]

# 2. Use VectorAssembler to merge everything into one "features" column
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data_with_features = assembler.transform(df)

# 3. Select only the necessary columns and split the data (80% Train, 20% Test)
# This is a key evaluation criterion (Proper train/test split)
final_data = data_with_features.select("features", "value_segment")
train_df, test_df = final_data.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

rf = RandomForestClassifier(
    labelCol="value_segment", 
    featuresCol="features", 
    numTrees=100,    
    maxDepth=10,
    maxBins=128
)

# COMMAND ----------

path = "/Volumes/workspace/codebasics/gold"

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
with mlflow.start_run(run_name="RandomForest_SpendTier_Model"):
    rf_model = rf.fit(train_df)
    predictions = rf_model.transform(test_df)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="value_segment", 
        predictionCol="prediction", 
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)

    mlflow.log_metric("accuracy",accuracy)
    mlflow.spark.log_model(rf_model,"random_forest_model",dfs_tmpdir=f"{path}/mlflow_stagging")
    print(f"Model Training Complete. Accuracy: {accuracy:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

import os

os.environ["SPARKML_TEMP_DFS_PATH"] = "/Volumes/workspace/codebasics/gold/mlflow_staging"

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# COMMAND ----------

model = RandomForestClassifier(
    labelCol="value_segment", 
    featuresCol="features",
    # We DON'T set numTrees or maxDepth here because the Grid will do it!
)

# 3. BUILD THE HYPERPARAMETER GRID
# These are the "Knobs" we are turning. 
# Spark will try EVERY combination (2 x 2 = 4 experiments total)
paramGrid = (ParamGridBuilder()
    .addGrid(model.numTrees, [10, 50])       # Try 50 trees, then 100 trees
    .addGrid(model.maxDepth, [5, 10])         # Try simple rules (5) vs complex rules (10)
    .build())

# 4. DEFINE THE EVALUATOR
# How do we decide which version is "best"? Accuracy.
evaluator = MulticlassClassificationEvaluator(
    labelCol="value_segment", 
    predictionCol="prediction", 
    metricName="accuracy"
)

# 5. SETUP CROSS VALIDATOR
# "3-Fold" means it trains 3 times for EVERY grid option to ensure stability.
cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4  # Run 4 models at the same time (Speed up!)
)

# 6. RUN THE EXPERIMENT WITH MLFLOW
with mlflow.start_run(run_name="RF_Hyperparam_Tuning"):
    print("Starting Hyperparameter Tuning... this may take a minute...")
    
    # Run the tuning!
    cvModel = cv.fit(train_df)
    
    # Get the best model from the experiments
    best_model = cvModel.bestModel
    
    # Evaluate the BEST model on the test set
    predictions = best_model.transform(test_df)
    accuracy = evaluator.evaluate(predictions)
    
    print(f"Tuning Complete!")
    print(f"Best Accuracy on Test Data: {accuracy:.4f}")
    
    # Log the BEST model and its specific parameters
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_param("best_numTrees", best_model.getNumTrees)
    mlflow.log_param("best_maxDepth", best_model.getOrDefault("maxDepth"))
    
    # Save the winner to the Volume
    mlflow.spark.log_model(
        best_model, 
        "best_rf_model", 
        dfs_tmpdir=f"{path}/mlflow_staging"
    )

# COMMAND ----------

train_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/codebasics/gold/train_df")
test_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/codebasics/gold/test_df")

# COMMAND ----------

