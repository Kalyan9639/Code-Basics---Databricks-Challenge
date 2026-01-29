# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC ls "/Volumes/workspace/codebasics/bronze/customer_spending"

# COMMAND ----------

df = spark.read.load("/Volumes/workspace/codebasics/bronze/customer_spending")
display(df)

# COMMAND ----------

df.count(),len(df.columns)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ➡️ Calculating the null values

# COMMAND ----------

null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
display(null_counts)

# COMMAND ----------

print(df.count())
df.count()-96065

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ➡️ Calculating the `value_counts()` for relevant columns

# COMMAND ----------

for i in df.columns:
  if i in ['Transaction_ID','Transaction_date','Age','Amount_spent']: continue
  # print(i)
  display(df.groupBy(i).count())
  # print("== "*20)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ➡️ Checking if missingness in `Amount_spent` is random

# COMMAND ----------

df_missing_flag = df.withColumn(
    "is_amount_missing",
    F.col("Amount_spent").isNull()
)

# Compare counts and average Age for missing vs non-missing Amount_spent
display(
    df_missing_flag.groupBy("is_amount_missing")
    .agg(
        F.count("*").alias("row_count"),
        F.mean("Age").alias("avg_age"),
        F.countDistinct("Gender").alias("distinct_genders"),
        F.countDistinct("Marital_status").alias("marital_status"),
        F.countDistinct("Employees_status").alias("employee_status")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observation:
# MAGIC
# MAGIC Missingness in the target column is related to `age` and is not completely random. The distributions of `gender`, `marital status`, and `employee status` are similar between groups, so missingness does not appear related to those features.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ➡️ Checking whether the missingness all null columns is random or not

# COMMAND ----------

cols_with_nulls = ["Gender", "Age", "Employees_status", "Referral", "Amount_spent"]

for col in cols_with_nulls:
    missing_flag = f"is_{col.lower()}_missing"
    df_flag = df.withColumn(missing_flag, F.col(col).isNull())
    summary = (
        df_flag.groupBy(missing_flag)
        .agg(
            F.count("*").alias("row_count"),
            F.mean("Age").alias("avg_age"),
            F.countDistinct("Gender").alias("distinct_genders"),
            F.countDistinct("Employees_status").alias("distinct_employee_status"),
            F.mean("Amount_spent").alias("avg_amount_spent")
        )
    )
    display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observation:
# MAGIC
# MAGIC For most columns, missingness is associated with lower average age and lower average amount spent. This suggests that missingness is not completely random and is related to these features.
# MAGIC
# MAGIC ### Proposed Solution:
# MAGIC - Fill Age with the median (since missingness is related to age)
# MAGIC - Use age-based imputation for other columns with nulls (i.e., fill missing values in other columns based on age groups)
# MAGIC
# MAGIC > **This reduces bias compared to global imputation methods**
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## ➡️ Filling Null Values:

# COMMAND ----------

# df = spark.read.load("/Volumes/workspace/codebasics/bronze/customer_spending")
# display(df)

# COMMAND ----------

# Fill age with median
age_median = df.approxQuantile('Age', [0.5], 0.01)[0]

df = df.fillna({'Age': age_median})
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Impute other columns based on Age group
# Example: Fill Gender, Employees_status, Referral with mode within age bins

# Create age bins
df = df.withColumn(
    "age_bin",
    F.when(F.col("Age") < 30, "young")
    .when((F.col("Age") >= 30) & (F.col("Age") < 50), "middle")
    .otherwise("old")
)

# Function to get mode per age_bin
def get_mode(df, col, bin_col="age_bin"):
    mode_df = (
        df.groupBy(bin_col, col)
        .count()
        .orderBy(bin_col, F.desc("count"))
        .dropDuplicates([bin_col])
    )
    return mode_df

# Get modes for each column
gender_mode_df = get_mode(df, "Gender")
employees_mode_df = get_mode(df, "Employees_status")
referral_mode_df = get_mode(df, "Referral")

# Join modes back to main df
df = df.join(gender_mode_df.withColumnRenamed("Gender", "mode_gender"), ["age_bin"], "left")
df = df.join(employees_mode_df.withColumnRenamed("Employees_status", "mode_employees"), ["age_bin"], "left")
df = df.join(referral_mode_df.withColumnRenamed("Referral", "mode_referral"), ["age_bin"], "left")

# Fill nulls in relevant columns
df = df.withColumn(
    "Gender",
    F.when(F.col("Gender").isNull(), F.col("mode_gender")).otherwise(F.col("Gender"))
).withColumn(
    "Employees_status",
    F.when(F.col("Employees_status").isNull(), F.col("mode_employees")).otherwise(F.col("Employees_status"))
).withColumn(
    "Referral",
    F.when(F.col("Referral").isNull(), F.col("mode_referral")).otherwise(F.col("Referral"))
)

# 3. Fill Amount_spent with median
amount_median = df.approxQuantile("Amount_spent", [0.5], 0.01)[0]
df = df.fillna({"Amount_spent": amount_median})

# Drop helper columns
df = df.drop("age_bin", "mode_gender", "mode_employees", "mode_referral")

display(df)

# COMMAND ----------

df = df.drop("count")
display(df)

# COMMAND ----------

display(df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### ➡️ Checking for duplicates

# COMMAND ----------

duplicate_count = df.groupBy(df.columns).count().filter(F.col("count") > 1)
display(duplicate_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observation:
# MAGIC
# MAGIC There are no duplicate rows present in the dataset
# MAGIC
# MAGIC ----

# COMMAND ----------

spark.sql("""
          create volume if not exists codebasics.silver
""")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/Volumes/workspace/codebasics/silver/df_filled")

# COMMAND ----------

