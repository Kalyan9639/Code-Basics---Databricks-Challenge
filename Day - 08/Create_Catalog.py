# Databricks notebook source
# MAGIC %sql
# MAGIC -- create catalog and use/access it
# MAGIC create catalog example;
# MAGIC use catalog example;
# MAGIC
# MAGIC -- create different schemas inside the accessed catalog
# MAGIC create schema bronze;
# MAGIC create schema silver;
# MAGIC create schema gold;
# MAGIC
# MAGIC -- use/access any one schema and create database and volume in it
# MAGIC use schema bronze;
# MAGIC create database bronze_db;
# MAGIC create volume bronze_volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use schema silver;
# MAGIC create volume silver_volume;
# MAGIC create table silver_table;
# MAGIC create database silver_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now, we will create the tables and register the tables created

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronze.custom_table(
# MAGIC   id int primary key,
# MAGIC   name string,
# MAGIC   age int
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into bronze.custom_table(id,name,age)values
# MAGIC   (1, 'Kalyan',19),
# MAGIC   (2, 'Hima', 17);

# COMMAND ----------

display(spark.read.table("example.bronze.custom_table"))