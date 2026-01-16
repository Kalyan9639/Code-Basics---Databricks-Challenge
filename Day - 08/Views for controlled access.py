# Databricks notebook source
# MAGIC %md
# MAGIC # Views in Databricks:
# MAGIC
# MAGIC A view in Databricks is:
# MAGIC
# MAGIC - A virtual table defined by a SQL query over one or more tables or other views.
# MAGIC - **Read-only**: You cannot directly change data in a view; any changes must be made to the underlying tables.
# MAGIC - Used for **controlled access**: You can restrict which rows or columns users see, or mask sensitive data, by defining the view’s query appropriately (for example, using dynamic views for row/column-level access control).
# MAGIC - **Does not store data**: It only stores the query definition, not a copy of the data. Querying a view runs the underlying query each time.

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from example.bronze.custom_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into example.bronze.custom_table values 
# MAGIC (3, 'John', 25),
# MAGIC (4, 'Jane', 30),
# MAGIC (5, 'Bob', 35);

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from example.bronze.custom_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC create view example.bronze.custom_view as select name,age from example.bronze.custom_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from example.bronze.custom_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this gives error as 'custom_view' is not a table, but a view
# MAGIC insert into example.bronze.custom_view values('asdf',29);

# COMMAND ----------

