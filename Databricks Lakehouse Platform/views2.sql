-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Changes in the views before and after restarting the cluster.
-- MAGIC
-- MAGIC Temporary views have already been dropped automatically since they were session based views. Once a session ends, they got dropped.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Before cluster restart
-- MAGIC

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

select * from car_view

-- COMMAND ----------

select * from global_temp.global_temp_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # After cluster restart, global_temp view is dropped since it is tied to cluster. 

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

select * from global_temp.global_temp_view

-- COMMAND ----------


