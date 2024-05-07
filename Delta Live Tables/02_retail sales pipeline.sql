-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Creating and running Delta Live Tables pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Bronze dataset
-- MAGIC Importing the dataset using cloudFile with AutoLoader

-- COMMAND ----------

-- Creating delta live table

-- COMMAND ----------

create streaming live table customers
comment "The customers buying finished products, ingested from /databricks-datasets."
tblproperties ("quality" = "mapping")
as select * from cloudFiles("/mnt/retail-org/customers/","csv");

-- COMMAND ----------

create streaming live table sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
as select * from cloudFiles("/mnt/retail-org/sales_orders/","json", map("cloudFiles.inferColumnTypes","true"));

-- COMMAND ----------

-- Above pipelines will not run without any cluster or pipeline so we will create one pipeline for it
-- Go to workflows
-- delta live tables
-- create pipeline
-- choose pipeline mode (triggered or continuous)
-- select source notebook and destination
-- choose cluster mode (enhanced autoscaling, legacy autoscaling, fixed size)

-- this pipeline will just ingest the data from source to sink
