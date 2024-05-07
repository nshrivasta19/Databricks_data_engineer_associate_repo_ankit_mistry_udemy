-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Lake Tables Lab 1 and 2
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a table into Databricks Delta Lake
-- MAGIC

-- COMMAND ----------

CREATE TABLE Employee
(EmployeeID INT, Name STRING, Title STRING, Salary DOUBLE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting data into Delta Lake Table

-- COMMAND ----------

insert into employee
values
(1,"nikita","data engineer",44000.00),
(2,"yatish","trainer",30000.00)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Retrieved the data by using select query
-- MAGIC

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use describle query to get the metadata of any table
-- MAGIC

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ## use below magic command to list down all the files from any location

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee/_delta_log/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating some data in the existing table

-- COMMAND ----------

UPDATE employee
SET Salary = salary + 10000
where Title = "trainer"

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee/_delta_log/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use below magic command to display the content of a json file

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000002.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use below command to describe history of the table
-- MAGIC

-- COMMAND ----------

describe history employee
