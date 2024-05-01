-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Creating a table with nested data to apply transformations on it
-- MAGIC

-- COMMAND ----------

-- creating a table with nested data
CREATE OR REPLACE TEMPORARY VIEW nested_data AS
SELECT   id AS key,
         ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT), CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT)) AS values
         ,
         ARRAY(ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT)), ARRAY(CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT))) AS nested_values
FROM range(5)

-- COMMAND ----------

select * from nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lambda Function -- 
-- MAGIC Lambda function is similar to user defined function without a name. They are anonymous functions.
-- MAGIC Lambda functions are efficient whenever you want to create a function that will only contain simple expressions – that is, expressions that are usually a single line of a statement. They're also useful when you want to use the function once.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Applying transformation on values column and store that value in third column nested_values

-- COMMAND ----------

-- using lambda function to transform the values
SELECT  key,
        values,
        TRANSFORM(values, value -> value + 5) AS values_plus_five
FROM    nested_data

-- COMMAND ----------

-- transform values of nested data table using key and values... So for the third column, it will add key in the values 
SELECT  key,
        values, nested_values,
        TRANSFORM(values, value -> value + key) AS values_plus_key
FROM  nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Applying transformation on nested_values column
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT   key, values,
         nested_values,
         TRANSFORM(nested_values,
           values -> TRANSFORM(values,
             value -> value + key + SIZE(values))) AS new_nested_values
FROM     nested_data

-- COMMAND ----------

select key, 
values,
transform(values, value -> value + key ) tranformed_value
from nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exists function 
-- MAGIC The EXISTS operator returns true if the subquery returns at least one record and false if no row is selected. 

-- COMMAND ----------

SELECT   key,
         values,
         EXISTS(values, value -> value % 10 == 3) filtered_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Filter function
-- MAGIC The filter clause is another tool for filtering in SQL. The filter clause is used to, as the name suggests, filter the input data to an aggregation function. They differ from the WHERE clause because they are more flexible than the WHERE clause. Only one WHERE clause can be used at a time in a query. While on the other hand, multiple FILTER clauses can be present in a single query.
-- MAGIC you can filter records to extract only those records that fulfill a specific expression. 
-- MAGIC FILTER is a modifier used on an aggregate function to limit the values used in an aggregation. 

-- COMMAND ----------

SELECT   key,
         values,
         FILTER(values, value -> value > 50) filtered_values
FROM     nested_data

-- COMMAND ----------

SELECT   key,
         values,
         REDUCE(values, 0, (value, acc) -> value + acc, acc -> acc) summed_values,
         REDUCE(values, 0, (value, acc) -> value + acc) summed_values_simple
FROM     nested_data1

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 1+35+18+92+48

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Reduce function zero se start hota h and sari digits ko original digit me accumulate krke sum krta h... so we can use reduce function to sum the values
-- MAGIC

-- COMMAND ----------

# 
