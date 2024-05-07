-- Databricks notebook source
-- MAGIC %fs ls dbfs:/FileStore/extracted_files/employee_details/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Interacting with json files 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/json_files/")
-- MAGIC display(files)
-- MAGIC # this command will display the list of files within provided location

-- COMMAND ----------

-- we will now look at the data in json files by below commands
-- we are using json. in the command because we want to read json files
-- json. is json parsing to read the json files

-- COMMAND ----------

-- reading the data from single json file
select * from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files/employee_001.json`

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ## we can use wildcard (*) in the above command to view all json file data together

-- COMMAND ----------

-- reading all the json files using wildcard
select * from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files/employee_*.json`

-- COMMAND ----------

-- reading all the json files using folder
select * from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- using count to know the total no. of records
select count(*) from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- use input_file_name() function to get the source file name for each single record.
select input_file_name() source_file 
 from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- use * along with the input_file_name() source_file function  to get the data with source_file info
select *, input_file_name() source_file
from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- instead of json parsing, if we use text then it will not be able to parse every single stuff of json file
select * from text.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- reading the json file data as a binary file
select * from binaryFile.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Interacting with csv files

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # listing all the csv files 
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/csv_files/")
-- MAGIC display(files
-- MAGIC         )

-- COMMAND ----------

-- using csv. parsing to read the data
select * from csv.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

-- using count to know the total no. of records
select count(*) from csv.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

-- instead of csv parsing, if we use text then it will not be able to parse every single stuff of csv file
select * from text.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

-- using binaryFile to view the csv data 
select * from binaryFile.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating a table and see the difference between delta and non delta tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. creating table from csv files (non-delta table)

-- COMMAND ----------

-- we have seen in previous cell that csv parsing has some problem and it has additional 4 header rows
-- by creating this table, we have provided the schema of the table and column names have been correct and that problem has been resolved 
-- this table has been created by the csv files we have in our dbfs location

create table employee_csv
(id string, first_name string, last_name string, email string, gender string, salary double, team string)
using csv
options (
  header = "true",
  delimeter = ","
)
location "${dataset.employee}/FileStore/extracted_files/employee_details/csv_files"

-- COMMAND ----------

select * from employee_csv

-- COMMAND ----------

describe extended employee_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # listing all the csv files
-- MAGIC
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/csv_files/")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # adding some more data in csv files using python
-- MAGIC # spark.read.table() will read from the table provided. write.mode() will append the table. format() is file format.
-- MAGIC # option() can be used to customize the behavior of reading or writing, such as controlling behavior of the header, delimiter character, character set, and so on.
-- MAGIC # save() will save to specified location.
-- MAGIC
-- MAGIC (spark.read.table("employee_csv").write.mode("append").format("csv").option('header','true').option('delimeter',',').save("dbfs:/FileStore/extracted_files/employee_details/csv_files/"))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/extracted_files/employee_details/csv_files/")
-- MAGIC display(files)

-- COMMAND ----------

-- since this is a non delta table, it is not giving latest information because the are caching this data somewhere in a local storage and they are giving this information from that local storage only.
select count(*) from employee_csv

-- COMMAND ----------

-- refreshing the table to get the latest information about the non delta table employee_csv
refresh table employee_csv

-- COMMAND ----------

select count(*) from employee_csv


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. creating table from json files (delta table)

-- COMMAND ----------

-- creating a delta table from json files
create table employee_json2
as select * from json.`${dataset.employee}/FileStore/extracted_files/employee_details/json_files/`

-- COMMAND ----------

select * from employee_json2

-- COMMAND ----------

describe extended employee_json2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. creating table from csv files (delta table)

-- COMMAND ----------

-- creating a delta table using csv files and retrieving the data
create table employee_csv_unparsed
as select * from csv.`${dataset.employee}/FileStore/extracted_files/employee_details/csv_files/`;

select * from employee_csv_unparsed

-- COMMAND ----------

-- we again facing same issue where there are 8 header rows added in the data and inproper header format due to parsing issue
-- we will now solve this proble by creating a temporary view

-- we will create a view by below statement
create temporary view employee_temp_csv
(id string, first_name string, last_name string, email string, gender string, salary double, team string)
using csv
options
(
 path = "${dataset.employee}/FileStore/extracted_files/employee_details/csv_files",
 header = "true",
 delimeter = ','
);

-- now we will create a new table by using temporary view employee_temp_csv
create table employee_csv_new
as select * from employee_temp_csv;

-- now we will retrieve the data from employee_csv_new
select * from employee_csv_new

-- COMMAND ----------

describe extended employee_csv_new

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. creating table from parquet files (delta table)

-- COMMAND ----------

-- creating a delta file using parquet files

create table employee_parquet
as select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

select * from employee_parquet

-- COMMAND ----------

describe extended employee_parquet

-- COMMAND ----------

show tables

-- COMMAND ----------

drop table employee_csv;
drop table employee_csv_new;
drop table employee_csv_unparsed;
drop table employee_json2;
drop table employee_parquet;
drop table employee_temp_csv

-- COMMAND ----------


