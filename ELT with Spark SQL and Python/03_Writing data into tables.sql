-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Importing the zip file

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import zipfile
-- MAGIC
-- MAGIC # define the path to the zip file 
-- MAGIC zip_path = "/dbfs/FileStore/employee_details.zip"
-- MAGIC
-- MAGIC # extract the files from the zip file
-- MAGIC with zipfile.ZipFile(zip_path,"r") as zip_ref:
-- MAGIC     zip_ref.extractall("/dbfs/FileStore/extracted_files/")

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/FileStore/"

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/FileStore/extracted_files/employee_details'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating tables

-- COMMAND ----------

-- creating tables from parquet files
create table employee_1 as
select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

select * from employee_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Overwriting the table

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 1. Overwiting the table (CRAS statement)

-- COMMAND ----------

-- overwriting a table based on CRAS statement ( create or replace as)
-- this create or replace table will fully replace the content of the table every time it will execute.
-- in this statement we are replacing the table content with the same data

create or replace table employee_1
as select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

select * from employee_1

-- COMMAND ----------

describe history employee_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Overwriting the table ( INSERT OVERWRITE statement)

-- COMMAND ----------

-- overwriting a table based on Insert overwrite statement 
-- this statement will only execute on the existing table, doesn't create a new table
-- this statement can only overwrite the new record that matches table's current schema, so if you will try to replace with some other schema with new data it won't work

insert overwrite employee_1
select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

describe history employee_1

-- COMMAND ----------

-- trying to insert a new column by this method, which will going to throw an error
insert overwrite employee_1
select *, current_timestamp() from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending records to table (INSERT INTO statement)

-- COMMAND ----------


select * from employee_1

-- COMMAND ----------

insert into employee_1
select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_005.parquet`

-- COMMAND ----------

select * from employee_1

-- COMMAND ----------

-- executing the same query again and it will append the employee_1 with the same data again, it will not check if the data already exists or not, it will just append the data.
-- insert into command doesn't care for existing historical data it will just insert 

insert into employee_1
select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_005.parquet`

-- COMMAND ----------

select count(*) from employee_1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending records to table ( MERGE INTO  statemnt)
-- MAGIC
-- MAGIC It will solve the de-duplicating records problem occurs while using insert into statement
-- MAGIC

-- COMMAND ----------

-- creating new tables 
create table employee_merge1
as select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

select count(*) from employee_merge1

-- COMMAND ----------

create table employee_merge2
as select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_005.parquet`

-- COMMAND ----------

select count(*) from employee_merge2

-- COMMAND ----------

-- by using merge into statement, we are going to insert employee_merge2 table records into employee_merge1 table
-- we are creating alias t and s for the tables

merge into employee_merge1 as t 
using employee_merge2 s on
t.id = s.id
when not matched then
insert *

-- COMMAND ----------

select * from employee_merge1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting new records and updating the old one

-- COMMAND ----------

create table employee_merge_3
as select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files/`

-- COMMAND ----------

select count(*) from employee_merge_3

-- COMMAND ----------

create table employee_merge_4
as select * from parquet.`${dataset.employee}/FileStore/extracted_files/employee_details/parquet_files_new/employee_006.parquet`

-- COMMAND ----------

select * from employee_merge_4

-- COMMAND ----------

-- this statement only updated the email address of the matched records and inserted the new records that doesn't match the condition

merge into employee_merge_3 s
using employee_merge_4 t
on s.id = t.id 
when matched then 
update set s.email = t.email
when not matched then
insert *

-- COMMAND ----------

select count(*) from employee_merge_3

-- COMMAND ----------

describe history employee_merge_3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Deleting all tables.. will create new tables for further labs

-- COMMAND ----------

show tables

-- COMMAND ----------

drop table employee;
drop table employee_1;
drop table employee_merge1;
drop table employee_merge2;
drop table employee_merge_3;
drop table employee_merge_4;

-- COMMAND ----------

show tables

-- COMMAND ----------


