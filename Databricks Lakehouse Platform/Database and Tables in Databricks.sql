-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Managed Tables

-- COMMAND ----------

create table managed_tb_employee
(employeeId int, name string, region string, salary double)

-- COMMAND ----------

select * from managed_tb_employee

-- COMMAND ----------

insert into managed_tb_employee
values (1,"nikita","bangalore",45000.00),
(2,"yatish","dewas",40000.00),
(3,"ravi","bangalore",50000.00)

-- COMMAND ----------

select * from managed_tb_employee 
where employeeId >= 2

-- COMMAND ----------

describe detail managed_tb_employee

-- COMMAND ----------

describe history managed_tb_employee

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/managed_tb_employee"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use below command to get the metadata information of the table

-- COMMAND ----------

describe extended managed_tb_employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating metadata by adding a new column to the existing table

-- COMMAND ----------

alter table managed_tb_employee
add columns (age int)

-- COMMAND ----------

select * from managed_tb_employee

-- COMMAND ----------

describe extended managed_tb_employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. External Table
-- MAGIC

-- COMMAND ----------

create table unmanaged_tb_company
(name string,companyId int,location string) location "/dbfs/mnt/demo/external_table"

-- COMMAND ----------

insert into unmanaged_tb_company
values
("Dxc Technology",012,"bangalore"),
("Infosys",213,"Indore"),
("yash Technology",317,"indore"),
("happiest minds technology",098,"hyderabad")

-- COMMAND ----------

select * from unmanaged_tb_company

-- COMMAND ----------

describe extended unmanaged_tb_company

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping a managed table
-- MAGIC
-- MAGIC It will delete all the metadata and data of the table

-- COMMAND ----------

select * from managed_tb_employee

-- COMMAND ----------

drop table managed_tb_employee

-- COMMAND ----------

select * from managed_tb_employee

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/managed_tb_employee"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping an external table
-- MAGIC
-- MAGIC It will delete the metadata of the table but we can still list down the parquet files and delta log files by using external location 
-- MAGIC

-- COMMAND ----------

select * from unmanaged_tb_company

-- COMMAND ----------

drop table unmanaged_tb_company

-- COMMAND ----------

select * from unmanaged_tb_company

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/dbfs/mnt/demo/external_table'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Creating a new schema apart from the default schema

-- COMMAND ----------

create schema mydb

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using below magic command to get the information of the new database

-- COMMAND ----------

describe database extended mydb

-- COMMAND ----------

use mydb;
create table company
(name string,companyId int,location string)

-- COMMAND ----------

insert into company
values
("Dxc Technology",012,"bangalore"),
("Infosys",213,"Indore"),
("yash Technology",317,"indore"),
("happiest minds technology",098,"hyderabad")

-- COMMAND ----------

select * from company

-- COMMAND ----------

create table employee
(name string, age int, location string)

-- COMMAND ----------

insert into employee
values
("nikita", 26, "bangalore"),
("yatish", 25, "Indore")

-- COMMAND ----------

select * from employee


-- COMMAND ----------

describe database extended mydb

-- COMMAND ----------

show tables

-- COMMAND ----------

drop table stationary


-- COMMAND ----------

create table stationary
(item string, price double, quantity int) location "/mnt/demo/ext"

-- COMMAND ----------

insert into stationary
values
("pen", 20.00,10 ),
("notebook",50.00, 5),
("marker",30.00,3),
("sticky notes",15.00,2)

-- COMMAND ----------

select * from stationary

-- COMMAND ----------

describe extended company

-- COMMAND ----------

describe extended stationary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping both managed and external table

-- COMMAND ----------


drop table employee;
drop table stationary;

-- COMMAND ----------

select * from employee 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## listing the data from the dropped tables

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/mydb.db/company'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/ext/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Creating new schema in a custom location outside of the hive directory

-- COMMAND ----------

drop database mydb2

-- COMMAND ----------

create database mydb2
location "dbfs:/shared/schema/mydb2.db"


-- COMMAND ----------

describe database extended mydb2

-- COMMAND ----------

use database mydb2;
create table customers 
(name string, id int, address varchar(40),mobileno varchar(15));
insert into customers values ("nikita",1,"awas nagar","9340810024"),("yatish",2,"bherugarh","9340142278")

-- COMMAND ----------

select * from customers

-- COMMAND ----------

drop table shop

-- COMMAND ----------

use database mydb2;
create table shop
(name string, area string, areacode int) location "dbfs:/mnt/shop"

-- COMMAND ----------

insert into shop
values
("general store","awas nagar",455001),
("cosmetic shop","bnp",455001)

-- COMMAND ----------

describe extended customers

-- COMMAND ----------

describe extended shop


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## deleting both table and check for data files

-- COMMAND ----------

drop table customers;
drop table shop;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/shared/schema/mydb2.db/customers'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/shop'
