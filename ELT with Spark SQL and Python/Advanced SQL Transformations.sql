-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Extracting the data within json from colon (:) notation and dot (.) notation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Importing the data

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import zipfile
-- MAGIC
-- MAGIC #define the path to the zip file 
-- MAGIC zip_path = "/dbfs/FileStore/client_details.zip"
-- MAGIC
-- MAGIC #extract the files from the zip files
-- MAGIC with zipfile.ZipFile(zip_path,"r") as zip_ref:
-- MAGIC     zip_ref.extractall("/dbfs/FileStore/client_details_extracted")

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/FileStore/"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/FileStore/client_details_extracted/client_details/"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/client_details_extracted/client_details/")
-- MAGIC display(files)
-- MAGIC # this command will display the list of files within provided location

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Creating a table

-- COMMAND ----------

create table client_details as
select * from json.`${dataset.client}/FileStore/client_details_extracted/client_details/client_details.json`

-- COMMAND ----------

select * from client_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By looking at the format of the json table we can determine that there are four root columns or fields i.e., client_id, details, email and updated.
-- MAGIC inside details there are more complex subfields

-- COMMAND ----------

describe extended client_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Use : syntax to query the nested data of json tables 

-- COMMAND ----------

-- we will use below command to get the data from the subfields....
---  Suppose we want to get the first_name and address of the clients but these data resides under details column then we will use below syntax to retrieve the data 

select client_id, details:first_name, details:address: country from client_details

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Parse the data into structure type and use . notation to query the nested fields

-- COMMAND ----------

-- we are going to use json function to struct the data  


-- COMMAND ----------

-- parsing the details folder from json function

select from_json(details) as details_struct
from client_details

-- cannot parse and tell the function to parse all the data from the nested fields within your details column. you have to pass some kind of schema also.

-- COMMAND ----------

select * from client_details
limit 1

-- COMMAND ----------

-- creating a view to structure the data with passing schema 
--- schema can be passed via schema_of_json function

create or replace temp view parsed_client as
select client_id, from_json(details, schema_of_json('{"first_name":"Arnold","last_name":"Addicote","gender":"Male","address":{"street":"55831 Helena Court","city":"Zhangtan","country":"China"}}')) as details_struct
from client_details;

-- COMMAND ----------

select * from parsed_client

-- COMMAND ----------

-- describing the temporary view
describe parsed_client

-- COMMAND ----------

-- we will use below command to retrieve the data of the subfields from temporary view
select client_id, details_struct.first_name, details_struct.address.city, details_struct.last_name, details_struct.gender from parsed_client


-- COMMAND ----------

-- creating a new temp view to struct all data

create or replace temporary view client_new
as select client_id, details_struct.*
from parsed_client

-- COMMAND ----------

select * from client_new

-- COMMAND ----------

--creating a new view to struct the fields of address
create or replace temp view client_address as
select client_id, address.*
from client_new

-- COMMAND ----------

select * from client_address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Working with arrays

-- COMMAND ----------

create table My_table
(id int, my_array array<struct<name string, age int>>)

-- COMMAND ----------

insert into My_table values
(1, array(named_struct('name','john','age',23))),
(2, array(named_struct('name','nikita','age',25),named_struct('name','yatish','age',25))),
(3, array(named_struct('name','kat','age',53),named_struct('name','johnny','age',45),named_struct('name','arpita','age',20)));


-- COMMAND ----------

-- since I have inserted the values on the table in wrong syntax at the first time, it created 5 rows and after correcting the syntax it aaded another 3 rows... to correct the table data i have done overwrite of the table by below command
insert overwrite My_table values
(1, array(named_struct('name','john','age',23))),
(2, array(named_struct('name','nikita','age',25),named_struct('name','yatish','age',25))),
(3, array(named_struct('name','kat','age',53),named_struct('name','johnny','age',45),named_struct('name','arpita','age',20)));

-- COMMAND ----------

select * from My_table

-- COMMAND ----------

-- applying filter on the arrays to fetch the records using our filteration
-- Here we are applying filter to fetch the record where the array contains more than one element
select id, my_array 
from my_table
where size(my_array) > 1

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode function
-- MAGIC The EXPLODE function can now expand more than one array in a single call, removing the need to use subqueries to explode multiple arrays. 

-- COMMAND ----------

select id, explode(my_array) as new_array from my_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting array
-- MAGIC It has multiple functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Collect set function
-- MAGIC

-- COMMAND ----------

CREATE TABLE grocery_items (
  item_id INT,
  item_name STRING,
  category STRING
);

-- COMMAND ----------

INSERT INTO grocery_items VALUES
(1, 'Bananas', 'Fruits'),
(2, 'Apples', 'Fruits'),
(3, 'Oranges', 'Fruits'),
(4, 'Carrots', 'Vegetables'),
(5, 'Broccoli', 'Vegetables'),
(6, 'Spinach', 'Vegetables'),
(7, 'Lamb', 'Meat'),
(8, 'Chicken', 'Meat'),
(9, 'Salmon', 'Seafood'),
(10, 'Shrimp', 'Seafood');

-- COMMAND ----------

select * from grocery_items


-- COMMAND ----------

-- we are collecting items in set based on some grouping
select category, collect_set(item_name) as item_name_new
from grocery_items
group by category

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Flatten function
-- MAGIC Flattening an array is a process of reducing the dimensionality of an array. In other words, it a process of reducing the number of dimensions of an array to a lower number.
-- MAGIC An array is called a nested array when there are arrays inside this array as its elements.

-- COMMAND ----------

create table people 
(name string,
age int,
address array<array<string>>)

-- COMMAND ----------

INSERT INTO people VALUES
  ('Alice', 25, array(array('123 Main St', 'San Francisco', 'CA', '12345'), array('456 Pine St', 'San Jose', 'CA', '67890'))),
  ('Bob', 30, array(array('789 Oak St', 'Los Angeles', 'CA', '45678')))

-- COMMAND ----------

select * from people

-- COMMAND ----------

select name, age, flatten(address) as address_flat
from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Array distinct function -- function to get distinct values

-- COMMAND ----------

CREATE TABLE cars_array (
  name STRING,
  age INT,
  brand ARRAY<ARRAY<STRING>>
);
 


-- COMMAND ----------

INSERT INTO cars_array VALUES
  ('Alice', 25, ARRAY(ARRAY('Ford', 'BMW'), ARRAY('Tesla', 'Ford'))),
  ('Bob', 30, ARRAY(ARRAY('BMW', 'Mercedes-Benz'), ARRAY('Audi', 'Porsche'))),
  ('Carol', 35, ARRAY(ARRAY('Audi', 'Porsche'), ARRAY('Ford', 'Audi'))),
  ('David', 40, ARRAY(ARRAY('BMW', 'Mercedes-Benz'), ARRAY('Audi', 'Porsche')));
  

-- COMMAND ----------

select * from cars_array

-- COMMAND ----------

-- to get the distinct values of car brands we will use distinct function and flatten function on brand
select name, age, array_distinct(flatten(brand)) as distinct_brands
from cars_array

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Join Operation

-- COMMAND ----------

CREATE or replace TEMP VIEW employee(id, name, deptno) AS
     VALUES(105, 'Chloe', 5),
           (103, 'Paul' , 3),
           (101, 'John' , 1),
           (102, 'Lisa' , 2),
           (104, 'Evan' , 4),
           (106, 'Amy'  , 6);


-- COMMAND ----------

create or replace temp view department(deptno, depart_name ) as
values (3, 'Engineering'),
       (2, 'Sales'      ),
       (1, 'Marketing'  )

-- COMMAND ----------

select * from employee


-- COMMAND ----------

select * from department

-- COMMAND ----------

SELECT id, name, employee.deptno, depart_name
FROM employee
INNER JOIN department ON employee.deptno = department.deptno

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Left Join

-- COMMAND ----------

SELECT id, name, employee.deptno, depart_name
FROM employee
LEFT JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Right Join

-- COMMAND ----------

SELECT id, name, employee.deptno, depart_name
FROM employee
RIGHT JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Full Join

-- COMMAND ----------

SELECT id, name, employee.deptno, depart_name
FROM employee
FULL JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cross join
-- MAGIC CROSS JOINs are used to combine each row of one table with each row of another table, and return the Cartesian product of the sets of rows from the tables that are joined. 
-- MAGIC The CROSS JOIN query in SQL is used to generate all combinations of records in two tables.

-- COMMAND ----------

SELECT id, name, employee.deptno, depart_name
FROM employee
CROSS JOIN department 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Semi Join

-- COMMAND ----------

SELECT * FROM employee
SEMI JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Anti Join

-- COMMAND ----------

SELECT * FROM employee
ANTI JOIN department ON employee.deptno = department.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lateral joins

-- COMMAND ----------

SELECT id, name, deptno, depart_name
FROM employee JOIN LATERAL (SELECT depart_name
FROM department
WHERE employee.deptno = department.deptno);

-- COMMAND ----------

SELECT id, name, deptno, depart_name
FROM employee LEFT JOIN LATERAL (SELECT depart_name
FROM department
WHERE employee.deptno = department.deptno);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operators 

-- COMMAND ----------

CREATE TEMPORARY VIEW number1(c) AS VALUES (1), (2), (2), (3), (3), (4), (5), (6), (7), (8), (9), (10);

-- COMMAND ----------

CREATE TEMPORARY VIEW number2(c) AS VALUES (1), (2), (3), (4), (4), (5), (5);

-- COMMAND ----------

SELECT * FROM number1

-- COMMAND ----------

SELECT * FROM number2

-- COMMAND ----------

SELECT c FROM number1 EXCEPT SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 MINUS SELECT c FROM number2;

-- COMMAND ----------

SELECT c FROM number1 EXCEPT ALL (SELECT c FROM number2);

-- COMMAND ----------

SELECT c FROM number1 MINUS ALL (SELECT c FROM number2);

-- COMMAND ----------

(SELECT c FROM number1) INTERSECT (SELECT c FROM number2);

-- COMMAND ----------

(SELECT c FROM number1) INTERSECT DISTINCT (SELECT c FROM number2);

-- COMMAND ----------

(SELECT c FROM number1) INTERSECT ALL (SELECT c FROM number2);

-- COMMAND ----------

(SELECT c FROM number1) UNION (SELECT c FROM number2);

-- COMMAND ----------

(SELECT c FROM number1) UNION ALL (SELECT c FROM number2);

-- COMMAND ----------

(SELECT c FROM number1) UNION DISTINCT (SELECT c FROM number2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pivot Clause

-- COMMAND ----------

CREATE TEMP VIEW sales(year, quarter, region, sales) AS
VALUES
(2018, 1, 'east', 100),
(2018, 2, 'east',  20),
(2018, 3, 'east',  40),
(2018, 4, 'east',  40),
(2019, 1, 'east', 120),
(2019, 2, 'east', 110),
(2019, 3, 'east',  80),
(2019, 4, 'east',  60),
(2018, 1, 'west', 105),
(2018, 2, 'west',  25),
(2018, 3, 'west',  45),
(2018, 4, 'west',  45),
(2019, 1, 'west', 125),
(2019, 2, 'west', 115),
(2019, 3, 'west',  85),
(2019, 4, 'west',  65);

-- COMMAND ----------

Select * from sales;

-- COMMAND ----------

SELECT year, region, q1, q2, q3, q4
FROM sales
PIVOT (sum(sales) AS sales
FOR quarter
IN (1 AS q1, 2 AS q2, 3 AS q3, 4 AS q4));

-- COMMAND ----------

Select * from sales where year = 2018

-- COMMAND ----------

SELECT year, region, sum(sales) FILTER(WHERE quarter = 1) AS q1,
                     sum(sales) FILTER(WHERE quarter = 2) AS q2,
                     sum(sales) FILTER(WHERE quarter = 3) AS q2,
                     sum(sales) FILTER(WHERE quarter = 4) AS q4
FROM sales
GROUP BY year, region;

-- COMMAND ----------


