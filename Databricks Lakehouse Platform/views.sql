-- Databricks notebook source
create table cars
(carId int,name string, brand string, year int)

-- COMMAND ----------

insert into cars
values
(1,"swift","maruti",2006),
(2,"ciaz","maruti",2009),
(3,"verna","hyundai",2008),
(4,"creta","hyundai",2003),
(5,"eon","tata",2005)

-- COMMAND ----------

select * from cars

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC # Create a stored procedure

-- COMMAND ----------

create view car_view
as select * from cars
where year > 2003

-- COMMAND ----------


select * from car_view

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from car_view
where brand in ("maruti","hyundai")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Temporary view

-- COMMAND ----------

create temporary view temp_car_view
as select distinct brand
from cars

-- COMMAND ----------

select * from temp_car_view

-- COMMAND ----------

show tables

-- COMMAND ----------

drop view if exists temp_car_view1

-- COMMAND ----------

create temporary view temp_car_view1
as select *
from cars
where brand in ("tata","maruti")

-- COMMAND ----------

select * from temp_car_view1
where year < 2009

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Global Temporary View

-- COMMAND ----------

create global temporary view global_temp_view
as select * from cars
where year > 2003
order by year desc

-- COMMAND ----------

select * from global_temp_view

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from global_temp.global_temp_view

-- COMMAND ----------

select * from global_temp.global_temp_view
where brand = "maruti"

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------


