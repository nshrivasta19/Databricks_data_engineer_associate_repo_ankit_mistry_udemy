-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating a dataset
-- MAGIC

-- COMMAND ----------

-- creating a temp view foods with food as a column
create or replace temporary view foods(food) as values 
('Noodles'),
('Manchurian'),
('Rice'),
('Fruit Chaat'),
('Chilli Paneer');

select * from foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Applying transformation using UDF

-- COMMAND ----------

-- we want to apply some transformation on our table data such as changing the words in Uppercase or concatation of the strings etc.
-- For eg.
-- mean => NOODLES !!

-- COMMAND ----------

-- now we will write some user defined functions to perform our required actions
create or replace function food_change(text string)
returns string
return concat(upper(text), "!!")

-- COMMAND ----------

select food_change(food) from foods

-- COMMAND ----------

describe function food_change

-- COMMAND ----------

describe function extended food_change

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Case / When

-- COMMAND ----------

-- using case and when to apply some conditions like we used to do in python.. here we are writing some strings in case statements
-- by using the case it has attached the strings on the matched cases and else where it has return value as null
select *,
case 
  when food = "Noodles" then " I love noodles.."
  when food = 'Fruit Chaat' then " I love fruits.!"
  end
  from foods

-- COMMAND ----------

select *,
case 
  when food = "Noodles" then " I love noodles.."
  when food = 'Manchurian' then " I don't like it that much"
  when food <> "Rice" then concat("Do you have any good recipe for ",food," ?")
  else concat(" I am bored of ",food )
  end
  from foods

-- COMMAND ----------

-- instead of writing the whole logic in the query, we can create a function to contain all the logics and then call that function in the code
create function food_i_like(food string)
returns string
return case 
  when food = "Noodles" then " I love noodles.."
  when food = 'Manchurian' then " I don't like it that much"
  when food <> "Rice" then concat("Do you have any good recipe for ",food," ?")
  else concat(" I am bored of ",food )
  end

-- COMMAND ----------

select food_i_like(food) from foods

-- COMMAND ----------


