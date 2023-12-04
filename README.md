# DK_Snowflake_Dynamic_Table_Example

Welcome to the Snowflake Dynamic Table Demo! This demo showcases the creation and usage of dynamic tables in Snowflake, focusing on building a pipeline to calculate revenue from orders as well as referential integrity.

Prerequisites
Before running the demo, make sure you have access to a Snowflake account and have the required privileges to execute the SQL statements provided.

Setup
---------------------------------------------------------------------
-- Create a database and schema
---------------------------------------------------------------------

create database dt_database;

use database dt_database;

create schema dt_schema;

use schema dt_schema;

---------------------------------------------------------------------
-- Create tables for products, orders, and order lines.
---------------------------------------------------------------------
create or replace table products(
    id int, name string, category string, price number);
    
create or replace table orders(
    id int, order_time timestamp);
    
create or replace table lines(
    id int, order_id int, product_id int, count int);

---------------------------------------------------------------------
-- Next couple of steps is to Generate sample data for 1,000 products, 1,000 orders, and 10,000 order lines.
-- Generate 1k products with sequence
---------------------------------------------------------------------
create or replace sequence product_ids start = 10000;

insert into products
  select
    product_ids.nextval
      id,      -- product ids start with '1'
    randstr(abs(random()) % 7 + 3, random()) 
      name,     -- random strings of length 3–10
    randstr(1, random()) 
      category, -- random strings of length 1
    uniform(0.50, 100, random()) 
      price     -- random between $0.50 and $100
  from table(generator(rowcount => 1e3));

select count(*) from products;

---------------------------------------------------------------------
-- Generate 1k orders
---------------------------------------------------------------------
create or replace sequence order_ids start = 2000000;

insert into orders 
  select
    order_ids.nextval
      id,       -- order ids start with '2'
    timeadd(minute, id, '2020-01-01 0:00')
      order_time  -- 1 order per minute starting in 2020.
  from table(generator(rowcount => 1e3));

select count(*) from orders;

---------------------------------------------------------------------
-- Generate 10K lines
---------------------------------------------------------------------
create or replace sequence line_ids start = 30000000;

insert overwrite into lines
  select
    line_ids.nextval
      id,         -- line ids start with '3'
    trunc(id / 10) - 1e6
      order_id,   -- 10 lines per order
    uniform(10000, 11000, random())
      product_id, -- random product
    uniform(1, 5, random())
      count       -- random count
  from table(generator(rowcount => 1e4));

select count(*) from lines;

---------------------------------------------------------------------
-- Pipeline!
-- Goal is to calculate the revenue from the orders
---------------------------------------------------------------------
select
 date_trunc(day, order_time) day,
 sum(count) item_count,
 sum(price * count) revenue
from orders o, lines l, products p
where true
and o.id = l.order_id
and p.id = l.product_id
group by day;

---------------------------------------------------------------------
-- First step for building pipeline to join the tables together
-- Create a dynamic table named "enriched_lines" by joining the "orders," "lines," and "products" tables.
-- Display records from the "enriched_lines" table.
---------------------------------------------------------------------
-- stage

create or replace dynamic table enriched_lines 
  lag = '1 minute'
  warehouse = 'DT_WH'
as
  select product_id, order_id, l.id line_id, order_time, name, category, price, count
  from orders o, lines l, products p
  where true
    and o.id = l.order_id
    and p.id = l.product_id;

select * from enriched_lines;

set created_first_time = current_timestamp();

<img width="1076" alt="image" src="https://github.com/durandkwok-snowflake/DK_Snowflake_Dynamic_Table_Example/assets/109616231/c20063f2-3b72-4f0a-9f7f-1a650e353152">


---------------------------------------------------------------------
-- 2nd step is to build a DT on top of the enrich_lines 
-- Create another dynamic table named "daily_revenue" by aggregating data from the "enriched_lines" table.
-- Display records from the "daily_revenue" table.
---------------------------------------------------------------------

create or replace dynamic table daily_revenue
  lag = '1 minute'
  warehouse = 'DT_WH'
  as
    select date_trunc(day, order_time) day, sum(count) item_count, sum(price * count) revenue
    from enriched_lines
    group by day;

-- Check to see if Dynamic Tables got created

show dynamic tables in schema;

-- Check to if Daily_Revenue is working

select * from daily_revenue;

<img width="1082" alt="image" src="https://github.com/durandkwok-snowflake/DK_Snowflake_Dynamic_Table_Example/assets/109616231/55ce37cf-0851-420d-9fc9-6d20607bfd6b">


---------------------------------------------------------------------
-- Create Dynamic Table to validate with referential integrity
---------------------------------------------------------------------

create or replace dynamic table unknown_products
  lag = '1 minute'
  warehouse = 'DT_WH'
  as
    select l.id line_id, order_id, product_id
    from lines l left join products p
    on l.product_id = p.id
    where p.id is null;

select * from unknown_products;
-- Noticed there are order lines where product_id is not in product table

<img width="1077" alt="image" src="https://github.com/durandkwok-snowflake/DK_Snowflake_Dynamic_Table_Example/assets/109616231/a58eaa59-1d75-4be6-8070-34c63bb513fb">


---------------------------------------------------------------------
-- Run Insert products again to show DT gets updated incrementaly
---------------------------------------------------------------------

insert into products
  select
    product_ids.nextval
      id,      -- product ids start with '1'
    randstr(abs(random()) % 7 + 3, random()) 
      name,     -- random strings of length 3–10
    randstr(1, random()) 
      category, -- random strings of length 1
    uniform(0.50, 100, random()) 
      price     -- random between $0.50 and $100
  from table(generator(rowcount => 1e3));
  
-- Query unknow_products again to show up to date

select * from unknown_products;


----------------------------------------------------------------------------------------------
-- End of Demo
----------------------------------------------------------------------------------------------


Conclusion
This Snowflake Dynamic Table Demo showcases the power and flexibility of dynamic tables for building data pipelines and performing analytical tasks. Feel free to explore and adapt the provided script to suit your specific use cases.

Enjoy the demo! 🚀
