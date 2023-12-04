----------------------------------------------------------------------------------------------
-- Beginning of Demo
----------------------------------------------------------------------------------------------
create database dt_database;
use database dt_database;

create schema dt_schema;
use schema dt_schema;

-- Create tables
create or replace table products(
    id int, name string, category string, price number);
create or replace table orders(
    id int, order_time timestamp);
create or replace table lines(
    id int, order_id int, product_id int, count int);

    -- Generate 1k products
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

-- Generate 1k orders
create or replace sequence order_ids start = 2000000;

insert into orders 
  select
    order_ids.nextval
      id,       -- order ids start with '2'
    timeadd(minute, id, '2020-01-01 0:00')
      order_time  -- 1 order per minute starting in 2020.
  from table(generator(rowcount => 1e3));

select count(*) from orders;
  
  -- Generate 10K lines
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
  
show tables in schema;

select * from orders limit 10;
select * from lines limit 15;

-- Pipeline!
-- Goal is to calculate the revenue from the orders
select
 date_trunc(day, order_time) day,
 sum(count) item_count,
 sum(price * count) revenue
from orders o, lines l, products p
where true
and o.id = l.order_id
and p.id = l.product_id
group by day;

 
-- First step for building pipeline to join the tables together

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

-- 2nd step is to build a DT on top of the enrich_lines 
-- analyze
create or replace dynamic table daily_revenue
  lag = '1 minute'
  warehouse = 'DT_WH'
  as
    select date_trunc(day, order_time) day, sum(count) item_count, sum(price * count) revenue
    from enriched_lines
    group by day;

show dynamic tables in schema;

select * from table(information_schema.dynamic_table_graph_history(as_of => $created_first_time));

select * from daily_revenue;

--select name, state, query_id, refresh_version, refresh_start_time, datediff(seconds, refresh_start_time, refresh_end_time) duration 
--  from table(information_schema.dynamic_table_refresh_history(name_prefix => 'TEST'))
--  order by refresh_version desc, refresh_start_time desc;


--select * from table(information_schema.dynamic_table_refresh_history(name => 'TEST.TELCO2.UNKNOWN_PRODUCTS'));  

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

---------------------------------------------------------------------
-- Run Insert products again to show DT gets updated incrementaly
---------------------------------------------------------------------

-- Query unknow_products again to show up to date
select * from unknown_products;


----------------------------------------------------------------------------------------------
-- End of Demo
----------------------------------------------------------------------------------------------
