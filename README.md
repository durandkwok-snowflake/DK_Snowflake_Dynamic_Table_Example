# DK_Snowflake_Dynamic_Table_Example

Welcome to the Snowflake Dynamic Table Demo! This demo showcases the creation and usage of dynamic tables in Snowflake, focusing on building a pipeline to calculate revenue from orders.

Prerequisites
Before running the demo, make sure you have access to a Snowflake account and have the required privileges to execute the SQL statements provided.

Setup
-- Create a database and schema

create database dt_database;

use database dt_database;

create schema dt_schema;

use schema dt_schema;

-- Create tables
-- (Code for creating tables is provided in the demo script)


-- Create tables
-- (Code for creating tables is provided in the demo script)
Demo Steps
The demo includes the following steps:

Generate Sample Data:

Create tables for products, orders, and order lines.
Generate sample data for 1,000 products, 1,000 orders, and 10,000 order lines.
Explore Data:

Display information about the created tables.
Display sample records from the "orders" and "lines" tables.
Build Pipeline - First Step:

Create a dynamic table named "enriched_lines" by joining the "orders," "lines," and "products" tables.
Display records from the "enriched_lines" table.
Build Pipeline - Second Step:

Create another dynamic table named "daily_revenue" by aggregating data from the "enriched_lines" table.
Display records from the "daily_revenue" table.
Dynamic Table History:

View the history of dynamic tables using the information_schema.dynamic_table_graph_history function.
Additional Examples:

Create a dynamic table named "unknown_products" to validate with referential integrity.
Run additional queries to demonstrate the dynamic updating of tables.
Conclusion
This Snowflake Dynamic Table Demo showcases the power and flexibility of dynamic tables for building data pipelines and performing analytical tasks. Feel free to explore and adapt the provided script to suit your specific use cases.

Enjoy the demo! 🚀
