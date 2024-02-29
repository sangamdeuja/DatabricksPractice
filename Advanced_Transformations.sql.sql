-- Databricks notebook source
-- MAGIC %run /Workspace/Repos/DatabricksPractice/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers


-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

-- colon syntax to interact with nested data structure(json)
SELECT customer_id, profile:first_name, profile:address:country 
FROM customers;

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers;


-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT * FROM parsed_customers;

-- COMMAND ----------

DESCRIBE parsed_customers;

-- COMMAND ----------

-- with struct datatypes we can use . syntax instead of : syntax
SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

-- * operation to flatten fields into columns
CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders;

-- COMMAND ----------

-- explode to put each element of array in its own row
SELECT order_id, customer_id, explode(books) AS book 
FROM orders


-- COMMAND ----------

-- collect-set function to collect unique value of field including the unique value for field in array
SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id


-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

-- COMMAND ----------

SELECT * FROM orders_enriched

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;


-- COMMAND ----------

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 


-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 


-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions

-- COMMAND ----------


