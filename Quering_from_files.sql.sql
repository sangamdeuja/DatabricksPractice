-- Databricks notebook source
-- MAGIC %run /Workspace/Repos/DatabricksPractice/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC pwd
-- MAGIC ls

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

  SELECT *,input_file_name() source_file FROM JSON.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header ="true",
  delimiter=";"
)
LOCATION "${dataset.bookstore}/books-csv";

-- COMMAND ----------

SELECT * FROM books_csv;

-- COMMAND ----------

DESCRIBE EXTENDED books_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=(spark.read.table("books_csv")
-- MAGIC   .write
-- MAGIC     .mode("append")
-- MAGIC     .format("csv")
-- MAGIC     .option("header","true")
-- MAGIC     .option("delimiter",";")
-- MAGIC     .save(f"{dataset_bookstore}/books-csv")
-- MAGIC
-- MAGIC
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore/books-csv"))

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv;

-- COMMAND ----------

REFRESH TABLE books_csv;
SELECT COUNT(*) FROM books_csv;

-- COMMAND ----------

-- create delta table using CTAS statement for well formated files like json and parquet
CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

DESCRIBE EXTENDED books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);
DESCRIBE EXTENDED books_tmp_vw;

-- COMMAND ----------

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/books"))

-- COMMAND ----------


