-- Databricks notebook source
CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5),
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3),
  (5, "Anna", 2500.0),
  (6, "Kim", 6200.3);

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- Check metadata info of table

DESCRIBE DETAIL employees;

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/employees/_delta_log/"

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

DESCRIBE EXTENDED employees;

-- COMMAND ----------

DELETE FROM employees;
SELECT * FROM employees;

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 3;

-- COMMAND ----------

SELECT * FROM employees@v3;

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 3;

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS;

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SELECT * FROM employees@v2

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

DROP TABLE employees;

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------


