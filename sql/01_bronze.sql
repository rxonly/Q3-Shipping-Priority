CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

SET SESSION lakehouse.compression_codec = 'ZSTD';

DROP TABLE IF EXISTS lakehouse.bronze.customer;
CREATE TABLE lakehouse.bronze.customer
WITH (format = 'PARQUET') AS
SELECT custkey, name, nationkey, acctbal, mktsegment
FROM tpch.sf1.customer;

DROP TABLE IF EXISTS lakehouse.bronze.orders;
CREATE TABLE lakehouse.bronze.orders
WITH (format = 'PARQUET') AS
SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, shippriority
FROM tpch.sf1.orders;

DROP TABLE IF EXISTS lakehouse.bronze.lineitem;
CREATE TABLE lakehouse.bronze.lineitem
WITH (format = 'PARQUET') AS
SELECT orderkey, linenumber, extendedprice, discount, shipdate
FROM tpch.sf1.lineitem;
