CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

SET SESSION lakehouse.compression_codec = 'ZSTD';

DROP TABLE IF EXISTS lakehouse.silver.order_lines;
CREATE TABLE lakehouse.silver.order_lines
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(orderdate)']
) AS
SELECT
  l.orderkey,
  l.linenumber,
  l.extendedprice,
  l.discount,
  l.extendedprice * (1 - l.discount) AS line_revenue,   -- обогащение: выручка строки
  l.shipdate,
  o.orderdate,
  o.shippriority,
  upper(trim(c.mktsegment)) AS mktsegment                -- нормализация
FROM lakehouse.bronze.customer c
JOIN lakehouse.bronze.orders   o ON c.custkey = o.custkey
JOIN lakehouse.bronze.lineitem l ON l.orderkey = o.orderkey
WHERE upper(trim(c.mktsegment)) = 'BUILDING'
  AND o.orderdate < DATE '1995-03-15'
  AND l.shipdate  > DATE '1995-03-15'
  AND l.extendedprice IS NOT NULL
  AND l.discount BETWEEN 0 AND 1;
