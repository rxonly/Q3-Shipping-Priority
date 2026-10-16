CREATE SCHEMA IF NOT EXISTS lakehouse.gold;

SET SESSION lakehouse.compression_codec = 'ZSTD';

DROP TABLE IF EXISTS lakehouse.gold.shipping_priority_top10;
CREATE TABLE lakehouse.gold.shipping_priority_top10
WITH (format = 'PARQUET') AS
SELECT
  orderkey,
  sum(line_revenue) AS revenue,
  orderdate,
  shippriority
FROM lakehouse.silver.order_lines
GROUP BY orderkey, orderdate, shippriority
ORDER BY revenue DESC
LIMIT 10;

SELECT * FROM lakehouse.gold.shipping_priority_top10 ORDER BY revenue DESC;
