ALTER TABLE lakehouse.silver.order_lines ADD COLUMN discount_pct double;
UPDATE lakehouse.silver.order_lines SET discount_pct = discount * 100;

ALTER TABLE lakehouse.silver.order_lines RENAME COLUMN discount_pct TO discount_percent;

SELECT snapshot_id, committed_at, operation
FROM lakehouse.silver."order_lines$snapshots"
ORDER BY committed_at;