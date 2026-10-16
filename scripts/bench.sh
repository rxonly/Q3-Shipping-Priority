set -euo pipefail

TRINO="docker exec -i trino trino --server localhost:8080 --catalog lakehouse"
OUT=benchmark_results.csv
echo "format,codec,write_sec,query_sec_avg,size_mb" > "$OUT"

now() { date +%s.%N; }

for FMT in PARQUET ORC; do
  for CODEC in NONE SNAPPY GZIP ZSTD; do
    SCHEMA="bench_$(echo "$FMT" | tr 'A-Z' 'a-z')_$(echo "$CODEC" | tr 'A-Z' 'a-z')"

    $TRINO --execute "DROP SCHEMA IF EXISTS $SCHEMA CASCADE" >/dev/null 2>&1 || true
    $TRINO --execute "CREATE SCHEMA $SCHEMA"

    START=$(now)
    $TRINO --execute "
      SET SESSION compression_codec = '$CODEC';
      CREATE TABLE $SCHEMA.customer WITH (format='$FMT') AS SELECT custkey,name,nationkey,acctbal,mktsegment FROM tpch.sf1.customer;
      CREATE TABLE $SCHEMA.orders   WITH (format='$FMT') AS SELECT orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,shippriority FROM tpch.sf1.orders;
      CREATE TABLE $SCHEMA.lineitem WITH (format='$FMT') AS SELECT orderkey,linenumber,extendedprice,discount,shipdate FROM tpch.sf1.lineitem;
    " >/dev/null || { echo "$FMT/$CODEC: не поддерживается, пропуск"; continue; }
    END=$(now)
    WRITE=$(echo "$END - $START" | bc)

    Q=$(sed "s/lakehouse\.bronze/$SCHEMA/g; s/ lakehouse\./ /g" sql/q3_on_bronze.sql | sed "s/FROM $SCHEMA\./FROM $SCHEMA./")
    TOTAL=0
    for i in 1 2 3; do
      S=$(now)
      echo "$Q" | $TRINO >/dev/null
      E=$(now)
      TOTAL=$(echo "$TOTAL + $E - $S" | bc)
    done
    AVG=$(echo "scale=2; $TOTAL / 3" | bc)

    BYTES=$(docker run --rm --network lakehouse_default --entrypoint /bin/sh minio/mc:latest -c \
      "mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && mc du --recursive l/lakehouse | tail -1" | awk '{print $1}')
    echo "$FMT,$CODEC,$WRITE,$AVG,$BYTES" | tee -a "$OUT"
  done
done
echo "Готово: $OUT"
