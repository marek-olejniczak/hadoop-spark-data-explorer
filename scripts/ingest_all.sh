#!/bin/bash
# Ingest all years: CSV -> HDFS -> parquet (PySpark job) -> CSV cleanup
# Usage: bash scripts/ingest_all.sh
set -u

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CSV_DIR="$PROJECT_ROOT/data/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018/versions/1"
CSV_DIR_IN_CONTAINER="/project/data/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018/versions/1"

SPARK_SUBMIT_ARGS="--master yarn --num-executors 1 --executor-cores 2 --executor-memory 1536m --driver-memory 768m"

for CSV in "$CSV_DIR"/*.csv; do
    YEAR="$(basename "$CSV" .csv)"

    if docker exec hadoop-master hdfs dfs -test -d "/flights/parquet/year=$YEAR" 2>/dev/null; then
        echo "SKIP $YEAR (parquet already exists)"
    else
        echo "PUT $YEAR..."
        docker exec hadoop-master hdfs dfs -put -f \
            "$CSV_DIR_IN_CONTAINER/$YEAR.csv" /flights/raw/ \
            || { echo "FAIL put $YEAR"; exit 1; }

        echo "JOB $YEAR..."
        docker exec hadoop-master spark-submit $SPARK_SUBMIT_ARGS \
            /project/jobs/ingest_to_parquet.py $YEAR \
            > "/tmp/ingest_$YEAR.log" 2>&1 \
            || { echo "FAIL job $YEAR (log: /tmp/ingest_$YEAR.log)"; exit 1; }

        docker exec hadoop-master hdfs dfs -test -d "/flights/parquet/year=$YEAR" \
            || { echo "FAIL missing output $YEAR"; exit 1; }
    fi

    # Cleanup
    docker exec hadoop-master hdfs dfs -rm -skipTrash "/flights/raw/$YEAR.csv" >/dev/null 2>&1
    command rm -f "$CSV"

    SIZE="$(docker exec hadoop-master hdfs dfs -du -s -h "/flights/parquet/year=$YEAR" | awk '{print $1 $2}')"
    echo "DONE $YEAR parquet: $SIZE"
done

echo "ALL DONE"
docker exec hadoop-master hdfs dfs -du -s -h /flights/parquet
