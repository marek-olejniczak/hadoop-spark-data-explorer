#!/bin/bash
# Export computed aggregates from HDFS to the local results/ directory
# Usage: bash scripts/export_results.sh
set -eu

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$PROJECT_ROOT/results"

# Everything runs inside the container so file ownership (root) is not an issue
docker exec hadoop-master bash -c '
  dest=/project/results
  rm -rf "$dest"
  mkdir -p "$dest"
  hdfs dfs -get /flights/results/* "$dest"
  for dir in "$dest"/*/; do
    [ -d "$dir" ] || continue
    name=$(basename "$dir")
    part=$(find "$dir" -name "part-*.parquet" | head -1)
    [ -n "$part" ] || { echo "FAIL: no parquet in $name"; exit 1; }
    mv "$part" "$dest/$name.parquet"
    rm -rf "$dir"
    echo "OK $name.parquet"
  done
  # Make files accessible to the host user (UID 1000 = marek)
  chown -R 1000:1000 "$dest"
'

command ls -lh "$DEST"
