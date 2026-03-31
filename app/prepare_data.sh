#!/bin/bash
set -euo pipefail

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

N_DOCS="${1:-1000}"

hdfs dfs -rm -r -f /data >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /input >/dev/null 2>&1 || true

if [ -f a.parquet ]; then
  echo "[PREP] parquet mode: using /app/a.parquet"
  hdfs dfs -test -e /a.parquet && hdfs dfs -rm -f /a.parquet || true
  hdfs dfs -put -f a.parquet /a.parquet

  rm -rf data

  spark-submit \
    --driver-memory 4g \
    --conf spark.executor.memory=4g \
    --conf spark.sql.parquet.enableVectorizedReader=false \
    --conf spark.sql.files.maxPartitionBytes=16m \
    prepare_data.py "$N_DOCS"
else
  local_count=$(find data -type f -name '*.txt' 2>/dev/null | wc -l | tr -d ' ')
  if [ "${local_count:-0}" -eq 0 ]; then
    echo "ERROR: a.parquet not found in /app and no local .txt documents found in /app/data"
    exit 1
  fi

  echo "[PREP] fallback mode: a.parquet is missing, using ${local_count} existing local documents from /app/data"

  # Reuse local plain-text corpus when parquet is unavailable.
  spark-submit \
    --driver-memory 2g \
    --conf spark.executor.memory=2g \
    prepare_data.py --from-local "$N_DOCS"
fi

hdfs dfs -mkdir -p /data

# Upload staged documents to HDFS with simple progress logging.
total=$(find data -type f -name '*.txt' | wc -l)
i=0
failed=0
start_ts=$(date +%s)

echo "[UPLOAD] starting upload to /data: total_files=${total}"

find data -type f -name '*.txt' -print0 | while IFS= read -r -d '' file; do
  i=$((i+1))

  if ! hdfs dfs -put -f "$file" /data/; then
    failed=$((failed+1))
    echo "[UPLOAD][ERROR] failed file=$(basename "$file")"
  fi

  if (( i == 1 || i % 25 == 0 || i == total )); then
    now=$(date +%s)
    elapsed=$((now - start_ts))
    pct=$((100 * i / total))
    echo "[UPLOAD][${i}/${total}] ${pct}% elapsed=${elapsed}s failed=${failed} file=$(basename "$file")"
  fi
done

hdfs dfs -ls /data | head -20 || true
hdfs dfs -ls /input/data || true

echo "Data preparation completed successfully."