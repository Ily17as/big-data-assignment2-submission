#!/bin/bash
set -euo pipefail

echo "Running BM25 search on YARN"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

QUERY_TEXT="$*"
if [ -z "$QUERY_TEXT" ]; then
  echo "Usage: bash search.sh <query text>"
  exit 1
fi

# Ship the packed virtualenv so executors use the same Python setup.
spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives .venv.tar.gz#.venv \
  query.py "$QUERY_TEXT"