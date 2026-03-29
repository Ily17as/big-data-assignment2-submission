#!/bin/bash
set -euo pipefail

source .venv/bin/activate

EXPORT_DIR=/tmp/index_export
rm -rf "$EXPORT_DIR"
mkdir -p "$EXPORT_DIR"

hdfs dfs -cat /indexer/documents/part-* > "$EXPORT_DIR/documents.tsv"
hdfs dfs -cat /indexer/vocabulary/part-* > "$EXPORT_DIR/vocabulary.tsv"
hdfs dfs -cat /indexer/postings/part-* > "$EXPORT_DIR/postings.tsv"
hdfs dfs -cat /indexer/stats/part-* > "$EXPORT_DIR/stats.tsv"

python load_index.py "$EXPORT_DIR"
