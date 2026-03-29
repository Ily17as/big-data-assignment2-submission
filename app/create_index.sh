#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"
RAW_OUT="/tmp/indexer/raw"
INDEX_ROOT="/indexer"
LOCAL_TMP=$(mktemp -d)
trap 'rm -rf "$LOCAL_TMP"' EXIT

hdfs dfs -rm -r -f /tmp/indexer || true
hdfs dfs -rm -r -f "$INDEX_ROOT" || true
hdfs dfs -mkdir -p /tmp/indexer

hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming"*.jar \
  -D mapreduce.job.name="assignment2-indexer" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$RAW_OUT"

hdfs dfs -cat "$RAW_OUT"/part-* > "$LOCAL_TMP/raw.tsv"

awk -F '\t' '$1=="DOC"{print $2"\t"$3"\t"$4}' "$LOCAL_TMP/raw.tsv" > "$LOCAL_TMP/documents.tsv"
awk -F '\t' '$1=="VOCAB"{print $2"\t"$3}' "$LOCAL_TMP/raw.tsv" > "$LOCAL_TMP/vocabulary.tsv"
awk -F '\t' '$1=="POSTING"{print $2"\t"$3"\t"$4}' "$LOCAL_TMP/raw.tsv" > "$LOCAL_TMP/postings.tsv"
awk -F '\t' '$1=="STAT"{print $2"\t"$3}' "$LOCAL_TMP/raw.tsv" > "$LOCAL_TMP/stats.tsv"

for part in documents vocabulary postings stats; do
  hdfs dfs -mkdir -p "$INDEX_ROOT/$part"
  hdfs dfs -put -f "$LOCAL_TMP/$part.tsv" "$INDEX_ROOT/$part/part-00000"
done

hdfs dfs -ls -R "$INDEX_ROOT"
echo "Index creation completed successfully."
