#!/bin/bash
set -euo pipefail

source .venv/bin/activate

DOC_PATH="${1:-}"

if [ -z "$DOC_PATH" ]; then
  echo "Usage: bash add_to_index.sh /path/to/document.txt"
  exit 1
fi

if [ ! -f "$DOC_PATH" ]; then
  echo "[ERROR] file not found: $DOC_PATH"
  exit 1
fi

echo "[STEP 1/5] Preparing metadata for the new document"

BASE_NAME="$(basename "$DOC_PATH")"
STEM="${BASE_NAME%.txt}"

DOC_ID="${STEM%%_*}"
TITLE_PART="${STEM#*_}"

if [ "$TITLE_PART" = "$STEM" ]; then
  TITLE_PART="$STEM"
fi

TITLE="${TITLE_PART//_/ }"

SAFE_NAME=$(python3 - "$BASE_NAME" <<'PY'
import os
import re
import sys
import unicodedata

# Keep HDFS-safe ASCII file names.
name = sys.argv[1]
root, ext = os.path.splitext(name)
root = unicodedata.normalize("NFKD", root).encode("ascii", "ignore").decode("ascii")
root = re.sub(r"[^A-Za-z0-9._-]+", "_", root)
root = re.sub(r"_+", "_", root).strip("._")
if not root:
    root = "new_document"
if not ext:
    ext = ".txt"
print(root + ext)
PY
)

TEXT=$(python3 - "$DOC_PATH" <<'PY'
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
text = re.sub(r"\s+", " ", text).strip()
print(text)
PY
)

if [ -z "$TEXT" ]; then
  echo "[ERROR] document text is empty after normalization"
  exit 1
fi

mkdir -p data
cp "$DOC_PATH" "data/$SAFE_NAME"
echo "[INFO] local copy created: data/$SAFE_NAME"

echo "[STEP 2/5] Uploading the new file to HDFS /data"
hdfs dfs -mkdir -p /data

start_ts=$(date +%s)
hdfs dfs -put -f "data/$SAFE_NAME" /data/
end_ts=$(date +%s)

echo "[UPLOAD][1/1] 100% elapsed=$((end_ts - start_ts))s file=$SAFE_NAME"

echo "[STEP 3/5] Rebuilding /input/data with the new document"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

EXISTING="$TMP_DIR/existing.tsv"
MERGED="$TMP_DIR/input.tsv"

hdfs dfs -cat /input/data/part-* > "$EXISTING" 2>/dev/null || true

python3 - "$EXISTING" "$MERGED" "$DOC_ID" "$TITLE" "$TEXT" <<'PY'
import os
import sys

existing_path, merged_path, doc_id, title, text = sys.argv[1:]
lines = []
replaced = False

if os.path.exists(existing_path):
    with open(existing_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            parts = line.split("\t", 2)
            if len(parts) != 3:
                continue
            # Replace by doc_id to avoid duplicates.
            if parts[0] == doc_id:
                lines.append(f"{doc_id}\t{title}\t{text}")
                replaced = True
            else:
                lines.append(line)

if not replaced:
    lines.append(f"{doc_id}\t{title}\t{text}")

with open(merged_path, "w", encoding="utf-8") as out:
    for line in lines:
        out.write(line + "\n")

print(f"[INFO] rebuilt /input/data with total_docs={len(lines)} replaced_existing={replaced}")
PY

hdfs dfs -rm -r -f /input/data >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /input/data
hdfs dfs -put -f "$MERGED" /input/data/part-00000
hdfs dfs -ls /input/data || true

echo "[STEP 4/5] Rebuilding the index and reloading Cassandra"
# Full rebuild is simpler than partial merge here.
bash index.sh

echo "[STEP 5/5] Smoke-test search"
bash search.sh "$TITLE" || true

echo "[DONE] add_to_index finished successfully"