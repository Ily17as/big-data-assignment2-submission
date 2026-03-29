#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"

bash create_index.sh "$INPUT_PATH"
bash store_index.sh

echo "Indexing pipeline completed successfully."
