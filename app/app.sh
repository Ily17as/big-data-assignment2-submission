#!/bin/bash
set -euo pipefail

service ssh restart
bash start-services.sh

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate

pip install -r requirements.txt

rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

bash prepare_data.sh 1000
bash index.sh

bash search.sh "computer science"
bash search.sh "artificial intelligence"

echo "Running add_to_index smoke test"

bash add_to_index.sh /app/99999999_GalievTestToken.txt

bash search.sh "galievtesttoken"