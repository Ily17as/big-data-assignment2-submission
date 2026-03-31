#!/bin/bash
set -euo pipefail

service ssh restart
bash start-services.sh

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate

export PIP_DISABLE_PIP_VERSION_CHECK=1

# Install minimal build tooling for fresh clones.
if [ ! -f /usr/include/python3.8/Python.h ] || ! command -v gcc >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends build-essential python3-dev
  rm -rf /var/lib/apt/lists/*
fi

python -m pip install --upgrade pip setuptools wheel
python -m pip install --prefer-binary -r requirements.txt

rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

bash prepare_data.sh 1000
bash index.sh

bash search.sh "computer science"
bash search.sh "artificial intelligence"

echo "Running add_to_index smoke test"

# Smoke test for the optional extra task.
bash add_to_index.sh /app/99999999_GalievTestToken.txt

bash search.sh "galievtesttoken"