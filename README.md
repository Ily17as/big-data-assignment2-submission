# Big Data Assignment 2 — Simple Search Engine

This repository contains a simple search engine built with:

- **PySpark** for data preparation
- **Hadoop Streaming MapReduce** for inverted index construction
- **Cassandra** for index storage
- **Spark on YARN** for BM25 search

The assignment specification explicitly says the instructor will clone the repository and run `docker compose up`, and that `app.sh` should start services, run the indexer, and run example queries.

---

## Repository structure

```text
.
├── docker-compose.yml
├── README.md
└── app/
    ├── app.sh
    ├── start-services.sh
    ├── prepare_data.py
    ├── prepare_data.sh
    ├── create_index.sh
    ├── index.sh
    ├── store_index.sh
    ├── load_index.py
    ├── query.py
    ├── search.sh
    ├── add_to_index.sh
    ├── requirements.txt
    ├── data/
    └── mapreduce/
        ├── mapper1.py
        └── reducer1.py
```

Main HDFS outputs used by the pipeline:

- `/data` — plain text documents
- `/input/data` — line-based corpus used as MapReduce input

---

## What the pipeline does

The full pipeline performs these steps:

1. Starts HDFS, YARN, and the Hadoop history server
2. Creates a Python virtual environment inside the container
3. Installs Python dependencies from `requirements.txt`
4. Packs the environment with `venv-pack` so Spark on YARN can use the same Python environment
5. Prepares documents:
   - from `a.parquet` if it exists
   - or from existing local `.txt` files in `app/data` if `a.parquet` is missing
6. Uploads documents to HDFS `/data`
7. Builds the inverted index in HDFS
8. Loads the index into Cassandra
9. Runs BM25 search for example queries
10. Runs a test for `add_to_index.sh`

---

## Prerequisites

You need:

- **Docker**
- **Docker Compose**
- enough RAM for Docker containers
- internet access during the first build so the container can install Python dependencies

On Windows + WSL2, Docker Desktop WSL integration should be enabled.

---

## Input data modes

The project supports **two data preparation modes**.

### Mode 1 — `a.parquet` exists
If `app/a.parquet` is present, the pipeline reads documents from the parquet file and generates a local plain-text corpus plus `/input/data`.

### Mode 2 — `a.parquet` is missing, but `app/data/*.txt` already exists
If `a.parquet` is not present, the pipeline falls back to the existing plain-text files inside `app/data`.

Expected filename format:

```text
<doc_id>_<doc_title>.txt
```

Example:

```text
1_Art.txt
```
---

## How to run the project

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd <repository-folder>
```

### 2. Optional: provide `a.parquet`

If you want to run the parquet-based mode, place the file here:

```text
app/a.parquet
```

If this file is missing, the pipeline will use existing `.txt` documents from:

```text
app/data/
```

### 3. Start the full pipeline

Run from the **repository root**:

```bash
docker compose down -v
docker compose up --build
```

---

## What happens during `docker compose up --build`

The container entrypoint (`app.sh`) performs the main workflow:

- starts SSH and Hadoop services
- creates `.venv` if missing
- installs dependencies from `requirements.txt`
- creates `.venv.tar.gz` with `venv-pack`
- runs:
  - `bash prepare_data.sh 1000`
  - `bash index.sh`
  - `bash search.sh "computer science"`
  - `bash search.sh "artificial intelligence"`

---

## Running search manually

If the cluster is already up and the index is available, you can run a query manually inside the app container:

```bash
bash app/search.sh "computer science"
bash app/search.sh "artificial intelligence"
```

---

## Extra task: add a new document

You can also test the optional indexing extension manually.

Example:

```bash
bash app/add_to_index.sh /app/99999999_GalievTestToken.txt
```

Then query for the new token:

```bash
bash app/search.sh "galievtesttoken"
```

---

## Author

Ilyas Galiev
