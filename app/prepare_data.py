import argparse
import re
import shutil
import sys
from pathlib import Path
import unicodedata

from pyspark.sql import SparkSession, functions as F

LOCAL_DATA_DIR = Path("data")
INPUT_HDFS_PATH = "/input/data"
HDFS_DATA_DIR = "/data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("n_docs", nargs="?", default=1000, type=int)
    # Fallback mode: reuse existing local .txt files.
    parser.add_argument("--from-local", action="store_true", help="Build /input/data from existing local text files in ./data")
    return parser.parse_args()


def safe_text(value: str) -> str:
    if value is None:
        return ""

    value = value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def sanitize_title_for_filename(title: str) -> str:
    title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    title = re.sub(r"[^A-Za-z0-9._-]+", "_", title).strip("._")
    return title or "untitled"


def resolve_parquet_path(spark: SparkSession) -> str:
    candidates = [
        "hdfs://cluster-master:9000/a.parquet",
        "hdfs:///a.parquet",
        "a.parquet",
        "/app/a.parquet",
        "/a.parquet",
    ]

    last_error = None

    for path in candidates:
        try:
            spark.read.parquet(path).limit(1).count()
            return path

        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Could not read parquet from any known path. Last error: {last_error}")


def prepare_from_parquet(spark: SparkSession, n_docs: int):
    parquet_path = resolve_parquet_path(spark)
    print(f"Using parquet source: {parquet_path}")

    df = spark.read.parquet(parquet_path)
    df = df.select("id", "title", "text")
    df = df.filter(
        F.col("id").isNotNull()
        & F.col("title").isNotNull()
        & F.col("text").isNotNull()
        & (F.length(F.trim(F.col("text"))) > 0)
    ).limit(n_docs)

    rows = df.collect()
    if len(rows) < n_docs:
        print(f"Warning: only {len(rows)} non-empty documents found, expected at least {n_docs}.")

    if LOCAL_DATA_DIR.exists():
        shutil.rmtree(LOCAL_DATA_DIR)
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    prepared = []
    for row in rows:
        doc_id = str(row["id"]).strip()
        title = safe_text(str(row["title"]))
        text = safe_text(str(row["text"]))
        if not doc_id or not title or not text:
            continue

        filename = f"{doc_id}_{sanitize_title_for_filename(title)}.txt"
        filepath = LOCAL_DATA_DIR / filename
        filepath.write_text(text, encoding="utf-8")
        prepared.append((doc_id, title.replace("\t", " "), text))

    return prepared


def parse_doc_metadata(path: str):
    name = Path(path).name
    stem = name[:-4] if name.lower().endswith(".txt") else name
    parts = stem.split("_", 1)
    doc_id = parts[0].strip()
    title = parts[1].replace("_", " ").strip() if len(parts) > 1 else doc_id
    return doc_id, title or doc_id


def prepare_from_local_docs(spark: SparkSession, n_docs: int):
    abs_dir = LOCAL_DATA_DIR.resolve()
    if not abs_dir.exists():
        raise RuntimeError("Local data directory does not exist: ./data")

    files = sorted(abs_dir.glob("*.txt"))
    if not files:
        raise RuntimeError("No local .txt documents found in ./data")

    if n_docs > 0:
        files = files[: min(n_docs, len(files))]

    print(f"Using existing local documents from: {abs_dir} (count={len(files)})")

    # Read local text files through Spark to keep the same preparation flow.
    paths = [f"file://{p}" for p in files]
    rdd = spark.sparkContext.parallelize(paths, 1).map(
        lambda p: (p, Path(p[7:]).read_text(encoding="utf-8", errors="ignore"))
    )

    prepared = []
    for path, raw_text in rdd.collect():
        doc_id, title = parse_doc_metadata(path)
        text = safe_text(raw_text)
        if not doc_id or not title or not text:
            continue
        prepared.append((doc_id, title.replace("\t", " "), text))

    if not prepared:
        raise RuntimeError("No valid local documents were prepared from ./data")

    return prepared


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("data preparation")
        .master("local[1]")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.files.maxPartitionBytes", "16m")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    if args.from_local:
        prepared = prepare_from_local_docs(spark, args.n_docs)
    else:
        prepared = prepare_from_parquet(spark, args.n_docs)

    # Produce the line-based corpus expected by Hadoop Streaming.
    lines_rdd = spark.sparkContext.parallelize(
        [f"{doc_id}\t{title}\t{text}" for doc_id, title, text in prepared],
        1,
    )
    lines_rdd.saveAsTextFile(INPUT_HDFS_PATH)

    print(f"Prepared {len(prepared)} documents in local folder: {LOCAL_DATA_DIR}")
    print(f"Prepared input lines in HDFS: {INPUT_HDFS_PATH}")
    print(f"Next step: upload local text files to HDFS folder {HDFS_DATA_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()