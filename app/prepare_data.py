import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

N_DOCS = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
LOCAL_DATA_DIR = Path("data")
INPUT_HDFS_PATH = "/input/data"
HDFS_DATA_DIR = "/data"


def safe_text(value: str) -> str:
    if value is None:
        return ""
    
    value = value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value

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

def main() -> None:
    spark = (
        SparkSession.builder.appName("data preparation")
        .master("local[1]")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.files.maxPartitionBytes", "16m")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    parquet_path = resolve_parquet_path(spark)

    print(f"Using parquet source: {parquet_path}")

    df = spark.read.parquet(parquet_path)
    df = df.select("id", "title", "text")
    df = df.filter(
            F.col("id").isNotNull()
            & F.col("title").isNotNull()
            & F.col("text").isNotNull()
            & (F.length(F.trim(F.col("text"))) > 0)
        ).limit(N_DOCS)

    rows = df.collect()

    if len(rows) < N_DOCS:
        print(f"Warning: only {len(rows)} non-empty documents found, expected at least {N_DOCS}.")

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
        
        safe_title = re.sub(r"[^A-Za-z0-9._-]+", "_", title).strip("._")

        if not safe_title:
            safe_title = "untitled"

        filename = f"{doc_id}_{safe_title}.txt"
        filepath = LOCAL_DATA_DIR / filename
        filepath.write_text(text, encoding="utf-8")
        prepared.append((doc_id, title.replace("\t", " "), text))

    if not prepared:
        raise RuntimeError("No valid documents were prepared.")

    lines_rdd = spark.sparkContext.parallelize([f"{doc_id}\t{title}\t{text}" for doc_id, title, text in prepared], 1,)

    lines_rdd.saveAsTextFile(INPUT_HDFS_PATH)

    print(f"Prepared {len(prepared)} documents in local folder: {LOCAL_DATA_DIR}")
    print(f"Prepared input lines in HDFS: {INPUT_HDFS_PATH}")
    print(f"Next step: upload local text files to HDFS folder {HDFS_DATA_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()