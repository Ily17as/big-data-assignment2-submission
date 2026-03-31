import math
import re
import sys
from collections import defaultdict

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

K1 = 1.5
B = 0.75
TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
KEYSPACE = "search_engine"


def tokenize(text: str):
    # simple normalization
    return TOKEN_RE.findall(text.lower())

def fetch_one_value(session, query, args):
    rows = list(session.execute(query, args))
    return rows[0] if rows else None

def main() -> None:
    query_text = " ".join(sys.argv[1:]).strip()
    if not query_text:
        print("Usage: spark-submit query.py <query text>", file=sys.stderr)
        sys.exit(1)

    spark = SparkSession.builder.appName("bm25-query").getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    terms = tokenize(query_text)

    if not terms:
        print("No valid query terms found.")
        spark.stop()
        return

    cluster = Cluster(["cassandra-server"])
    session = cluster.connect(KEYSPACE)

    # load corpus stats
    n_row = fetch_one_value(session, "SELECT stat_value FROM corpus_stats WHERE stat_key=%s", ("N",))
    avgdl_row = fetch_one_value(session, "SELECT stat_value FROM corpus_stats WHERE stat_key=%s", ("AVGDL",))

    if n_row is None or avgdl_row is None:
        raise RuntimeError("Corpus stats are missing in Cassandra.")

    n_docs = float(n_row.stat_value)
    avgdl = float(avgdl_row.stat_value)

    doc_meta = {}
    postings_payload = []

    for term in terms:
        vocab_row = fetch_one_value(session, "SELECT df FROM vocabulary WHERE term=%s", (term,))

        if vocab_row is None or vocab_row.df <= 0:
            continue

        df = int(vocab_row.df)
        idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)
        posting_rows = list(session.execute("SELECT doc_id, tf FROM postings WHERE term=%s", (term,)))

        for row in posting_rows:
            doc_id = row.doc_id

            if doc_id not in doc_meta:
                # fetch doc info once
                doc_row = fetch_one_value(
                    session,
                    "SELECT title, doc_len FROM documents WHERE doc_id=%s",
                    (doc_id,),
                )

                if doc_row is None:
                    continue

                doc_meta[doc_id] = {"title": doc_row.title, "doc_len": int(doc_row.doc_len)}

            postings_payload.append((term, doc_id, int(row.tf), idf, doc_meta[doc_id]["doc_len"]))

    if not postings_payload:
        print("No matching documents found.")
        session.shutdown()
        cluster.shutdown()
        spark.stop()
        return

    bc_avgdl = sc.broadcast(avgdl)

    def contribution(rec):
        _, doc_id, tf, idf, doc_len = rec
        # bm25 term score
        denom = tf + K1 * (1.0 - B + B * (doc_len / bc_avgdl.value))
        score = idf * ((tf * (K1 + 1.0)) / denom)
        return doc_id, score

    scores = (sc.parallelize(postings_payload)
        .map(contribution)
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda x: -x[1])
    )

    print(f"Query: {query_text}")
    print("Top 10 documents:")

    for rank, (doc_id, score) in enumerate(scores, start=1):
        title = doc_meta.get(doc_id, {}).get("title", "")
        print(f"{rank}. {doc_id}\t{title}\t{score:.6f}")

    # graceful shutdown
    session.shutdown()
    cluster.shutdown()
    spark.stop()


if __name__ == "__main__":
    main()