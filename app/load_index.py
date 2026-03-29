import base64
import sys
import time
from pathlib import Path

from cassandra.cluster import Cluster

BASE_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/index_export")
KEYSPACE = "search_engine"


def decode_title(value: str) -> str:
    return base64.b64decode(value.encode("ascii")).decode("utf-8")

def connect_with_retry(hosts, attempts=20, delay=3):
    last_exc = None
    for _ in range(attempts):
        try:
            cluster = Cluster(hosts)
            session = cluster.connect()
            return cluster, session
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Cassandra. Last error: {last_exc}")


def main() -> None:
    cluster, session = connect_with_retry(["cassandra-server"])

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)

    session.execute("DROP TABLE IF EXISTS documents")
    session.execute("DROP TABLE IF EXISTS vocabulary")
    session.execute("DROP TABLE IF EXISTS postings")
    session.execute("DROP TABLE IF EXISTS corpus_stats")

    session.execute(
        """
        CREATE TABLE documents (
            doc_id text PRIMARY KEY,
            title text,
            doc_len int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE postings (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE corpus_stats (
            stat_key text PRIMARY KEY,
            stat_value double
        )
        """
    )

    insert_doc = session.prepare("INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)")
    insert_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    insert_posting = session.prepare("INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)")
    insert_stat = session.prepare("INSERT INTO corpus_stats (stat_key, stat_value) VALUES (?, ?)")

    for line in (BASE_DIR / "documents.tsv").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        doc_id, title_b64, doc_len = line.split("\t")
        session.execute(insert_doc, (doc_id, decode_title(title_b64), int(doc_len)))

    for line in (BASE_DIR / "vocabulary.tsv").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        term, df = line.split("\t")
        session.execute(insert_vocab, (term, int(df)))

    for line in (BASE_DIR / "postings.tsv").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        term, doc_id, tf = line.split("\t")
        session.execute(insert_posting, (term, doc_id, int(tf)))

    for line in (BASE_DIR / "stats.tsv").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        stat_key, stat_value = line.split("\t")
        session.execute(insert_stat, (stat_key, float(stat_value)))

    print("Index loaded into Cassandra successfully.")
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
