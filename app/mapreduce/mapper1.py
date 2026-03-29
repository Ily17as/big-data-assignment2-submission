#!/usr/bin/env python3
import base64
import re
import sys
from collections import Counter

TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


for raw_line in sys.stdin:

    line = raw_line.rstrip("\n")

    if not line:
        continue

    parts = line.split("\t", 2)

    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    tokens = tokenize(text)

    if not tokens:
        continue

    title_b64 = base64.b64encode(title.encode("utf-8")).decode("ascii")

    print(f"DOC\t{doc_id}\t{title_b64}\t{len(tokens)}")

    tf_counter = Counter(tokens)
    
    for term, tf in tf_counter.items():
        print(f"TERM\t{term}\t{doc_id}\t{tf}")
