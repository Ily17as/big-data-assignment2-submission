#!/usr/bin/env python3
import sys

current_term = None
postings = []
doc_count = 0
total_doc_len = 0


def flush_term(term, postings_list):

    if term is None or not postings_list:
        return
    
    df = len(postings_list)

    print(f"VOCAB\t{term}\t{df}")

    for doc_id, tf in postings_list:
        print(f"POSTING\t{term}\t{doc_id}\t{tf}")

for raw_line in sys.stdin:

    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    record_type = parts[0]

    if record_type == "DOC":
        if len(parts) != 4:
            continue
        
        doc_id, title_b64, doc_len = parts[1], parts[2], int(parts[3])
        doc_count += 1
        total_doc_len += doc_len

        print(f"DOC\t{doc_id}\t{title_b64}\t{doc_len}")

        continue

    if record_type != "TERM" or len(parts) != 4:
        continue

    term, doc_id, tf = parts[1], parts[2], int(parts[3])
    
    if current_term is None:
        current_term = term

    if term != current_term:
        flush_term(current_term, postings)
        current_term = term
        postings = []

    postings.append((doc_id, tf))

flush_term(current_term, postings)

avgdl = (total_doc_len / doc_count) if doc_count else 0.0
print(f"STAT\tN\t{doc_count}")
print(f"STAT\tAVGDL\t{avgdl}")
