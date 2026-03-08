import json
from typing import List

from models import SimilarTicket, Ticket


def load_rag_corpus(path: str):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
    except FileNotFoundError:
        return []
    return rows


def retrieve_similar_tickets(ticket: Ticket, issue_family: str, config) -> List[SimilarTicket]:
    if not config.use_rag:
        return []

    corpus = load_rag_corpus(config.rag_index_path)

    matches = []
    for row in corpus:
        if row.get("issue_family") == issue_family:
            matches.append(SimilarTicket(
                ticket_id=row.get("ticket_id", ""),
                issue_family=row.get("issue_family", ""),
                summary=row.get("summary", ""),
                resolution_category=row.get("resolution_category", ""),
                score=1.0,
            ))

    return matches[:config.rag_top_k]
