import json
from models import Ticket

try:
    import feedparser
except ImportError:
    feedparser = None


def load_jsonl_tickets(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row = json.loads(line)
                yield Ticket(
                    ticket_id=row.get("ticket_id") or row.get("key") or "UNKNOWN",
                    summary=row.get("summary", ""),
                    description=row.get("description", ""),
                    comments=row.get("comments", []) or [],
                    status=row.get("status", ""),
                    resolution=row.get("resolution", ""),
                )


def fetch_rss_tickets(rss_url: str):
    if feedparser is None:
        raise ImportError("feedparser est nécessaire pour le mode server. Installe-le avec pip install feedparser")

    feed = feedparser.parse(rss_url)
    tickets = []

    for entry in feed.entries:
        ticket_id = getattr(entry, "id", None) or getattr(entry, "title", "UNKNOWN")
        title = getattr(entry, "title", "")
        description = getattr(entry, "summary", "") or getattr(entry, "description", "")

        tickets.append(Ticket(
            ticket_id=ticket_id,
            summary=title,
            description=description,
            comments=[],
            status="",
            resolution="",
        ))

    return tickets
