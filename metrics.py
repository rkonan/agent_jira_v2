import json
from collections import Counter


def compute_backtest_metrics(result_rows):
    total = len(result_rows)
    status_counter = Counter(row.get("status", "unknown") for row in result_rows)
    issue_counter = Counter(row.get("issue_type", row.get("issue_family", "unknown")) for row in result_rows)

    elapsed_values = [
        row.get("elapsed_seconds", 0.0)
        for row in result_rows
        if isinstance(row.get("elapsed_seconds", None), (int, float))
    ]
    avg_elapsed = sum(elapsed_values) / len(elapsed_values) if elapsed_values else 0.0

    return {
        "model": result_rows[0].get("model", "unknown") if result_rows else "unknown",
        "total_tickets": total,
        "status_counts": dict(status_counter),
        "issue_counts": dict(issue_counter),
        "average_elapsed_seconds": round(avg_elapsed, 3),
    }


def save_metrics(path: str, metrics: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
