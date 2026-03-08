import json
from pathlib import Path


def save_jsonl(path: str, rows, append: bool = False):
    mode = "a" if append else "w"
    with open(path, mode, encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_processed_ids(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    data = json.loads(p.read_text(encoding="utf-8"))
    return set(data.get("processed_ids", []))


def save_processed_ids(path: str, ids: set[str]) -> None:
    Path(path).write_text(
        json.dumps({"processed_ids": sorted(ids)}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
