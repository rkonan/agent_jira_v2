
import json
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL_NAME = "qwen2.5:3B"

INPUT_XLSX = "formatted_back_test_results.xlsx"
OUTPUT_XLSX = "formatted_back_test_results_annotated.xlsx"

TICKET_TEXT_COL = "ticket_text"

SAVE_EVERY = 20
MAX_ROWS = None
MAX_WORKERS = 10

ANNOTATION_COLUMNS = [
    "scope_reel",
    "request_type_reel",
    "tool_reel",
    "issue_type_reel",
    "portfolio_reel",
    "nav_date_reel",
    "valeur_reel",
    "resolution_category",
    "is_ticket_exploitable",
]

SYSTEM_PROMPT = """
Tu es un assistant d’annotation de tickets Jira orientés fund administration.

Tu dois lire le texte du ticket et remplir les champs suivants.

Champs :
scope_reel, request_type_reel, tool_reel, issue_type_reel,
portfolio_reel, nav_date_reel, valeur_reel,
resolution_category, is_ticket_exploitable

Répond uniquement en JSON strict.
"""

def extract_json(text):
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```json\\s*", "", text)
        text = re.sub(r"^```\\s*", "", text)
        text = re.sub(r"\\s*```$", "", text)

    match = re.search(r"\\{.*\\}", text, re.DOTALL)
    if not match:
        raise ValueError("JSON introuvable")

    return json.loads(match.group(0))


def call_ollama(ticket_text):

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": ticket_text},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0,
            "num_predict": 200,
        },
    }

    r = requests.post(OLLAMA_URL, json=payload, timeout=120)
    r.raise_for_status()

    data = r.json()
    raw = data["message"]["content"]

    parsed = extract_json(raw)

    return parsed, raw


def normalize_date(value):

    if not value:
        return ""

    value = str(value).strip()

    patterns = [
        (r"^(\\d{4})-(\\d{2})-(\\d{2})$", "{0}-{1}-{2}"),
        (r"^(\\d{2})/(\\d{2})/(\\d{4})$", "{2}-{1}-{0}"),
        (r"^(\\d{2})\\.(\\d{2})\\.(\\d{4})$", "{2}-{1}-{0}"),
    ]

    for pattern, fmt in patterns:

        m = re.match(pattern, value)

        if m:
            return fmt.format(*m.groups())

    return value


def validate(d):

    out = {
        "scope_reel": d.get("scope_reel", "unknown"),
        "request_type_reel": d.get("request_type_reel", "unknown"),
        "tool_reel": d.get("tool_reel", "unknown"),
        "issue_type_reel": d.get("issue_type_reel", "unknown"),
        "portfolio_reel": d.get("portfolio_reel", ""),
        "nav_date_reel": normalize_date(d.get("nav_date_reel", "")),
        "valeur_reel": d.get("valeur_reel", ""),
        "resolution_category": d.get("resolution_category", "unknown"),
        "is_ticket_exploitable": bool(d.get("is_ticket_exploitable", True)),
    }

    return out


def annotate_one(idx, ticket_text):

    try:

        raw_annotation, raw_json = call_ollama(ticket_text)

        annotation = validate(raw_annotation)

        return idx, annotation, raw_json, None

    except Exception as e:

        return idx, None, None, str(e)


def ensure_columns(df):

    for col in ANNOTATION_COLUMNS:

        if col not in df.columns:
            df[col] = ""

    extra = [
        "annotation_status",
        "annotation_raw_json",
        "annotation_error",
    ]

    for col in extra:

        if col not in df.columns:
            df[col] = ""

    return df


def save(df):

    df.to_excel(OUTPUT_XLSX, index=False)
    print("SAVE ->", OUTPUT_XLSX)


def main():

    input_path = Path(INPUT_XLSX)
    output_path = Path(OUTPUT_XLSX)

    if output_path.exists():
        print("Reprise depuis", OUTPUT_XLSX)
        df = pd.read_excel(output_path)
    else:
        df = pd.read_excel(input_path)

    df = ensure_columns(df)

    rows = df.index.tolist()

    if MAX_ROWS:
        rows = rows[:MAX_ROWS]

    tasks = []

    for idx in rows:

        row = df.loc[idx]

        status = str(row.get("annotation_status", "")).lower()

        if status == "ok":
            continue

        ticket_text = str(row.get(TICKET_TEXT_COL, "")).strip()

        if not ticket_text:
            continue

        tasks.append((idx, ticket_text))

    print("Tickets à annoter:", len(tasks))

    processed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(annotate_one, idx, text)
            for idx, text in tasks
        ]

        for future in as_completed(futures):

            idx, annotation, raw_json, error = future.result()

            if error:

                df.at[idx, "annotation_status"] = "error"
                df.at[idx, "annotation_error"] = error

            else:

                for col, val in annotation.items():

                    current = df.at[idx, col]

                    if pd.isna(current) or str(current).strip() == "":
                        df.at[idx, col] = val

                df.at[idx, "annotation_status"] = "ok"
                df.at[idx, "annotation_raw_json"] = json.dumps(annotation, ensure_ascii=False)

            processed += 1

            if processed % SAVE_EVERY == 0:
                save(df)

    save(df)

    print("DONE")


if __name__ == "__main__":
    main()
