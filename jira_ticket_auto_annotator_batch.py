
import json
import re
from pathlib import Path

import pandas as pd
import requests

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL_NAME = "qwen2.5:3B"

INPUT_XLSX = "formatted_back_test_results.xlsx"
OUTPUT_XLSX = "formatted_back_test_results_annotated.xlsx"

TICKET_TEXT_COL = "ticket_text"

BATCH_SIZE = 25
SAVE_EVERY_BATCH = 2

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
Tu es un assistant d’annotation de tickets Jira pour fund administration.

Pour chaque ticket fourni, tu dois produire un JSON d’annotation.

Champs à remplir :
scope_reel
request_type_reel
tool_reel
issue_type_reel
portfolio_reel
nav_date_reel
valeur_reel
resolution_category
is_ticket_exploitable

Répond uniquement avec un JSON de type LISTE.

Exemple de sortie :

[
{
"ticket_id":"1",
"scope_reel":"peres",
"request_type_reel":"analysis_request",
"tool_reel":"compute_pnl",
"issue_type_reel":"performance_issue",
"portfolio_reel":"PF001",
"nav_date_reel":"2026-03-05",
"valeur_reel":"",
"resolution_category":"performance_recalc",
"is_ticket_exploitable":true
}
]
"""

def extract_json(text):

    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```json\s*", "", text)
        text = re.sub(r"^```\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    match = re.search(r"\[.*\]", text, re.DOTALL)

    if not match:
        raise ValueError("JSON list introuvable")

    return json.loads(match.group(0))


def call_ollama_batch(batch):

    tickets_text = ""

    for i, row in batch:

        tickets_text += f"\nTICKET_ID:{i}\n"
        tickets_text += row + "\n"

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": tickets_text},
        ],
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 800,
        },
    }

    r = requests.post(OLLAMA_URL, json=payload, timeout=180)
    r.raise_for_status()

    data = r.json()
    content = data["message"]["content"]

    return extract_json(content)


def ensure_columns(df):

    for col in ANNOTATION_COLUMNS:

        if col not in df.columns:
            df[col] = ""

    for col in ["annotation_status", "annotation_error"]:

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
        df = pd.read_excel(output_path)
    else:
        df = pd.read_excel(input_path)

    df = ensure_columns(df)

    tasks = []

    for idx, row in df.iterrows():

        status = str(row.get("annotation_status", "")).lower()

        if status == "ok":
            continue

        text = str(row.get(TICKET_TEXT_COL, "")).strip()

        if not text:
            continue

        tasks.append((idx, text))

    print("Tickets à annoter:", len(tasks))

    batches = [
        tasks[i:i + BATCH_SIZE]
        for i in range(0, len(tasks), BATCH_SIZE)
    ]

    for batch_i, batch in enumerate(batches):

        print("Batch", batch_i + 1, "/", len(batches))

        try:

            result = call_ollama_batch(batch)

            for ann in result:

                idx = int(ann["ticket_id"])

                for col in ANNOTATION_COLUMNS:

                    df.at[idx, col] = ann.get(col, "")

                df.at[idx, "annotation_status"] = "ok"

        except Exception as e:

            print("Erreur batch:", e)

            for idx, _ in batch:

                df.at[idx, "annotation_status"] = "error"
                df.at[idx, "annotation_error"] = str(e)

        if batch_i % SAVE_EVERY_BATCH == 0:
            save(df)

    save(df)

    print("DONE")


if __name__ == "__main__":
    main()
