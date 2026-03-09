
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
SAVE_EVERY = 20
MAX_ROWS = None

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

Champs à remplir :
- scope_reel
- request_type_reel
- tool_reel
- issue_type_reel
- portfolio_reel
- nav_date_reel
- valeur_reel
- resolution_category
- is_ticket_exploitable

Valeurs autorisées :

1. scope_reel
- peres
- ucits
- institutional
- unknown

2. request_type_reel
- analysis_request
- action_request
- information_request
- incomplete_ticket
- unknown

3. tool_reel
- extract_nav
- compute_pnl
- check_valuation_issue
- check_oid_issue
- check_rappro_break
- check_pricing_issue
- check_positions_issue
- check_cash_flow_issue
- no_tool
- unknown

4. issue_type_reel
- valuation_gap
- performance_issue
- oid_issue
- rappro_break
- pricing_issue
- positions_issue
- cash_flow_issue
- action_request
- information_request
- unknown

5. portfolio_reel
- code portefeuille exact si présent ou fortement inférable
- sinon ""

6. nav_date_reel
- format obligatoire YYYY-MM-DD
- sinon ""

7. valeur_reel
- montant ou valeur utile si présent
- sinon ""

8. resolution_category
- positions_issue
- performance_recalc
- cash_flow_issue
- pricing_issue
- missing_information
- action_done
- unknown

9. is_ticket_exploitable
- true
- false

Règles :
- Réponds uniquement avec un JSON valide
- N’ajoute aucun texte avant ou après
- N’invente pas un portefeuille ou une date sans base suffisante
- Si la demande porte sur un calcul, une vérification ou une analyse, request_type_reel = analysis_request
- Si la demande porte sur une action directe comme forcer la NAV, request_type_reel = action_request
- Si les informations sont trop pauvres pour une analyse correcte, request_type_reel = incomplete_ticket
- Si aucun check métier n’est justifié, tool_reel = no_tool

Format strict :
{
  "scope_reel": "",
  "request_type_reel": "",
  "tool_reel": "",
  "issue_type_reel": "",
  "portfolio_reel": "",
  "nav_date_reel": "",
  "valeur_reel": "",
  "resolution_category": "",
  "is_ticket_exploitable": true
}
""".strip()


def call_ollama(ticket_text: str) -> tuple[dict, str]:
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
            "top_p": 0.9,
            "num_predict": 300,
        },
    }

    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()

    raw_content = data["message"]["content"]
    parsed = extract_json(raw_content)
    return parsed, raw_content


def extract_json(text: str) -> dict:
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```json\\s*", "", text)
        text = re.sub(r"^```\\s*", "", text)
        text = re.sub(r"\\s*```$", "", text)

    match = re.search(r"\\{.*\\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"JSON introuvable dans la réponse : {text}")

    return json.loads(match.group(0))


def normalize_date(value: str) -> str:
    if value is None:
        return ""

    value = str(value).strip()
    if not value:
        return ""

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


def validate_annotation(d: dict) -> dict:
    allowed_scope = {"peres", "ucits", "institutional", "unknown"}
    allowed_request = {
        "analysis_request",
        "action_request",
        "information_request",
        "incomplete_ticket",
        "unknown",
    }
    allowed_tool = {
        "extract_nav",
        "compute_pnl",
        "check_valuation_issue",
        "check_oid_issue",
        "check_rappro_break",
        "check_pricing_issue",
        "check_positions_issue",
        "check_cash_flow_issue",
        "no_tool",
        "unknown",
    }
    allowed_issue = {
        "valuation_gap",
        "performance_issue",
        "oid_issue",
        "rappro_break",
        "pricing_issue",
        "positions_issue",
        "cash_flow_issue",
        "action_request",
        "information_request",
        "unknown",
    }
    allowed_resolution = {
        "positions_issue",
        "performance_recalc",
        "cash_flow_issue",
        "pricing_issue",
        "missing_information",
        "action_done",
        "unknown",
    }

    out = {
        "scope_reel": d.get("scope_reel", "unknown"),
        "request_type_reel": d.get("request_type_reel", "unknown"),
        "tool_reel": d.get("tool_reel", "unknown"),
        "issue_type_reel": d.get("issue_type_reel", "unknown"),
        "portfolio_reel": d.get("portfolio_reel", "") or "",
        "nav_date_reel": normalize_date(d.get("nav_date_reel", "") or ""),
        "valeur_reel": d.get("valeur_reel", "") or "",
        "resolution_category": d.get("resolution_category", "unknown"),
        "is_ticket_exploitable": bool(d.get("is_ticket_exploitable", True)),
    }

    if out["scope_reel"] not in allowed_scope:
        out["scope_reel"] = "unknown"

    if out["request_type_reel"] not in allowed_request:
        out["request_type_reel"] = "unknown"

    if out["tool_reel"] not in allowed_tool:
        out["tool_reel"] = "unknown"

    if out["issue_type_reel"] not in allowed_issue:
        out["issue_type_reel"] = "unknown"

    if out["resolution_category"] not in allowed_resolution:
        out["resolution_category"] = "unknown"

    return out


def should_annotate_row(row: pd.Series) -> bool:
    text = str(row.get(TICKET_TEXT_COL, "") or "").strip()
    if not text:
        return False

    status = str(row.get("annotation_status", "") or "").strip().lower()
    if status == "ok":
        return False

    empty_count = 0
    for col in ANNOTATION_COLUMNS:
        val = row.get(col, None)
        if pd.isna(val) or str(val).strip() == "":
            empty_count += 1

    return empty_count > 0


def save_checkpoint(df: pd.DataFrame, output_path: Path):
    df.to_excel(output_path, index=False)
    print(f"[SAVE] {output_path}")


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ANNOTATION_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    extra_cols = [
        "annotation_status",
        "annotation_raw_json",
        "annotation_error",
    ]

    for col in extra_cols:
        if col not in df.columns:
            df[col] = ""

    return df


def main():
    input_path = Path(INPUT_XLSX)
    output_path = Path(OUTPUT_XLSX)

    if output_path.exists():
        print(f"[INFO] reprise depuis {output_path}")
        df = pd.read_excel(output_path)
    else:
        print(f"[INFO] lecture depuis {input_path}")
        df = pd.read_excel(input_path)

    df = ensure_columns(df)

    if TICKET_TEXT_COL not in df.columns:
        raise ValueError(f"Colonne absente : {TICKET_TEXT_COL}")

    rows_to_process = df.index.tolist()
    if MAX_ROWS is not None:
        rows_to_process = rows_to_process[:MAX_ROWS]

    processed_since_save = 0
    total = len(rows_to_process)

    for pos, idx in enumerate(rows_to_process, start=1):
        row = df.loc[idx]

        if not should_annotate_row(row):
            continue

        ticket_text = str(row[TICKET_TEXT_COL])

        try:
            raw_annotation, raw_json = call_ollama(ticket_text)
            annotation = validate_annotation(raw_annotation)

            for col, value in annotation.items():
                current = df.at[idx, col]
                if pd.isna(current) or str(current).strip() == "":
                    df.at[idx, col] = value

            df.at[idx, "annotation_status"] = "ok"
            df.at[idx, "annotation_raw_json"] = json.dumps(annotation, ensure_ascii=False)
            df.at[idx, "annotation_error"] = ""

            processed_since_save += 1
            print(f"[OK] ligne excel={idx + 2} progression={pos}/{total}")

        except Exception as e:
            df.at[idx, "annotation_status"] = "error"
            df.at[idx, "annotation_error"] = str(e)
            print(f"[ERREUR] ligne excel={idx + 2} progression={pos}/{total} : {e}")

        if processed_since_save >= SAVE_EVERY:
            save_checkpoint(df, output_path)
            processed_since_save = 0

    save_checkpoint(df, output_path)
    print("[DONE]")


if __name__ == "__main__":
    main()
