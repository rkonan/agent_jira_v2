import json
import re
from pathlib import Path

import pandas as pd
import requests


OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL_NAME = "qwen2.5:3B"

INPUT_XLSX = "formatted_back_test_results.xlsx"
OUTPUT_XLSX = "formatted_back_test_results_regex_llm.xlsx"

TICKET_TEXT_COL = "ticket_text"

PORTFOLIO_REF_FILE = "portfolio_reference_realistic.csv"

SAVE_EVERY = 20
MAX_ROWS = None


LLM_FIELDS = [
    "request_type_reel",
    "issue_type_reel",
    "tool_reel",
    "resolution_category",
    "is_ticket_exploitable",
]


SYSTEM_PROMPT = """
Tu annotes un ticket Jira de fund administration.

Les champs suivants sont déjà extraits par règles :
- portfolio_reel
- nav_date_reel
- valeur_reel
- scope_reel

Tu dois remplir uniquement :
- request_type_reel
- issue_type_reel
- tool_reel
- resolution_category
- is_ticket_exploitable

Valeurs autorisées :

request_type_reel:
- analysis_request
- action_request
- information_request
- incomplete_ticket
- unknown

issue_type_reel:
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

tool_reel:
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

resolution_category:
- positions_issue
- performance_recalc
- cash_flow_issue
- pricing_issue
- missing_information
- action_done
- unknown

is_ticket_exploitable:
- true
- false

Règles :
- Réponds uniquement avec un JSON valide
- Si la demande porte sur une analyse, un calcul ou une vérification, request_type_reel = analysis_request
- Si la demande porte sur une action directe comme forcer la NAV, request_type_reel = action_request
- Si les informations sont trop pauvres, request_type_reel = incomplete_ticket
- Si aucun check métier n’est justifié, tool_reel = no_tool

Format de sortie :
{
  "request_type_reel": "",
  "issue_type_reel": "",
  "tool_reel": "",
  "resolution_category": "",
  "is_ticket_exploitable": true
}
""".strip()


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "portfolio_reel",
        "nav_date_reel",
        "valeur_reel",
        "scope_reel",
        "request_type_reel",
        "issue_type_reel",
        "tool_reel",
        "resolution_category",
        "is_ticket_exploitable",
        "annotation_status",
        "annotation_error",
        "annotation_raw_json",
    ]
    for col in needed:
        if col not in df.columns:
            df[col] = ""
    return df


def normalize_date(value: str) -> str:
    if value is None:
        return ""
    value = str(value).strip()
    if not value:
        return ""

    patterns = [
        (r"^(\d{4})-(\d{2})-(\d{2})$", lambda g: f"{g[0]}-{g[1]}-{g[2]}"),
        (r"^(\d{2})/(\d{2})/(\d{4})$", lambda g: f"{g[2]}-{g[1]}-{g[0]}"),
        (r"^(\d{2})\.(\d{2})\.(\d{4})$", lambda g: f"{g[2]}-{g[1]}-{g[0]}"),
        (r"^(\d{2})\.(\d{2})\.(\d{2})$", lambda g: f"20{g[2]}-{g[1]}-{g[0]}"),
        (r"^(\d{2})/(\d{2})/(\d{2})$", lambda g: f"20{g[2]}-{g[1]}-{g[0]}"),
    ]

    for pattern, formatter in patterns:
        m = re.match(pattern, value)
        if m:
            return formatter(m.groups())

    return ""


def extract_portfolio(text: str) -> str:
    if not text:
        return ""

    patterns = [
        r"\b\d{6}\b",
        r"\bPF\d{2,}\b",
        r"\bUC\d{2,}\b",
        r"\bINS\d{2,}\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return m.group(0)

    return ""


def extract_nav_date(text: str) -> str:
    if not text:
        return ""

    patterns = [
        (r"\b(\d{4})-(\d{2})-(\d{2})\b", lambda g: f"{g[0]}-{g[1]}-{g[2]}"),
        (r"\b(\d{2})/(\d{2})/(\d{4})\b", lambda g: f"{g[2]}-{g[1]}-{g[0]}"),
        (r"\b(\d{2})\.(\d{2})\.(\d{4})\b", lambda g: f"{g[2]}-{g[1]}-{g[0]}"),
        (r"\b(\d{2})\.(\d{2})\.(\d{2})\b", lambda g: f"20{g[2]}-{g[1]}-{g[0]}"),
        (r"\b(\d{2})/(\d{2})/(\d{2})\b", lambda g: f"20{g[2]}-{g[1]}-{g[0]}"),
    ]

    for pattern, formatter in patterns:
        m = re.search(pattern, text)
        if m:
            return formatter(m.groups())

    return ""


def extract_amount(text: str) -> str:
    if not text:
        return ""

    patterns = [
        r"\b\d[\d\s.,]*\s?(?:EUR|USD|GBP)\b",
        r"\b\d[\d\s.,]*\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            value = m.group(0).strip()
            if len(value) >= 4:
                return value

    return ""


def load_portfolio_reference(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["portfolio_code", "fund_name", "aliases", "scope"])
    return pd.read_csv(p)


def infer_scope_from_reference(portfolio_value: str, ticket_text: str, ref_df: pd.DataFrame) -> str:
    if ref_df.empty:
        return "unknown"

    if portfolio_value:
        match = ref_df[
            ref_df["portfolio_code"].astype(str).str.lower() == str(portfolio_value).lower()
        ]
        if not match.empty:
            return str(match.iloc[0]["scope"]).strip().lower()

    text_lower = str(ticket_text).lower()
    for _, row in ref_df.iterrows():
        fund_name = str(row.get("fund_name", "") or "").strip().lower()
        aliases = str(row.get("aliases", "") or "").strip().lower().split("|")

        if fund_name and fund_name in text_lower:
            return str(row.get("scope", "unknown")).strip().lower()

        for alias in aliases:
            alias = alias.strip()
            if alias and alias in text_lower:
                return str(row.get("scope", "unknown")).strip().lower()

    return "unknown"


def extract_json_object(text: str) -> dict:
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```json\s*", "", text)
        text = re.sub(r"^```\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"JSON introuvable dans la réponse : {text}")

    return json.loads(match.group(0))


def validate_llm_output(d: dict) -> dict:
    allowed_request = {
        "analysis_request", "action_request", "information_request", "incomplete_ticket", "unknown"
    }
    allowed_issue = {
        "valuation_gap", "performance_issue", "oid_issue", "rappro_break",
        "pricing_issue", "positions_issue", "cash_flow_issue",
        "action_request", "information_request", "unknown"
    }
    allowed_tool = {
        "extract_nav", "compute_pnl", "check_valuation_issue", "check_oid_issue",
        "check_rappro_break", "check_pricing_issue", "check_positions_issue",
        "check_cash_flow_issue", "no_tool", "unknown"
    }
    allowed_resolution = {
        "positions_issue", "performance_recalc", "cash_flow_issue",
        "pricing_issue", "missing_information", "action_done", "unknown"
    }

    out = {
        "request_type_reel": str(d.get("request_type_reel", "unknown")).strip(),
        "issue_type_reel": str(d.get("issue_type_reel", "unknown")).strip(),
        "tool_reel": str(d.get("tool_reel", "unknown")).strip(),
        "resolution_category": str(d.get("resolution_category", "unknown")).strip(),
        "is_ticket_exploitable": bool(d.get("is_ticket_exploitable", True)),
    }

    if out["request_type_reel"] not in allowed_request:
        out["request_type_reel"] = "unknown"
    if out["issue_type_reel"] not in allowed_issue:
        out["issue_type_reel"] = "unknown"
    if out["tool_reel"] not in allowed_tool:
        out["tool_reel"] = "unknown"
    if out["resolution_category"] not in allowed_resolution:
        out["resolution_category"] = "unknown"

    return out


def call_ollama(ticket_text: str, extracted_fields: dict) -> tuple[dict, str]:
    user_content = (
        f"TICKET_TEXT:\n{ticket_text}\n\n"
        f"CHAMPS_DEJA_EXTRAITS:\n"
        f"- portfolio_reel: {extracted_fields.get('portfolio_reel', '')}\n"
        f"- nav_date_reel: {extracted_fields.get('nav_date_reel', '')}\n"
        f"- valeur_reel: {extracted_fields.get('valeur_reel', '')}\n"
        f"- scope_reel: {extracted_fields.get('scope_reel', '')}\n"
    )

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0,
            "top_p": 0.9,
            "num_predict": 220,
        },
    }

    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()

    raw_content = data["message"]["content"]
    parsed = extract_json_object(raw_content)
    return validate_llm_output(parsed), raw_content


def save_checkpoint(df: pd.DataFrame, output_path: Path):
    df.to_excel(output_path, index=False)
    print(f"[SAVE] {output_path}")


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

    ref_df = load_portfolio_reference(PORTFOLIO_REF_FILE)

    rows = df.index.tolist()
    if MAX_ROWS is not None:
        rows = rows[:MAX_ROWS]

    processed_since_save = 0

    for idx in rows:
        ticket_text = str(df.at[idx, TICKET_TEXT_COL] or "").strip()
        if not ticket_text:
            continue

        if str(df.at[idx, "annotation_status"]).strip().lower() == "ok":
            continue

        try:
            portfolio_val = extract_portfolio(ticket_text)
            nav_date_val = extract_nav_date(ticket_text)
            amount_val = extract_amount(ticket_text)
            scope_val = infer_scope_from_reference(portfolio_val, ticket_text, ref_df)

            if is_empty(df.at[idx, "portfolio_reel"]):
                df.at[idx, "portfolio_reel"] = portfolio_val
            if is_empty(df.at[idx, "nav_date_reel"]):
                df.at[idx, "nav_date_reel"] = nav_date_val
            if is_empty(df.at[idx, "valeur_reel"]):
                df.at[idx, "valeur_reel"] = amount_val
            if is_empty(df.at[idx, "scope_reel"]):
                df.at[idx, "scope_reel"] = scope_val

            extracted_fields = {
                "portfolio_reel": df.at[idx, "portfolio_reel"],
                "nav_date_reel": df.at[idx, "nav_date_reel"],
                "valeur_reel": df.at[idx, "valeur_reel"],
                "scope_reel": df.at[idx, "scope_reel"],
            }

            llm_output, raw_json = call_ollama(ticket_text, extracted_fields)

            for col, value in llm_output.items():
                if is_empty(df.at[idx, col]):
                    df.at[idx, col] = value

            df.at[idx, "annotation_status"] = "ok"
            df.at[idx, "annotation_raw_json"] = raw_json
            df.at[idx, "annotation_error"] = ""

            processed_since_save += 1
            print(f"[OK] ligne {idx + 2}")

        except Exception as e:
            df.at[idx, "annotation_status"] = "error"
            df.at[idx, "annotation_error"] = str(e)
            print(f"[ERREUR] ligne {idx + 2} : {e}")

        if processed_since_save >= SAVE_EVERY:
            save_checkpoint(df, output_path)
            processed_since_save = 0

    save_checkpoint(df, output_path)
    print("[DONE]")


if __name__ == "__main__":
    main()
