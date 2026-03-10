
"""
Annotation hybride de tickets Jira :
1. embeddings + LogisticRegression pour prédiction rapide
2. fallback LLM pour les lignes / champs à faible confiance
3. sauvegarde d'un Excel enrichi

Installation :
pip install pandas openpyxl scikit-learn sentence-transformers requests

Usage :
python jira_hybrid_auto_annotator.py
"""

from pathlib import Path
from typing import Dict, List, Tuple
import json
import re

import numpy as np
import pandas as pd
import requests
from sentence_transformers import SentenceTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report


# =========================
# CONFIG
# =========================
INPUT_XLSX = "formatted_back_test_results.xlsx"
OUTPUT_XLSX = "formatted_back_test_results_hybrid_annotated.xlsx"

TEXT_COL = "ticket_text"

TARGET_COLUMNS = [
    "scope_reel",
    "request_type_reel",
    "issue_type_reel",
    "tool_reel",
    "resolution_category",
    "is_ticket_exploitable",
]

EXTRACT_COLUMNS = [
    "portfolio_reel",
    "nav_date_reel",
]

EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

ML_CONFIDENCE_THRESHOLD = 0.70
LLM_FALLBACK_THRESHOLD = 0.70

USE_LLM_FALLBACK = True
OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL = "qwen2.5:3B"

SAVE_CONFIDENCE_COLUMNS = True


# =========================
# PROMPT LLM
# =========================
LLM_SYSTEM_PROMPT = """
Tu es un assistant d’annotation de tickets Jira orientés fund administration.

Tu dois remplir uniquement les champs demandés. Réponds uniquement avec un JSON valide.

Valeurs autorisées :

scope_reel:
- peres
- ucits
- institutional
- unknown

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

portfolio_reel:
- code portefeuille exact si présent ou fortement inférable
- sinon ""

nav_date_reel:
- format YYYY-MM-DD
- sinon ""

Règles :
- Si la demande porte sur un calcul, une vérification ou une analyse, alors request_type_reel = analysis_request
- Si la demande porte sur une action directe comme forcer la NAV, alors request_type_reel = action_request
- Si les informations sont trop pauvres pour une analyse correcte, alors request_type_reel = incomplete_ticket
- Si aucun check métier n’est justifié, tool_reel = no_tool
- N’invente pas un portefeuille ou une date sans base suffisante

Format de sortie :
{
  "scope_reel": "",
  "request_type_reel": "",
  "issue_type_reel": "",
  "tool_reel": "",
  "resolution_category": "",
  "is_ticket_exploitable": true,
  "portfolio_reel": "",
  "nav_date_reel": ""
}
""".strip()


# =========================
# HELPERS
# =========================
def first_existing(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def ensure_ticket_text(df: pd.DataFrame) -> pd.DataFrame:
    if TEXT_COL in df.columns and df[TEXT_COL].fillna("").astype(str).str.strip().ne("").any():
        return df

    summary_col = first_existing(df, ["summary", "Summary"])
    description_col = first_existing(df, ["description", "Description"])
    comments_col = first_existing(df, ["comments", "Comments"])

    if summary_col is None and description_col is None:
        raise ValueError("Impossible de construire ticket_text : colonnes summary/description absentes.")

    summary = df[summary_col].fillna("").astype(str) if summary_col else ""
    description = df[description_col].fillna("").astype(str) if description_col else ""
    comments = df[comments_col].fillna("").astype(str) if comments_col else ""

    df[TEXT_COL] = (
        "SUMMARY:\n" + summary +
        "\n\nDESCRIPTION:\n" + description +
        "\n\nCOMMENTS:\n" + comments
    ).str.strip()

    return df


def is_empty(v) -> bool:
    if pd.isna(v):
        return True
    return str(v).strip() == ""


def normalize_binary_column(series: pd.Series) -> pd.Series:
    def norm(v):
        if pd.isna(v):
            return np.nan
        s = str(v).strip().lower()
        if s in {"true", "1", "yes", "oui"}:
            return "true"
        if s in {"false", "0", "no", "non"}:
            return "false"
        return np.nan if s == "" else s
    return series.apply(norm)


def extract_portfolio(text: str) -> str:
    if not text:
        return ""
    patterns = [
        r"\b[A-Z]{2,4}\d{2,}\b",
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
    ]

    for pattern, formatter in patterns:
        m = re.search(pattern, text)
        if m:
            return formatter(m.groups())

    return ""


def prepare_targets(df: pd.DataFrame) -> pd.DataFrame:
    for col in TARGET_COLUMNS + EXTRACT_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    if "is_ticket_exploitable" in df.columns:
        df["is_ticket_exploitable"] = normalize_binary_column(df["is_ticket_exploitable"])

    return df


def evaluate_classifier(X: np.ndarray, y: pd.Series, field_name: str) -> None:
    if len(y) < 20 or y.nunique() < 2:
        print(f"[{field_name}] Pas assez de données pour une vraie évaluation.")
        return

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42, stratify=y
        )
        clf = LogisticRegression(max_iter=2000, class_weight="balanced", random_state=42)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        print(f"\n=== Évaluation {field_name} ===")
        print(classification_report(y_test, y_pred, zero_division=0))
    except Exception as e:
        print(f"[{field_name}] Évaluation ignorée : {e}")


def fit_one_classifier(X: np.ndarray, y: pd.Series) -> LogisticRegression:
    clf = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        random_state=42,
    )
    clf.fit(X, y)
    return clf


# =========================
# LLM FALLBACK
# =========================
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


def normalize_date(value: str) -> str:
    if value is None:
        return ""
    value = str(value).strip()
    if not value:
        return ""

    patterns = [
        (r"^(\d{4})-(\d{2})-(\d{2})$", "{0}-{1}-{2}"),
        (r"^(\d{2})/(\d{2})/(\d{4})$", "{2}-{1}-{0}"),
        (r"^(\d{2})\.(\d{2})\.(\d{4})$", "{2}-{1}-{0}"),
    ]

    for pattern, fmt in patterns:
        m = re.match(pattern, value)
        if m:
            return fmt.format(*m.groups())

    return value


def validate_llm_annotation(d: dict) -> dict:
    allowed_scope = {"peres", "ucits", "institutional", "unknown"}
    allowed_request = {
        "analysis_request", "action_request", "information_request", "incomplete_ticket", "unknown"
    }
    allowed_tool = {
        "extract_nav", "compute_pnl", "check_valuation_issue", "check_oid_issue",
        "check_rappro_break", "check_pricing_issue", "check_positions_issue",
        "check_cash_flow_issue", "no_tool", "unknown",
    }
    allowed_issue = {
        "valuation_gap", "performance_issue", "oid_issue", "rappro_break",
        "pricing_issue", "positions_issue", "cash_flow_issue",
        "action_request", "information_request", "unknown",
    }
    allowed_resolution = {
        "positions_issue", "performance_recalc", "cash_flow_issue",
        "pricing_issue", "missing_information", "action_done", "unknown",
    }

    out = {
        "scope_reel": d.get("scope_reel", "unknown"),
        "request_type_reel": d.get("request_type_reel", "unknown"),
        "issue_type_reel": d.get("issue_type_reel", "unknown"),
        "tool_reel": d.get("tool_reel", "unknown"),
        "resolution_category": d.get("resolution_category", "unknown"),
        "is_ticket_exploitable": "true" if bool(d.get("is_ticket_exploitable", True)) else "false",
        "portfolio_reel": d.get("portfolio_reel", "") or "",
        "nav_date_reel": normalize_date(d.get("nav_date_reel", "") or ""),
    }

    if out["scope_reel"] not in allowed_scope:
        out["scope_reel"] = "unknown"
    if out["request_type_reel"] not in allowed_request:
        out["request_type_reel"] = "unknown"
    if out["issue_type_reel"] not in allowed_issue:
        out["issue_type_reel"] = "unknown"
    if out["tool_reel"] not in allowed_tool:
        out["tool_reel"] = "unknown"
    if out["resolution_category"] not in allowed_resolution:
        out["resolution_category"] = "unknown"

    return out


def call_ollama_fallback(ticket_text: str) -> tuple[dict, str]:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": ticket_text},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0,
            "top_p": 0.9,
            "num_predict": 250,
        },
    }

    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()

    raw_content = data["message"]["content"]
    parsed = extract_json_object(raw_content)
    return validate_llm_annotation(parsed), raw_content


# =========================
# ML ANNOTATION
# =========================
def annotate_field_ml(
    df: pd.DataFrame,
    embeddings: np.ndarray,
    field_name: str,
    confidence_threshold: float,
) -> tuple[pd.DataFrame, pd.Series]:
    train_mask = df[field_name].notna() & df[field_name].astype(str).str.strip().ne("")
    pred_mask = ~train_mask

    n_train = int(train_mask.sum())
    n_pred = int(pred_mask.sum())

    print(f"\n=== Champ {field_name} ===")
    print(f"Lignes annotées manuellement : {n_train}")
    print(f"Lignes à prédire : {n_pred}")

    conf_series = pd.Series(index=df.index, dtype=float)

    if n_train < 10:
        print(f"[{field_name}] Pas assez de lignes annotées. Champ ignoré.")
        return df, conf_series

    y_train = df.loc[train_mask, field_name].astype(str).str.strip()

    if y_train.nunique() < 2:
        print(f"[{field_name}] Une seule classe connue. Champ ignoré.")
        return df, conf_series

    X_train = embeddings[train_mask.values]
    X_pred = embeddings[pred_mask.values]

    evaluate_classifier(X_train, y_train, field_name)

    clf = fit_one_classifier(X_train, y_train)

    if n_pred == 0:
        print(f"[{field_name}] Rien à prédire.")
        return df, conf_series

    pred_labels = clf.predict(X_pred)
    pred_probas = clf.predict_proba(X_pred).max(axis=1)

    pred_indices = df.index[pred_mask]

    pred_col = f"{field_name}_pred"
    conf_col = f"{field_name}_confidence"

    if pred_col not in df.columns:
        df[pred_col] = np.nan
    if conf_col not in df.columns:
        df[conf_col] = np.nan

    for idx, label, proba in zip(pred_indices, pred_labels, pred_probas):
        df.at[idx, pred_col] = label
        df.at[idx, conf_col] = float(proba)
        conf_series.at[idx] = float(proba)

        if proba >= confidence_threshold and is_empty(df.at[idx, field_name]):
            df.at[idx, field_name] = label

    accepted = int((df.loc[pred_indices, conf_col] >= confidence_threshold).sum())
    print(f"[{field_name}] Prédictions acceptées : {accepted}/{n_pred}")

    return df, conf_series


# =========================
# MAIN
# =========================
def main():
    input_path = Path(INPUT_XLSX)
    output_path = Path(OUTPUT_XLSX)

    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {input_path}")

    df = pd.read_excel(input_path)
    print("Shape initiale :", df.shape)

    df = ensure_ticket_text(df)
    df = prepare_targets(df)

    # Colonnes audit
    for col in ["hybrid_annotation_status", "hybrid_annotation_error", "hybrid_annotation_raw_json"]:
        if col not in df.columns:
            df[col] = ""

    texts = df[TEXT_COL].fillna("").astype(str).tolist()

    print(f"Chargement embeddings model : {EMBEDDING_MODEL}")
    embedder = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = embedder.encode(
        texts,
        show_progress_bar=True,
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    # Extraction par règles
    if "portfolio_reel" in df.columns:
        fill_count = 0
        for idx, text in zip(df.index, texts):
            if is_empty(df.at[idx, "portfolio_reel"]):
                value = extract_portfolio(text)
                if value:
                    df.at[idx, "portfolio_reel"] = value
                    fill_count += 1
        print(f"[portfolio_reel] Remplis par regex : {fill_count}")

    if "nav_date_reel" in df.columns:
        fill_count = 0
        for idx, text in zip(df.index, texts):
            if is_empty(df.at[idx, "nav_date_reel"]):
                value = extract_nav_date(text)
                if value:
                    df.at[idx, "nav_date_reel"] = value
                    fill_count += 1
        print(f"[nav_date_reel] Remplis par regex : {fill_count}")

    # ML sur champs cibles
    field_confidences = {}
    for field_name in TARGET_COLUMNS:
        df, conf_series = annotate_field_ml(
            df=df,
            embeddings=embeddings,
            field_name=field_name,
            confidence_threshold=ML_CONFIDENCE_THRESHOLD,
        )
        field_confidences[field_name] = conf_series

    # Fallback LLM pour lignes encore incomplètes / faible confiance
    if USE_LLM_FALLBACK:
        fallback_count = 0

        for idx, text in zip(df.index, texts):
            need_fallback = False

            # Si un champ cible reste vide
            for col in TARGET_COLUMNS + EXTRACT_COLUMNS:
                if is_empty(df.at[idx, col]):
                    need_fallback = True
                    break

            # Ou si confiance faible sur au moins un champ cible
            if not need_fallback:
                for field_name in TARGET_COLUMNS:
                    conf_col = f"{field_name}_confidence"
                    if conf_col in df.columns:
                        conf = df.at[idx, conf_col]
                        if pd.notna(conf) and float(conf) < LLM_FALLBACK_THRESHOLD:
                            need_fallback = True
                            break

            if not need_fallback:
                df.at[idx, "hybrid_annotation_status"] = "ml_only"
                continue

            try:
                llm_ann, raw_json = call_ollama_fallback(text)

                # N'écrit que les champs encore vides
                for col, value in llm_ann.items():
                    if col in df.columns and is_empty(df.at[idx, col]):
                        df.at[idx, col] = value

                df.at[idx, "hybrid_annotation_status"] = "llm_fallback"
                df.at[idx, "hybrid_annotation_raw_json"] = json.dumps(llm_ann, ensure_ascii=False)
                df.at[idx, "hybrid_annotation_error"] = ""
                fallback_count += 1

            except Exception as e:
                df.at[idx, "hybrid_annotation_status"] = "llm_error"
                df.at[idx, "hybrid_annotation_error"] = str(e)

        print(f"\nFallback LLM utilisé sur {fallback_count} lignes.")

    if not SAVE_CONFIDENCE_COLUMNS:
        to_drop = [c for c in df.columns if c.endswith("_pred") or c.endswith("_confidence")]
        df = df.drop(columns=to_drop, errors="ignore")

    df.to_excel(output_path, index=False)
    print(f"\nFichier écrit : {output_path.resolve()}")


if __name__ == "__main__":
    main()
