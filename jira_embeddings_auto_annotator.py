
"""
Annotation semi-automatique de tickets Jira avec :
- embeddings SentenceTransformers
- classifieurs LogisticRegression
- remplissage automatique des champs vides
- score de confiance par champ

Hypothèse :
Le fichier Excel contient au moins :
- ticket_text
- quelques lignes déjà annotées manuellement sur certains champs

Le script :
1. lit l'Excel
2. construit ticket_text si absent
3. entraîne un classifieur par champ cible à partir des lignes déjà annotées
4. prédit les champs vides pour les autres lignes
5. n'écrit que les prédictions dont la confiance dépasse un seuil
6. sauvegarde un nouvel Excel enrichi

Installation :
pip install pandas openpyxl scikit-learn sentence-transformers

Exemple :
python jira_embeddings_auto_annotator.py
"""

from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split


# =========================
# CONFIG
# =========================
INPUT_XLSX = "formatted_back_test_results.xlsx"
OUTPUT_XLSX = "formatted_back_test_results_ml_annotated.xlsx"

TEXT_COL = "ticket_text"

# Champs à entraîner / annoter automatiquement
TARGET_COLUMNS = [
    "scope_reel",
    "request_type_reel",
    "issue_type_reel",
    "tool_reel",
    "resolution_category",
    "is_ticket_exploitable",
]

# Champs plutôt extraits par règles / regex
EXTRACT_COLUMNS = [
    "portfolio_reel",
    "nav_date_reel",
]

# Seuil minimum de confiance pour écrire la prédiction
CONFIDENCE_THRESHOLD = 0.65

# Modèle embeddings
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# Colonnes utiles si ticket_text absent
SUMMARY_COL_CANDIDATES = ["summary", "Summary"]
DESCRIPTION_COL_CANDIDATES = ["description", "Description"]
COMMENTS_COL_CANDIDATES = ["comments", "Comments"]

# Si True, sauvegarde aussi les probabilités
SAVE_CONFIDENCE_COLUMNS = True


# =========================
# UTILS
# =========================
def first_existing(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def ensure_ticket_text(df: pd.DataFrame) -> pd.DataFrame:
    if TEXT_COL in df.columns and df[TEXT_COL].fillna("").astype(str).str.strip().ne("").any():
        return df

    summary_col = first_existing(df, SUMMARY_COL_CANDIDATES)
    description_col = first_existing(df, DESCRIPTION_COL_CANDIDATES)
    comments_col = first_existing(df, COMMENTS_COL_CANDIDATES)

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
    import re

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
    import re

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


# =========================
# TRAIN + PREDICT
# =========================
def fit_one_classifier(
    X: np.ndarray,
    y: pd.Series,
    field_name: str,
) -> Tuple[LogisticRegression, Dict[str, str]]:
    clf = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        n_jobs=None,
        random_state=42,
    )
    clf.fit(X, y)
    return clf, {"field": field_name}


def evaluate_classifier(X: np.ndarray, y: pd.Series, field_name: str) -> None:
    # Évaluation rapide si assez de données
    if len(y) < 20 or y.nunique() < 2:
        print(f"[{field_name}] Pas assez de données pour une vraie évaluation.")
        return

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.25,
            random_state=42,
            stratify=y,
        )
        clf = LogisticRegression(
            max_iter=2000,
            class_weight="balanced",
            random_state=42,
        )
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        print(f"\n=== Évaluation {field_name} ===")
        print(classification_report(y_test, y_pred, zero_division=0))
    except Exception as e:
        print(f"[{field_name}] Évaluation ignorée : {e}")


def annotate_field(
    df: pd.DataFrame,
    embeddings: np.ndarray,
    field_name: str,
    confidence_threshold: float,
) -> pd.DataFrame:
    train_mask = df[field_name].notna() & df[field_name].astype(str).str.strip().ne("")
    pred_mask = ~train_mask

    n_train = int(train_mask.sum())
    n_pred = int(pred_mask.sum())

    print(f"\n=== Champ {field_name} ===")
    print(f"Lignes annotées manuellement : {n_train}")
    print(f"Lignes à prédire : {n_pred}")

    if n_train < 10:
        print(f"[{field_name}] Pas assez de lignes annotées. Champ ignoré.")
        return df

    y_train = df.loc[train_mask, field_name].astype(str).str.strip()

    if y_train.nunique() < 2:
        print(f"[{field_name}] Une seule classe connue. Champ ignoré.")
        return df

    X_train = embeddings[train_mask.values]
    X_pred = embeddings[pred_mask.values]

    evaluate_classifier(X_train, y_train, field_name)

    clf, _ = fit_one_classifier(X_train, y_train, field_name)

    if n_pred == 0:
        print(f"[{field_name}] Rien à prédire.")
        return df

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

        if proba >= confidence_threshold and is_empty(df.at[idx, field_name]):
            df.at[idx, field_name] = label

    accepted = int(((df.loc[pred_indices, conf_col] >= confidence_threshold)).sum())
    print(f"[{field_name}] Prédictions acceptées : {accepted}/{n_pred}")

    return df


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

    # Extraction simple par règles
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

    # Annotation ML
    for field_name in TARGET_COLUMNS:
        df = annotate_field(
            df=df,
            embeddings=embeddings,
            field_name=field_name,
            confidence_threshold=CONFIDENCE_THRESHOLD,
        )

    # Colonnes d'audit
    if "ml_annotation_done" not in df.columns:
        df["ml_annotation_done"] = "yes"

    # Optionnel : retirer les colonnes confidence/pred
    if not SAVE_CONFIDENCE_COLUMNS:
        to_drop = [c for c in df.columns if c.endswith("_pred") or c.endswith("_confidence")]
        df = df.drop(columns=to_drop, errors="ignore")

    df.to_excel(output_path, index=False)
    print(f"\nFichier écrit : {output_path.resolve()}")


if __name__ == "__main__":
    main()
