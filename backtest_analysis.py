import pandas as pd
from collections import Counter
from pathlib import Path

# =========================
# 1. CONFIG
# =========================
INPUT_JSONL = "backtest_results.jsonl"
OUTPUT_EXCEL = "backtest_analysis.xlsx"

# =========================
# 2. LOAD JSONL
# =========================
df = pd.read_json(INPUT_JSONL, lines=True)

print("Shape initiale :", df.shape)
print("Colonnes :", df.columns.tolist())

# =========================
# 3. FLATTEN NESTED COLUMNS
# =========================
nested_cols = ["scope_decision", "entities", "classification"]

flat_parts = []
base_df = df.copy()

for col in nested_cols:
    if col in base_df.columns:
        part = pd.json_normalize(base_df[col]).add_prefix(f"{col}_")
        flat_parts.append(part)
        base_df = base_df.drop(columns=[col])

if flat_parts:
    df_flat = pd.concat([base_df] + flat_parts, axis=1)
else:
    df_flat = base_df.copy()

print("Shape aplatie :", df_flat.shape)

# =========================
# 4. BASIC FEATURE ENGINEERING
# =========================
def safe_len_list(x):
    return len(x) if isinstance(x, list) else 0

df_flat["nb_checks"] = df_flat["checks_run"].apply(safe_len_list) if "checks_run" in df_flat.columns else 0
df_flat["nb_findings"] = df_flat["findings"].apply(safe_len_list) if "findings" in df_flat.columns else 0
df_flat["nb_missing_info"] = df_flat["missing_information"].apply(safe_len_list) if "missing_information" in df_flat.columns else 0
df_flat["has_similar_tickets"] = df_flat["similar_tickets"].apply(lambda x: len(x) > 0 if isinstance(x, list) else False) if "similar_tickets" in df_flat.columns else False

# =========================
# 5. DESCRIPTIVE METRICS
# =========================
global_metrics = {
    "nb_tickets": len(df_flat),
    "nb_level_1_done": int((df_flat["status"] == "level_1_done").sum()) if "status" in df_flat.columns else 0,
    "nb_need_human_review": int((df_flat["status"] == "need_human_review").sum()) if "status" in df_flat.columns else 0,
    "nb_out_of_scope": int((df_flat["status"] == "out_of_scope").sum()) if "status" in df_flat.columns else 0,
    "avg_elapsed_seconds": float(df_flat["elapsed_seconds"].mean()) if "elapsed_seconds" in df_flat.columns else 0.0,
    "median_elapsed_seconds": float(df_flat["elapsed_seconds"].median()) if "elapsed_seconds" in df_flat.columns else 0.0,
    "avg_nb_checks": float(df_flat["nb_checks"].mean()),
    "avg_nb_findings": float(df_flat["nb_findings"].mean()),
}

metrics_df = pd.DataFrame(
    [{"metric": k, "value": v} for k, v in global_metrics.items()]
)

# =========================
# 6. DISTRIBUTIONS
# =========================
def vc_to_df(series, col_name):
    return (
        series.value_counts(dropna=False)
        .rename_axis(col_name)
        .reset_index(name="count")
    )

status_dist = vc_to_df(df_flat["status"], "status") if "status" in df_flat.columns else pd.DataFrame()
scope_dist = vc_to_df(df_flat["scope"], "scope") if "scope" in df_flat.columns else pd.DataFrame()
issue_dist = vc_to_df(df_flat["issue_type"], "issue_type") if "issue_type" in df_flat.columns else pd.DataFrame()

clf_request_dist = (
    vc_to_df(df_flat["classification_request_type"], "classification_request_type")
    if "classification_request_type" in df_flat.columns else pd.DataFrame()
)

clf_issue_dist = (
    vc_to_df(df_flat["classification_issue_family"], "classification_issue_family")
    if "classification_issue_family" in df_flat.columns else pd.DataFrame()
)

scope_match_method_dist = (
    vc_to_df(df_flat["scope_decision_match_method"], "scope_decision_match_method")
    if "scope_decision_match_method" in df_flat.columns else pd.DataFrame()
)

confidence_dist = vc_to_df(df_flat["confidence"], "confidence") if "confidence" in df_flat.columns else pd.DataFrame()

# =========================
# 7. CHECK USAGE
# =========================
check_counter = Counter()
if "checks_run" in df_flat.columns:
    for checks in df_flat["checks_run"].dropna():
        if isinstance(checks, list):
            check_counter.update(checks)

checks_usage_df = (
    pd.DataFrame(check_counter.items(), columns=["check_name", "count"])
    .sort_values("count", ascending=False)
    .reset_index(drop=True)
)

# =========================
# 8. ISSUE TYPE x AVG TIME
# =========================
issue_time_df = pd.DataFrame()
if "issue_type" in df_flat.columns and "elapsed_seconds" in df_flat.columns:
    issue_time_df = (
        df_flat.groupby("issue_type", dropna=False)["elapsed_seconds"]
        .agg(["count", "mean", "median", "max"])
        .reset_index()
        .sort_values("mean", ascending=False)
    )

# =========================
# 9. TICKETS TO REVIEW
# =========================
review_cols = [
    c for c in [
        "ticket_id",
        "status",
        "scope",
        "issue_type",
        "classification_request_type",
        "classification_issue_family",
        "checks_run",
        "nb_checks",
        "confidence",
        "recommended_action",
        "elapsed_seconds",
        "mail_preview",
    ]
    if c in df_flat.columns
]

tickets_no_checks_df = df_flat.loc[df_flat["nb_checks"] == 0, review_cols].copy()
tickets_slow_df = (
    df_flat.sort_values("elapsed_seconds", ascending=False)[review_cols].head(50).copy()
    if "elapsed_seconds" in df_flat.columns else pd.DataFrame()
)
tickets_human_review_df = (
    df_flat.loc[df_flat["status"] == "need_human_review", review_cols].copy()
    if "status" in df_flat.columns else pd.DataFrame()
)

# =========================
# 10. SIMPLE HEURISTIC SCORE
# =========================
df_flat["score_simple"] = 0

if "status" in df_flat.columns:
    df_flat.loc[df_flat["status"] == "level_1_done", "score_simple"] += 1

df_flat.loc[df_flat["nb_checks"] > 0, "score_simple"] += 1

if "confidence" in df_flat.columns:
    df_flat.loc[df_flat["confidence"] == "high", "score_simple"] += 1

score_dist = vc_to_df(df_flat["score_simple"], "score_simple")

# =========================
# 11. OPTIONAL LOAD GROUND TRUTH
# =========================
# Si plus tard tu ajoutes ces colonnes dans le JSONL :
# - expected_scope
# - expected_issue_family
# - expected_tools
#
# Tu peux décommenter ceci.

accuracy_sheets = {}

if "expected_scope" in df_flat.columns and "scope" in df_flat.columns:
    df_flat["scope_correct"] = df_flat["expected_scope"] == df_flat["scope"]
    accuracy_sheets["scope_accuracy"] = pd.DataFrame(
        [{"metric": "scope_accuracy", "value": df_flat["scope_correct"].mean()}]
    )

if "expected_issue_family" in df_flat.columns and "issue_type" in df_flat.columns:
    df_flat["issue_family_correct"] = df_flat["expected_issue_family"] == df_flat["issue_type"]
    accuracy_sheets["issue_family_accuracy"] = pd.DataFrame(
        [{"metric": "issue_family_accuracy", "value": df_flat["issue_family_correct"].mean()}]
    )

# =========================
# 12. EXPORT TO EXCEL
# =========================
output_path = Path(OUTPUT_EXCEL)

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    metrics_df.to_excel(writer, sheet_name="metrics_globales", index=False)
    df_flat.to_excel(writer, sheet_name="resultats_aplatis", index=False)

    if not status_dist.empty:
        status_dist.to_excel(writer, sheet_name="dist_status", index=False)

    if not scope_dist.empty:
        scope_dist.to_excel(writer, sheet_name="dist_scope", index=False)

    if not issue_dist.empty:
        issue_dist.to_excel(writer, sheet_name="dist_issue_type", index=False)

    if not clf_request_dist.empty:
        clf_request_dist.to_excel(writer, sheet_name="dist_request_type", index=False)

    if not clf_issue_dist.empty:
        clf_issue_dist.to_excel(writer, sheet_name="dist_issue_family", index=False)

    if not scope_match_method_dist.empty:
        scope_match_method_dist.to_excel(writer, sheet_name="dist_scope_match", index=False)

    if not confidence_dist.empty:
        confidence_dist.to_excel(writer, sheet_name="dist_confidence", index=False)

    if not checks_usage_df.empty:
        checks_usage_df.to_excel(writer, sheet_name="checks_usage", index=False)

    if not issue_time_df.empty:
        issue_time_df.to_excel(writer, sheet_name="issue_time", index=False)

    if not tickets_no_checks_df.empty:
        tickets_no_checks_df.to_excel(writer, sheet_name="tickets_no_checks", index=False)

    if not tickets_slow_df.empty:
        tickets_slow_df.to_excel(writer, sheet_name="tickets_slow", index=False)

    if not tickets_human_review_df.empty:
        tickets_human_review_df.to_excel(writer, sheet_name="tickets_human_review", index=False)

    if not score_dist.empty:
        score_dist.to_excel(writer, sheet_name="score_simple", index=False)

    for sheet_name, sheet_df in accuracy_sheets.items():
        sheet_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

print(f"Excel écrit : {output_path.resolve()}")
print(metrics_df)