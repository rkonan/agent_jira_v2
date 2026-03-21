import streamlit as st
import pandas as pd

# --------------------------------------------------
# Exemple
# --------------------------------------------------
df = pd.DataFrame([
    {
        "DOMAINE": "Audit",
        "LISTE DES TABLES": "TABLE_LOGBOOK",
        "OPTION GP : traitement": "Tout traitement qui trace",
    },
    {
        "DOMAINE": "Cloture/arrete",
        "LISTE DES TABLES": "HISTORIQUE_CLOTURE_PERIODE",
        "OPTION GP : traitement": "CALPER",
    },
    {
        "DOMAINE": "Cloture/arrete",
        "LISTE DES TABLES": "PERIODE_COMPTABLE",
        "OPTION GP : traitement": "CLOEXO",
    },
])

TABLE_CONFIG = {
    "TABLE_LOGBOOK": {
        "pk": ["ID_LOG"],
        "date_cols": ["DATE_EXECUTION"],
        "use_cases": ["trace", "audit"],
        "default_where": "WHERE ROWNUM < 100"
    },
    "HISTORIQUE_CLOTURE_PERIODE": {
        "pk": ["ID_DOSSIER", "DATE_CLOTURE"],
        "date_cols": ["DATE_CLOTURE"],
        "use_cases": ["cloture"],
        "default_where": "WHERE DATE_CLOTURE = TO_DATE('2026-03-21', 'YYYY-MM-DD')"
    },
}



def build_sql_templates(table_name: str, config: dict) -> dict:
    table_cfg = config.get(table_name, {})
    pk_cols = table_cfg.get("pk", [])
    default_where = table_cfg.get("default_where", "WHERE ROWNUM < 100")

    select_sql = f"SELECT *\nFROM {table_name}\n{default_where};"

    count_sql = f"SELECT COUNT(*) AS NB_LIGNES\nFROM {table_name};"

    if pk_cols:
        where_pk = " AND\n    ".join([f"{col} = :{col}" for col in pk_cols])
        select_pk_sql = f"SELECT *\nFROM {table_name}\nWHERE {where_pk};"
    else:
        select_pk_sql = f"SELECT *\nFROM {table_name}\nWHERE ROWNUM = 1;"

    insert_cols = pk_cols if pk_cols else ["COL1", "COL2", "COL3"]
    insert_cols_sql = ",\n    ".join(insert_cols)
    insert_vals_sql = ",\n    ".join([f":{col}" for col in insert_cols])

    insert_sql = (
        f"INSERT INTO {table_name} (\n"
        f"    {insert_cols_sql}\n"
        f") VALUES (\n"
        f"    {insert_vals_sql}\n"
        f");"
    )

    return {
        "SELECT simple": select_sql,
        "SELECT sur PK": select_pk_sql,
        "COUNT": count_sql,
        "INSERT template": insert_sql,
    }


# --------------------------------------------------
# Fonction de génération de templates
# --------------------------------------------------
def build_sql_templates_old(table_name: str) -> dict:
    templates = {
        "SELECT simple": f"SELECT *\nFROM {table_name}\nWHERE ROWNUM < 100;",
        
        "SELECT avec filtre date": (
            f"SELECT *\n"
            f"FROM {table_name}\n"
            f"WHERE DATE_ARRETE = TO_DATE('2026-03-21', 'YYYY-MM-DD');"
        ),

        "COUNT": f"SELECT COUNT(*) AS NB_LIGNES\nFROM {table_name};",

        "INSERT template": (
            f"INSERT INTO {table_name} (\n"
            f"    COL1,\n"
            f"    COL2,\n"
            f"    COL3\n"
            f") VALUES (\n"
            f"    :COL1,\n"
            f"    :COL2,\n"
            f"    :COL3\n"
            f");"
        ),
    }
    return templates

# --------------------------------------------------
# Affichage tableau
# --------------------------------------------------
st.subheader("Tables GP")
st.dataframe(df, use_container_width=True, hide_index=True)

# --------------------------------------------------
# Sélection
# --------------------------------------------------
selected_table = st.selectbox(
    "Choisir une table",
    options=df["LISTE DES TABLES"].tolist()
)

selected_row = df[df["LISTE DES TABLES"] == selected_table].iloc[0]

st.markdown("### Détail de la table")
st.write(f"Domaine : {selected_row['DOMAINE']}")
st.write(f"Option GP : {selected_row['OPTION GP : traitement']}")

# --------------------------------------------------
# Templates SQL
# --------------------------------------------------
# templates = build_sql_templates(selected_table)
templates =build_sql_templates(selected_table,TABLE_CONFIG)
st.markdown("### Templates SQL")


tab1, tab2, tab3, tab4 = st.tabs(["SELECT", "INSERT", "UPDATE", "DELETE"])

with tab1:
   
    template_type = st.radio(
    "Type de template",
    options=list([c for c in templates.keys() if "SELECT" in templates[c]]),
    horizontal=True
)
    st.code(templates[template_type], language="sql")

with tab2:
    st.code(templates["INSERT template"], language="sql")

with tab3:
    st.code(f"UPDATE {selected_table}\nSET COL1 = :COL1\nWHERE ID = :ID;", language="sql")

with tab4:
    st.code(f"DELETE FROM {selected_table}\nWHERE ID = :ID;", language="sql")
