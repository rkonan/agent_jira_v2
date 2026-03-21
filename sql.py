import streamlit as st
import pandas as pd
import oracledb
from dataclasses import dataclass
from typing import Optional, List, Dict, Any


# ==========================================================
# CONFIG
# ==========================================================
st.set_page_config(page_title="Oracle Table Explorer", layout="wide")


# ==========================================================
# DATACLASSES
# ==========================================================
@dataclass
class OracleConnectionConfig:
    user: str
    password: str
    host: str
    port: int
    service_name: str


# ==========================================================
# CONNEXION ORACLE
# ==========================================================
@st.cache_resource
def get_oracle_connection(cfg: OracleConnectionConfig):
    dsn = oracledb.makedsn(
        host=cfg.host,
        port=cfg.port,
        service_name=cfg.service_name
    )
    conn = oracledb.connect(
        user=cfg.user,
        password=cfg.password,
        dsn=dsn
    )
    return conn


# ==========================================================
# REQUETES SYSTEME ORACLE
# ==========================================================
def read_sql_df(conn, query: str, params: Optional[dict] = None) -> pd.DataFrame:
    return pd.read_sql(query, conn, params=params)


@st.cache_data(show_spinner=False)
def get_tables_list(
    user: str,
    host: str,
    port: int,
    service_name: str,
    schema_name: Optional[str] = None,
    table_name_like: Optional[str] = None,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    if schema_name:
        query = """
        SELECT owner, table_name
        FROM all_tables
        WHERE owner = :owner
        """
        params = {"owner": schema_name.upper()}
    else:
        query = """
        SELECT owner, table_name
        FROM all_tables
        WHERE 1 = 1
        """
        params = {}

    if table_name_like:
        query += "\nAND UPPER(table_name) LIKE :table_name_like"
        params["table_name_like"] = f"%{table_name_like.upper()}%"

    query += "\nORDER BY owner, table_name"

    return read_sql_df(conn, query, params)


@st.cache_data(show_spinner=False)
def get_table_columns(
    user: str,
    host: str,
    port: int,
    service_name: str,
    owner: str,
    table_name: str,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    query = """
    SELECT
        owner,
        table_name,
        column_id,
        column_name,
        data_type,
        data_length,
        data_precision,
        data_scale,
        nullable,
        data_default
    FROM all_tab_columns
    WHERE owner = :owner
      AND table_name = :table_name
    ORDER BY column_id
    """
    return read_sql_df(conn, query, {"owner": owner.upper(), "table_name": table_name.upper()})


@st.cache_data(show_spinner=False)
def get_primary_key(
    user: str,
    host: str,
    port: int,
    service_name: str,
    owner: str,
    table_name: str,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    query = """
    SELECT
        acc.owner,
        acc.table_name,
        acc.constraint_name,
        acc.column_name,
        acc.position
    FROM all_constraints ac
    JOIN all_cons_columns acc
      ON ac.owner = acc.owner
     AND ac.constraint_name = acc.constraint_name
    WHERE ac.constraint_type = 'P'
      AND ac.owner = :owner
      AND ac.table_name = :table_name
    ORDER BY acc.position
    """
    return read_sql_df(conn, query, {"owner": owner.upper(), "table_name": table_name.upper()})


@st.cache_data(show_spinner=False)
def get_foreign_keys(
    user: str,
    host: str,
    port: int,
    service_name: str,
    owner: str,
    table_name: str,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    query = """
    SELECT
        c.owner,
        c.table_name,
        c.constraint_name,
        col.column_name,
        c_pk.owner AS referenced_owner,
        c_pk.table_name AS referenced_table_name,
        col_pk.column_name AS referenced_column_name
    FROM all_constraints c
    JOIN all_cons_columns col
      ON c.owner = col.owner
     AND c.constraint_name = col.constraint_name
    JOIN all_constraints c_pk
      ON c.r_owner = c_pk.owner
     AND c.r_constraint_name = c_pk.constraint_name
    JOIN all_cons_columns col_pk
      ON c_pk.owner = col_pk.owner
     AND c_pk.constraint_name = col_pk.constraint_name
     AND col.position = col_pk.position
    WHERE c.constraint_type = 'R'
      AND c.owner = :owner
      AND c.table_name = :table_name
    ORDER BY c.constraint_name, col.position
    """
    return read_sql_df(conn, query, {"owner": owner.upper(), "table_name": table_name.upper()})


@st.cache_data(show_spinner=False)
def get_indexes(
    user: str,
    host: str,
    port: int,
    service_name: str,
    owner: str,
    table_name: str,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    query = """
    SELECT
        ai.table_owner,
        ai.table_name,
        ai.index_name,
        ai.uniqueness,
        aic.column_name,
        aic.column_position
    FROM all_indexes ai
    JOIN all_ind_columns aic
      ON ai.owner = aic.index_owner
     AND ai.index_name = aic.index_name
    WHERE ai.table_owner = :owner
      AND ai.table_name = :table_name
    ORDER BY ai.index_name, aic.column_position
    """
    return read_sql_df(conn, query, {"owner": owner.upper(), "table_name": table_name.upper()})


@st.cache_data(show_spinner=False)
def get_table_comment(
    user: str,
    host: str,
    port: int,
    service_name: str,
    owner: str,
    table_name: str,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    query = """
    SELECT owner, table_name, comments
    FROM all_tab_comments
    WHERE owner = :owner
      AND table_name = :table_name
    """
    return read_sql_df(conn, query, {"owner": owner.upper(), "table_name": table_name.upper()})


@st.cache_data(show_spinner=False)
def get_column_comments(
    user: str,
    host: str,
    port: int,
    service_name: str,
    owner: str,
    table_name: str,
) -> pd.DataFrame:
    cfg = OracleConnectionConfig(
        user=user,
        password=st.session_state["_db_password"],
        host=host,
        port=port,
        service_name=service_name,
    )
    conn = get_oracle_connection(cfg)

    query = """
    SELECT owner, table_name, column_name, comments
    FROM all_col_comments
    WHERE owner = :owner
      AND table_name = :table_name
    ORDER BY column_name
    """
    return read_sql_df(conn, query, {"owner": owner.upper(), "table_name": table_name.upper()})


# ==========================================================
# HELPERS
# ==========================================================
def format_column_type(row: pd.Series) -> str:
    data_type = row["DATA_TYPE"]

    if data_type in {"VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"}:
        return f"{data_type}({row['DATA_LENGTH']})"

    if data_type == "NUMBER":
        precision = row["DATA_PRECISION"]
        scale = row["DATA_SCALE"]

        if pd.notna(precision) and pd.notna(scale):
            return f"NUMBER({int(precision)},{int(scale)})"
        if pd.notna(precision):
            return f"NUMBER({int(precision)})"
        return "NUMBER"

    return data_type


def guess_filter_columns(columns_df: pd.DataFrame, pk_df: pd.DataFrame, indexes_df: pd.DataFrame) -> List[str]:
    candidates = []

    if not pk_df.empty:
        candidates.extend(pk_df["COLUMN_NAME"].tolist())

    indexed_cols = indexes_df["COLUMN_NAME"].dropna().unique().tolist() if not indexes_df.empty else []
    candidates.extend(indexed_cols)

    semantic_priority = [
        "DATE_ARRETE", "DATE_CLOTURE", "DATE_EXECUTION", "DATE_OPERATION",
        "CODE_DOSSIER", "ID_DOSSIER", "DOSSIER",
        "ISIN", "DEVISE", "PORTEFEUILLE", "NUMERO_COMPTE"
    ]

    available_cols = columns_df["COLUMN_NAME"].tolist()

    for col in semantic_priority:
        if col in available_cols:
            candidates.append(col)

    seen = set()
    ordered = []
    for col in candidates:
        if col not in seen:
            ordered.append(col)
            seen.add(col)

    return ordered[:8]


def build_select_template(owner: str, table_name: str, columns_df: pd.DataFrame, pk_df: pd.DataFrame, indexes_df: pd.DataFrame) -> str:
    cols = columns_df["COLUMN_NAME"].tolist()
    col_lines = ",\n    ".join(cols[:50]) if len(cols) <= 50 else ",\n    ".join(cols[:50]) + ",\n    -- ..."

    filter_cols = guess_filter_columns(columns_df, pk_df, indexes_df)

    if filter_cols:
        where_clause = "\nAND ".join([f"{c} = :{c}" for c in filter_cols[:3]])
        where_block = f"WHERE {where_clause}"
    else:
        where_block = "WHERE ROWNUM < 100"

    return (
        f"SELECT \n"
        f"    {col_lines}\n"
        f"FROM {owner}.{table_name}\n"
        f"{where_block};"
    )


def build_count_template(owner: str, table_name: str, columns_df: pd.DataFrame, pk_df: pd.DataFrame, indexes_df: pd.DataFrame) -> str:
    filter_cols = guess_filter_columns(columns_df, pk_df, indexes_df)

    if filter_cols:
        where_clause = "\nAND ".join([f"{c} = :{c}" for c in filter_cols[:2]])
        where_block = f"WHERE {where_clause}"
    else:
        where_block = ""

    return (
        f"SELECT COUNT(*) AS NB_LIGNES\n"
        f"FROM {owner}.{table_name}\n"
        f"{where_block};"
    )


def build_insert_template(owner: str, table_name: str, columns_df: pd.DataFrame) -> str:
    cols = columns_df["COLUMN_NAME"].tolist()

    insertable_cols = []
    for _, row in columns_df.iterrows():
        col = row["COLUMN_NAME"]
        default = row["DATA_DEFAULT"]
        if default is not None:
            continue
        insertable_cols.append(col)

    if not insertable_cols:
        insertable_cols = cols

    col_block = ",\n    ".join(insertable_cols[:25])
    val_block = ",\n    ".join([f":{c}" for c in insertable_cols[:25]])

    suffix = "\n    -- ..." if len(insertable_cols) > 25 else ""

    return (
        f"INSERT INTO {owner}.{table_name} (\n"
        f"    {col_block}{suffix}\n"
        f") VALUES (\n"
        f"    {val_block}{suffix}\n"
        f");"
    )


def build_update_template(owner: str, table_name: str, columns_df: pd.DataFrame, pk_df: pd.DataFrame) -> str:
    cols = columns_df["COLUMN_NAME"].tolist()
    pk_cols = pk_df["COLUMN_NAME"].tolist() if not pk_df.empty else []

    updatable_cols = [c for c in cols if c not in pk_cols][:8]
    set_block = ",\n    ".join([f"{c} = :{c}" for c in updatable_cols]) if updatable_cols else "COL1 = :COL1"

    if pk_cols:
        where_block = "\nAND ".join([f"{c} = :{c}" for c in pk_cols])
    else:
        where_block = "/* AJOUTER UNE CLAUSE WHERE */ 1 = 0"

    return (
        f"UPDATE {owner}.{table_name}\n"
        f"SET \n"
        f"    {set_block}\n"
        f"WHERE {where_block};"
    )


def build_delete_template(owner: str, table_name: str, pk_df: pd.DataFrame) -> str:
    pk_cols = pk_df["COLUMN_NAME"].tolist() if not pk_df.empty else []

    if pk_cols:
        where_block = "\nAND ".join([f"{c} = :{c}" for c in pk_cols])
    else:
        where_block = "/* AJOUTER UNE CLAUSE WHERE */ 1 = 0"

    return (
        f"DELETE FROM {owner}.{table_name}\n"
        f"WHERE {where_block};"
    )


def build_templates(owner: str, table_name: str, columns_df: pd.DataFrame, pk_df: pd.DataFrame, indexes_df: pd.DataFrame) -> Dict[str, str]:
    return {
        "SELECT": build_select_template(owner, table_name, columns_df, pk_df, indexes_df),
        "COUNT": build_count_template(owner, table_name, columns_df, pk_df, indexes_df),
        "INSERT": build_insert_template(owner, table_name, columns_df),
        "UPDATE": build_update_template(owner, table_name, columns_df, pk_df),
        "DELETE": build_delete_template(owner, table_name, pk_df),
    }


def safe_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value)


# ==========================================================
# SIDEBAR
# ==========================================================
st.sidebar.title("Connexion Oracle")

db_user = st.sidebar.text_input("User")
db_password = st.sidebar.text_input("Password", type="password")
db_host = st.sidebar.text_input("Host")
db_port = st.sidebar.number_input("Port", value=1521, step=1)
db_service = st.sidebar.text_input("Service name")
schema_filter = st.sidebar.text_input("Schéma", value="")
table_search = st.sidebar.text_input("Recherche table", value="")

connect_clicked = st.sidebar.button("Charger les tables")

if db_password:
    st.session_state["_db_password"] = db_password


# ==========================================================
# MAIN
# ==========================================================
st.title("Oracle Table Explorer")
st.caption("Exploration metadata + génération automatique de templates SQL")

if connect_clicked:
    missing = [x for x in [db_user, db_password, db_host, db_service] if not x]
    if missing:
        st.error("Renseigne user, password, host et service name.")
        st.stop()

    try:
        tables_df = get_tables_list(
            user=db_user,
            host=db_host,
            port=int(db_port),
            service_name=db_service,
            schema_name=schema_filter or None,
            table_name_like=table_search or None,
        )
        st.session_state["tables_df"] = tables_df
        st.success(f"{len(tables_df)} table(s) trouvée(s).")
    except Exception as e:
        st.error(f"Erreur Oracle : {e}")
        st.stop()

tables_df = st.session_state.get("tables_df")

if tables_df is None or tables_df.empty:
    st.info("Charge d'abord la liste des tables.")
    st.stop()

search_ui = st.text_input("Filtre rapide dans la liste affichée", value="")
display_df = tables_df.copy()

if search_ui:
    mask = (
        display_df["OWNER"].astype(str).str.upper().str.contains(search_ui.upper(), na=False)
        | display_df["TABLE_NAME"].astype(str).str.upper().str.contains(search_ui.upper(), na=False)
    )
    display_df = display_df[mask].copy()

st.subheader("Tables trouvées")
st.dataframe(display_df, use_container_width=True, hide_index=True)

display_df["FULL_NAME"] = display_df["OWNER"] + "." + display_df["TABLE_NAME"]

selected_full_name = st.selectbox(
    "Choisis une table",
    options=display_df["FULL_NAME"].tolist()
)

selected_owner, selected_table = selected_full_name.split(".", 1)

try:
    columns_df = get_table_columns(
        user=db_user,
        host=db_host,
        port=int(db_port),
        service_name=db_service,
        owner=selected_owner,
        table_name=selected_table,
    )

    pk_df = get_primary_key(
        user=db_user,
        host=db_host,
        port=int(db_port),
        service_name=db_service,
        owner=selected_owner,
        table_name=selected_table,
    )

    fk_df = get_foreign_keys(
        user=db_user,
        host=db_host,
        port=int(db_port),
        service_name=db_service,
        owner=selected_owner,
        table_name=selected_table,
    )

    indexes_df = get_indexes(
        user=db_user,
        host=db_host,
        port=int(db_port),
        service_name=db_service,
        owner=selected_owner,
        table_name=selected_table,
    )

    table_comment_df = get_table_comment(
        user=db_user,
        host=db_host,
        port=int(db_port),
        service_name=db_service,
        owner=selected_owner,
        table_name=selected_table,
    )

    column_comments_df = get_column_comments(
        user=db_user,
        host=db_host,
        port=int(db_port),
        service_name=db_service,
        owner=selected_owner,
        table_name=selected_table,
    )

except Exception as e:
    st.error(f"Erreur pendant la lecture de la metadata : {e}")
    st.stop()

templates = build_templates(selected_owner, selected_table, columns_df, pk_df, indexes_df)

st.markdown(f"## {selected_owner}.{selected_table}")

comment_value = ""
if not table_comment_df.empty:
    comment_value = safe_text(table_comment_df.iloc[0]["COMMENTS"])

metric1, metric2, metric3, metric4 = st.columns(4)
metric1.metric("Colonnes", len(columns_df))
metric2.metric("PK", len(pk_df))
metric3.metric("FK", fk_df["CONSTRAINT_NAME"].nunique() if not fk_df.empty else 0)
metric4.metric("Index", indexes_df["INDEX_NAME"].nunique() if not indexes_df.empty else 0)

if comment_value:
    st.info(f"Commentaire table : {comment_value}")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Colonnes",
    "Contraintes",
    "Index",
    "Templates SQL",
    "Profil métier",
])

with tab1:
    col_comments_map = {}
    if not column_comments_df.empty:
        col_comments_map = dict(zip(column_comments_df["COLUMN_NAME"], column_comments_df["COMMENTS"]))

    columns_display = columns_df.copy()
    columns_display["FORMAT_TYPE"] = columns_display.apply(format_column_type, axis=1)
    columns_display["COMMENTAIRE"] = columns_display["COLUMN_NAME"].map(col_comments_map).fillna("")

    final_cols = [
        "COLUMN_ID",
        "COLUMN_NAME",
        "FORMAT_TYPE",
        "NULLABLE",
        "DATA_DEFAULT",
        "COMMENTAIRE",
    ]
    st.dataframe(columns_display[final_cols], use_container_width=True, hide_index=True)

with tab2:
    left, right = st.columns(2)

    with left:
        st.markdown("### Clé primaire")
        if pk_df.empty:
            st.write("Aucune PK trouvée")
        else:
            st.dataframe(pk_df[["CONSTRAINT_NAME", "COLUMN_NAME", "POSITION"]], use_container_width=True, hide_index=True)

    with right:
        st.markdown("### Clés étrangères")
        if fk_df.empty:
            st.write("Aucune FK trouvée")
        else:
            st.dataframe(
                fk_df[
                    [
                        "CONSTRAINT_NAME",
                        "COLUMN_NAME",
                        "REFERENCED_OWNER",
                        "REFERENCED_TABLE_NAME",
                        "REFERENCED_COLUMN_NAME",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

with tab3:
    if indexes_df.empty:
        st.write("Aucun index trouvé")
    else:
        st.dataframe(
            indexes_df[
                [
                    "INDEX_NAME",
                    "UNIQUENESS",
                    "COLUMN_NAME",
                    "COLUMN_POSITION",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

with tab4:
    sql_tab1, sql_tab2, sql_tab3, sql_tab4, sql_tab5 = st.tabs(["SELECT", "COUNT", "INSERT", "UPDATE", "DELETE"])

    with sql_tab1:
        st.code(templates["SELECT"], language="sql")

    with sql_tab2:
        st.code(templates["COUNT"], language="sql")

    with sql_tab3:
        st.code(templates["INSERT"], language="sql")

    with sql_tab4:
        st.code(templates["UPDATE"], language="sql")

    with sql_tab5:
        st.code(templates["DELETE"], language="sql")

with tab5:
    st.markdown("### Lecture rapide")

    pk_cols = pk_df["COLUMN_NAME"].tolist() if not pk_df.empty else []
    fk_tables = fk_df["REFERENCED_TABLE_NAME"].dropna().unique().tolist() if not fk_df.empty else []
    indexed_cols = indexes_df["COLUMN_NAME"].dropna().unique().tolist() if not indexes_df.empty else []
    guessed_filters = guess_filter_columns(columns_df, pk_df, indexes_df)

    st.write(f"Colonnes de PK : {', '.join(pk_cols) if pk_cols else 'Aucune'}")
    st.write(f"Tables liées via FK : {', '.join(fk_tables) if fk_tables else 'Aucune'}")
    st.write(f"Colonnes indexées : {', '.join(indexed_cols[:10]) if indexed_cols else 'Aucune'}")
    st.write(f"Filtres probables pour investigation : {', '.join(guessed_filters) if guessed_filters else 'Aucun'}")

    date_cols = [
        col for col in columns_df["COLUMN_NAME"].tolist()
        if "DATE" in col.upper()
    ]
    amount_cols = [
        col for col in columns_df["COLUMN_NAME"].tolist()
        if any(token in col.upper() for token in ["MONTANT", "AMOUNT", "VALOR", "NAV", "PRICE", "PNL"])
    ]

    st.write(f"Colonnes date : {', '.join(date_cols) if date_cols else 'Aucune'}")
    st.write(f"Colonnes montant / valorisation : {', '.join(amount_cols) if amount_cols else 'Aucune'}")
