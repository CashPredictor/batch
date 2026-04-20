# db_init.py
import argparse
import os
import sqlite3
import sys
import yaml
from pathlib import Path

def load_sql_file(path: str) -> str:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return p.read_text(encoding="utf-8")

def exec_sql_file(conn, path: str):
    sql = Path(path).read_text(encoding="utf-8")
    conn.executescript(sql)
    
def exec_sql_dir(conn, dir_path: str):
    for path in sorted(Path(dir_path).glob("*.sql")):
        print(f"[INFO] Executing {path}")
        exec_sql_file(conn, str(path))

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def exec_sql(conn: sqlite3.Connection, sql: str | None, label: str = "") -> None:
    if not sql:
        return
    try:
        conn.executescript(sql)
    except sqlite3.Error as e:
        raise RuntimeError(f"Błąd podczas wykonywania SQL [{label}]: {e}") from e


def table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    )
    return cursor.fetchone() is not None

def create_indexes(index_definitions: dict[str, list[str]], conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    for table_name, columns in index_definitions.items():
        if not table_exists(cursor, table_name):
            print(f"[WARN] Pomijam indeksy dla '{table_name}' – tabela jeszcze nie istnieje.")
            continue

        for column in columns:
            index_name = f"{table_name}_{column}_idx"
            sql_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column});"
            cursor.execute(sql_query)

    conn.commit()


def create_aggregated_data_temp_schema(cursor: sqlite3.Cursor, params: dict) -> None:
    cursor.execute(params.get("query_aggregated_data_temp"))
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ag_day
        ON aggregated_data_temp (DZIEN);
    """)

def seed_swieta(cursor: sqlite3.Cursor, params: dict) -> None:
    columns = params.get("swieta_columns")
    rows = params.get("swieta_rows")

    if not columns or not rows:
        print("[WARN] Brak definicji świąt w configu, pomijam seed tabeli swieta.")
        return

    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ", ".join(columns)

    # wstawiamy tylko jeśli identyczny rekord jeszcze nie istnieje
    where_clause = " AND ".join([f"{col} = ?" for col in columns])

    inserted = 0
    for row in rows:
        if len(row) != len(columns):
            raise RuntimeError(
                f"Nieprawidłowy rekord swieta. Oczekiwano {len(columns)} pól, "
                f"otrzymano {len(row)}: {row}"
            )

        cursor.execute(
            f"SELECT 1 FROM swieta WHERE {where_clause} LIMIT 1",
            tuple(row),
        )
        exists = cursor.fetchone() is not None

        if not exists:
            cursor.execute(
                f"INSERT INTO swieta ({columns_sql}) VALUES ({placeholders})",
                tuple(row),
            )
            inserted += 1

    print(f"[OK] Seed swieta zakończony. Dodano {inserted} nowych rekordów.")

def main():
    
    parser = argparse.ArgumentParser(description="SQLite DB schema initialization")
    parser.add_argument("--config", required=True)
    parser.add_argument("--db-path", required=True)
    args = parser.parse_args()

    # 1. Fail, jeśli DB już istnieje
    if os.path.exists(args.db_path):
        print(f"[ERROR] Baza już istnieje: {args.db_path}")
        sys.exit(1)

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

    cfg = load_config(args.config)
    params = cfg["first_model_params"]

    schema_dir = cfg["sql_paths"]["schema_dir"]
    views_dir = cfg["sql_paths"]["views_dir"]
    exec_sql_dir(conn, schema_dir)
    exec_sql_dir(conn, views_dir)    

    conn = sqlite3.connect(args.db_path)
    cursor = conn.cursor()

    try:
        # ============================================================
        # 2. Tabele statyczne / deterministyczne
        # ============================================================
        create_aggregated_data_temp_schema(cursor, params)

        exec_sql(conn, params.get("query_swieta"), "query_swieta")
        seed_swieta(cursor, params)
        conn.commit()
        
        exec_sql(conn, params.get("query_create_group_cust_inv_days"), "query_create_group_cust_inv_days")
        exec_sql(conn, params.get("query_create_summary_client_days_tab2"), "query_create_summary_client_days_tab2")
        exec_sql(conn, params.get("query_create_dataset_tab"), "query_create_dataset_tab")
        exec_sql(conn, params.get("statystyki_faktur"), "statystyki_faktur")

        exec_sql(conn, params.get("query_create_invo_table"), "query_create_invo_table")
        exec_sql(conn, params.get("query_create_debc_table"), "query_create_debc_table")
        exec_sql(conn, params.get("query_create_clhs_table"), "query_create_clhs_table")
        exec_sql(conn, params.get("query_create_dcmo_table"), "query_create_dcmo_table")
        
        exec_sql(conn, params.get("query_create_grouped_client_days_table"), "query_create_grouped_client_days_table")
        exec_sql(conn, params.get("query_create_dataset"), "query_create_dataset")
        exec_sql(conn, params.get("query_create_invo_clhs_joined_tab_table"), "query_create_invo_clhs_joined_tab_table")
        exec_sql(conn, params.get("query_create_extended_grouped_client_days_tab_table"), "query_create_extended_grouped_client_days_tab_table")
        exec_sql(conn, params.get("query_create_first_model_tab"), "query_create_first_model_tab")

        conn.commit()

        exec_sql(conn, params.get("query_create_invo_view"), "query_create_invo_view")
        exec_sql(conn, params.get("query_create_clhs_view"), "query_create_clhs_view")
        exec_sql(conn, params.get("query_create_DEBC_view"), "query_create_DEBC_view")
        exec_sql(conn, params.get("query_create_dcmo_view"), "query_create_dcmo_view")
        exec_sql(conn, params.get("query_create_INVO_CLHS_JOINED"), "query_create_INVO_CLHS_JOINED")
        exec_sql(conn, params.get("query_create_summary_client_days_view"), "query_create_summary_client_days_view")
        exec_sql(conn, params.get("query_create_extended_grouped_client_days_view"), "query_create_extended_grouped_client_days_view")
        exec_sql(conn, params.get("query_create_stats_pivot_view"), "query_create_stats_pivot_view")
        exec_sql(conn, params.get("query_create_dataset_raw_view"), "query_create_dataset_raw_view")
        conn.commit()

        # ============================================================
        # 5. Indeksy na znanych tabelach
        # ============================================================
        index_definitions = {
            "invo": ["INVO_NO", "INVO_ADMNO", "INVO_CLNTNO", "INVO_DEBH_NO"],
            "debc": ["DEBC_NO", "DEBC_DEBH", "DEBC_ADMNO", "DEBC_CLNTNO", "DEBC_REINSURANCENUMBER"],
            "clhs": ["CLHS_NO", "CLHS_DEBC_NO", "CLHS_DEBH_NO", "CLHS_ADMNO", "CLHS_CLNTNO", "CLHS_DATCHANGED", "CLHS_TIMCHANGED"],
            "dcmo": ["DCMO_DEBC_NO", "DCMO_YEAR", "DCMO_MONTH"],
            "group_cust_inv_days": ["INVO_NO", "INVO_ADMNO", "INVO_CLNTNO", "INVO_DEBH_NO", "INVO_DEBC_NO", "DZIEN", "MIESIAC", "ROK"],
            "grouped_client_days": ["INVO_ADMNO", "INVO_CLNTNO", "INVO_DEBH_NO", "INVO_DEBC_NO", "DZIEN", "MIESIAC", "ROK"],
            "INVO_CLHS_JOINED_TAB": ["INVO_NO", "INVO_ADMNO", "INVO_CLNTNO", "INVO_DEBH_NO", "INVO_DEBC_NO", "CLHS_NEXT_CHANGED_DATETIME", "CLHS_CHANGED_DATETIME", "CLHS_LIMITOLD", "CLHS_LIMITNEW"],
        }
        create_indexes(index_definitions, conn)

        cursor.execute(params['idx_invo_debcno_invdate_etc'])
        cursor.execute(params['idx_invo_debcno_invdate'])
        cursor.execute(params['idx_summary_days_ids_dzien'])
        cursor.execute(params['idx_ext_days_ids_dzien'])
        cursor.execute(params['idx_clhs_joined_keys_datetime'])
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_aggregated_data_temp_all ON aggregated_data_temp (DZIEN, DZIS_DZIEN_TYG, DZIS_SOBOTA, DZIS_NIEDZIELA);')
        conn.commit()
        
        # indeksy jawnie z configu: idx_*
        for key, sql in params.items():
            if not key.startswith("idx_"):
                continue
            try:
                exec_sql(conn, sql, key)
            except RuntimeError as e:
                print(f"[WARN] Pomijam {key}: {e}")

        # kilka jawnych indeksów, jeśli tabele istnieją
        if table_exists(cursor, "dataset"):
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dataset_inv_day ON dataset (INVO_NO, INVO_CLNTNO, DZIEN DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dataset_clear ON dataset (INVO_CLNTNO, INVO_FINALPAYMENTDATE);")

        if table_exists(cursor, "summary_client_days_tab"):
            cursor.execute("CREATE INDEX IF NOT EXISTS summary_client_days_tab_idx_one_for_all ON summary_client_days_tab (ROK, MIESIAC);")

        if table_exists(cursor, "group_cust_inv_days"):
            cursor.execute("CREATE INDEX IF NOT EXISTS group_cust_inv_days_idx_year_month ON group_cust_inv_days (ROK, MIESIAC);")

        if table_exists(cursor, "invo"):
            cursor.execute("CREATE INDEX IF NOT EXISTS invo_idx_one_for_all ON invo (INVO_ADMNO, INVO_DEBH_NO, INVO_DEBC_NO, INVO_INVDATE);")

        if table_exists(cursor, "first_model_tab"):
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_first_model_tab_runid ON first_model_tab (RunIdentifier);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_first_model_tab_client_day ON first_model_tab (INVO_CLNTNO, DZIEN);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_first_model_tab_invoice ON first_model_tab (INVO_NO);")

        conn.commit()

        print(f"[OK] Database schema initialized: {args.db_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
