import argparse
import csv
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
def exec_sql_file(conn: sqlite3.Connection, path: str) -> None:
    sql = load_sql_file(path)
    conn.executescript(sql)
def exec_sql_dir(conn: sqlite3.Connection, dir_path: str) -> None:
    sql_dir = Path(dir_path)
    if not sql_dir.exists():
        raise FileNotFoundError(f"SQL directory not found: {dir_path}")
    for path in sorted(sql_dir.glob("*.sql")):
        print(f"[INFO] Executing {path.name}")
        exec_sql_file(conn, str(path))
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
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
def seed_swieta_from_csv(cursor: sqlite3.Cursor, csv_path: str) -> None:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file with holidays not found: {csv_path}")
    required_columns = ["nazwa", "rodzaj", "data", "rok", "mies", "dzien", "dzien_tyg"]
    inserted = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        if reader.fieldnames is None:
            raise RuntimeError(f"CSV has no header: {csv_path}")
        missing = [c for c in required_columns if c not in reader.fieldnames]
        if missing:
            raise RuntimeError(
                f"CSV {csv_path} does not contain required columns: {missing}. "
                f"Found: {reader.fieldnames}"
            )
        for row in reader:
            values = (
                row["nazwa"],
                row["rodzaj"],
                row["data"],
                int(row["rok"]) if row["rok"] not in (None, "") else None,
                int(row["mies"]) if row["mies"] not in (None, "") else None,
                int(row["dzien"]) if row["dzien"] not in (None, "") else None,
                row["dzien_tyg"],
            )
            cursor.execute(
                """
                SELECT 1
                FROM swieta
                WHERE nazwa = ?
                  AND rodzaj = ?
                  AND data = ?
                  AND rok = ?
                  AND mies = ?
                  AND dzien = ?
                  AND dzien_tyg = ?
                LIMIT 1
                """,
                values,
            )
            exists = cursor.fetchone() is not None
            if not exists:
                cursor.execute(
                    """
                    INSERT INTO swieta (nazwa, rodzaj, data, rok, mies, dzien, dzien_tyg)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                inserted += 1
    print(f"[OK] Seed swieta z CSV zakończony. Dodano {inserted} nowych rekordów.")
def main() -> None:
    parser = argparse.ArgumentParser(description="SQLite DB schema initialization")
    parser.add_argument("--config", required=True)
    parser.add_argument("--db-path", required=True)
    args = parser.parse_args()
    if os.path.exists(args.db_path):
        print(f"[ERROR] Baza już istnieje: {args.db_path}")
        sys.exit(1)
    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)
    cfg = load_config(args.config)
    
    common = cfg["common_params"]
    sql_paths = common["sql_paths"]
    
    schema_dir = sql_paths["schema_dir"]
    views_dir = sql_paths["views_dir"]
    indexes_dir = sql_paths["indexes_dir"]
    # możesz trzymać też to w configu jako data_paths["swieta_csv"]
    swieta_csv_path = cfg.get("data_paths", {}).get("swieta_csv", "data/swieta.csv")
    conn = sqlite3.connect(args.db_path)
    cursor = conn.cursor()
    try:
        # 1. Schema
        exec_sql_dir(conn, schema_dir)
        conn.commit()
        # 2. Seed swieta z CSV
        if table_exists(cursor, "swieta"):
            seed_swieta_from_csv(cursor, swieta_csv_path)
            conn.commit()
        else:
            print("[WARN] Tabela 'swieta' nie istnieje po wykonaniu schema.")
        # 3. Views
        exec_sql_dir(conn, views_dir)
        conn.commit()
        # 4. Indexes
        exec_sql_dir(conn, indexes_dir)
        conn.commit()
        print(f"[OK] Database schema initialized: {args.db_path}")
    finally:
        conn.close()
if __name__ == "__main__":
    main()
