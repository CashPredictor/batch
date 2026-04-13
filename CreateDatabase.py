import os
import importlib
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date
import time
import numpy as np
import yaml
import glob
import logging
import re
from first_model import get_modulo_condition
import sys

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

import warnings
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated"
)

EXCEL_ORIGIN = "1899-12-30"  # standard dla pandas (Excel 1900 system)

def _coerce_excel_date_series(s: pd.Series) -> pd.Series:
    """
    Konwertuje serię, która może zawierać:
    - excel seriale (float/int lub "45911.25" jako string) -> datetime
    - normalne stringi dat -> datetime
    - braki / śmieci -> NaT
    """
    if s is None:
        return s

    # Zrób kopię
    s2 = s.copy()

    # 1) spróbuj wyciągnąć liczby (seriale)
    num = pd.to_numeric(s2, errors="coerce")

    # seriale: w tym miejscu nie musimy strzelać widełkami,
    # bo i tak robimy to TYLKO dla kolumn z konfiguracji.
    mask_num = num.notna()

    out = pd.Series(pd.NaT, index=s2.index, dtype="datetime64[ns]")

    # Excel serial -> datetime (część dziesiętna to czas)
    if mask_num.any():
        out.loc[mask_num] = pd.to_datetime(
            num.loc[mask_num],
            unit="D",
            origin=EXCEL_ORIGIN,
            errors="coerce"
        )

    # 2) reszta -> klasyczny parser dat
    mask_other = ~mask_num
    if mask_other.any():
        out.loc[mask_other] = pd.to_datetime(s2.loc[mask_other], errors="coerce")

    return out


def normalize_excel_date_columns(
    df: pd.DataFrame,
    date_cols: list[str] | None
) -> pd.DataFrame:
    """
    Jeśli date_cols jest puste / None -> nic nie rób.
    Dla każdej kolumny z listy: konwersja excel serial / string -> datetime.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    if not date_cols:  # None lub [] lub ""
        return df

    for col in date_cols:
        if col in df.columns:
            df[col] = _coerce_excel_date_series(df[col])

    return df


def apply_excel_date_config(
    dfs_by_name: dict[str, pd.DataFrame],
    params: dict
) -> dict[str, pd.DataFrame]:
    """
    Czyta params['excel_date_columns'] (mapa: tabela -> lista kolumn)
    i stosuje konwersję tylko tam, gdzie jest definicja.
    Jeśli brak definicji -> pomija.
    """
    mapping = (params or {}).get("excel_date_columns")
    if not mapping or not isinstance(mapping, dict):
        return dfs_by_name  # brak konfiguracji => nic nie robimy

    for table_name, df in dfs_by_name.items():
        cols = mapping.get(table_name)
        if cols:  # tylko jeśli lista niepusta
            dfs_by_name[table_name] = normalize_excel_date_columns(df, cols)

    return dfs_by_name

def load_excel_sheets(file_path, params=None):
    sheet_names = ['INVO', 'DEBC', 'CLHS', 'DCMO']
    dfs = pd.read_excel(file_path, sheet_name=sheet_names)

    # Uporządkuj do dict (żeby łatwo mapować po nazwie tabeli)
    dfs_by_name = {
        'INVO': dfs.get('INVO'),
        'DEBC': dfs.get('DEBC'),
        'CLHS': dfs.get('CLHS'),
        'DCMO': dfs.get('DCMO'),
    }

    # Konwersja dat wg konfiguracji (jeśli brak/ puste -> nic nie robi)
    dfs_by_name = apply_excel_date_config(dfs_by_name, params or {})

    return dfs_by_name['INVO'], dfs_by_name['DEBC'], dfs_by_name['CLHS'], dfs_by_name['DCMO']

# Funkcja do pobrania definicji tabeli dataset z bazy danych
def get_table_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(dataset);")
    schema = cursor.fetchall()
    conn.close()
    return schema

def migrate_data(data_start, data_end, params, field_types, db_path, conn_new):
    conn = sqlite3.connect(db_path)
    print("db_path",db_path)
    query_template = params['sql_query_test']
    query = query_template.format(data_start=data_start, data_end=data_end)
    count_query = query.replace("SELECT *", "SELECT COUNT(*) as row_count")

    count_df = pd.read_sql_query(count_query, conn)
    row_count = count_df['row_count'].iloc[0]

    divisor, condition_sql = get_modulo_condition(row_count, limit=params['rows_limit'], invo_no_col=params['invoice_id'])
    if divisor is not None:
        query = f"SELECT * FROM dataset WHERE 1=1 {condition_sql};"
        print(f"[INFO] row_count={row_count} > {params['rows_limit']}, divisor={divisor}, dodajemy {condition_sql}")

    # Wczytujemy dane partiami po 500000 rekordów
    for chunk in pd.read_sql_query(query, conn, chunksize=params['data_limit']):
        # Konwersja kolumn float64 na float32 (oszczędność pamięci)
        float_cols = chunk.select_dtypes(include=['float64']).columns
        if len(float_cols) > 0:
            chunk[float_cols] = chunk[float_cols].astype('float32')
        # Uzupełniamy brakujące kolumny zgodnie z field_types
        for col in field_types.keys():
            if col not in chunk.columns:
                chunk[col] = None
        # Uporządkujemy kolumny zgodnie z kolejnością zadeklarowaną w field_types
        chunk = chunk[list(field_types.keys())]
        # Zapisujemy partię do nowej bazy
        chunk.to_sql('dataset', conn_new, if_exists='append', index=False)
    conn.close()

def CreateOneDbForAll(train_start, train_end, params, t0):
    data_end_str = _to_date_str(t0)
    # Pobranie wszystkich definicji tabel
    schemas = {}
    print("params['train_client_id']",params['train_client_id'])
    for client_id in params['train_client_id']:
        db_path = fr'databases\2026-03-02\2026-03-02\CashPredictorT_{client_id}.db'
        if os.path.exists(db_path):
            schemas[client_id] = get_table_schema(db_path)
        else:
            print(f"Plik bazy danych {db_path} nie istnieje, pomijam.")
    
    # Zbudowanie ujednoliconej definicji tabeli
    field_types = {}
    for schema in schemas.values():
        for col in schema:
            col_name, col_type = col[1], col[2]  # Nazwa kolumny i jej typ
            if col_name in field_types:
                if field_types[col_name] != col_type:  # Jeśli typy się różnią, zamieniamy na NUMBER
                    field_types[col_name] = "NUMBER"
            else:
                field_types[col_name] = col_type
    
    # Tworzenie nowej tabeli w nowej bazie
    # 30122025 START dla 100 klientów nie da się przekazać tak długiej nazwy
    # conn_new = sqlite3.connect(params['database'])
    conn_new = sqlite3.connect('C:/Users/OE00SG/CashPredictor/dev3/databases/2026-03-02/2026-03-02/CashPredictorT_ALL.db')
    # 30122025 STOP
    logging.debug(f"[CreateOneDbForAll] Newly opened connection id={id(conn_new)} for {params['database']}")
    conn_new.execute("PRAGMA journal_mode=WAL;")
    conn_new.execute("PRAGMA busy_timeout=60000;")
    cursor_new = conn_new.cursor()
     
    # Przenoszenie danych z każdej bazy
    for client_id in params['train_client_id']:
        db_path = fr'databases\2026-03-02\2026-03-02\CashPredictorT_{client_id}.db'
        if os.path.exists(db_path):
            migrate_data(train_start, train_end, params, field_types, db_path, conn_new)

    create_aggregated_data_temp(conn_new, data_end_str)
    
    conn_new.commit()
    conn_new.close()
    
    print(f"Nowa baza danych {params['database']} została utworzona i wypełniona danymi.")

def _to_date_str(d) -> str:
    """Zwraca 'YYYY-MM-DD' niezależnie czy d to date/datetime/str."""
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    # string lub coś innego: spróbuj przyciąć do 10 znaków (YYYY-MM-DD)
    s = str(d)
    return s[:10]

# Funkcja generująca wiersze
def generate_dates(start_date, end_date):
    rows = []
    
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date:
        dzien = current_date.strftime("%Y-%m-%d")
        dzis_dzien_tyg = current_date.isoweekday()  # 1 = poniedziałek, 7 = niedziela
        dzis_sobota = 1 if dzis_dzien_tyg == 6 else 0
        dzis_niedziela = 1 if dzis_dzien_tyg == 7 else 0

        rows.append((dzien, dzis_dzien_tyg, dzis_sobota, dzis_niedziela))
        current_date += timedelta(days=1)

    return rows

# Funkcja do pobrania schematu kolumn z bazy danych
def get_column_types(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor.fetchall()
    return {col[1]: col[2] for col in columns_info}  # {column_name: data_type}

def remove_case_insensitive_duplicate_columns(df):
    """
    Usuwa powielone kolumny w DataFrame, ignorując różnice w wielkości liter, zachowując pierwsze wystąpienie.
    
    :param df: Pandas DataFrame
    :return: DataFrame z unikalnymi kolumnami
    """
    # Tworzymy mapę nazw kolumn w małych literach do ich pierwszego wystąpienia
    lowercase_columns_map = {}
    columns_to_drop = []

    for col in df.columns:
        col_lower = col.lower()
        if col_lower not in lowercase_columns_map:
            lowercase_columns_map[col_lower] = col
        else:
            # Jeśli nazwa już występuje w innej wersji (case-insensitive), dodajemy do usunięcia
            columns_to_drop.append(col)
    
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)

    return df

def create_aggregated_data_temp(conn, data_end, data_start="2023-03-12"):
    cursor = conn.cursor()

    rows_to_insert = generate_dates(data_start, data_end)

    insert_query = """
        INSERT OR IGNORE INTO aggregated_data_temp
        (DZIEN, DZIS_DZIEN_TYG, DZIS_SOBOTA, DZIS_NIEDZIELA)
        VALUES (?, ?, ?, ?)
    """
    cursor.executemany(insert_query, rows_to_insert)
    conn.commit()

    print(f"Próbowano wstawić {len(rows_to_insert)} wierszy do aggregated_data_temp (duplikaty pominięte).")

def sanitize_column_names(df):
    """
    Usuwa niechciane znaki z nazw kolumn w DataFrame, aby były zgodne z wymogami SQLite.
    Zamienia każdy znak nie-alfanumeryczny (poza _) na znak '_'.
    """
    new_columns = []
    for col in df.columns:
        # np. re.sub('[^A-Za-z0-9_]+', '', col) – usunąć w ogóle
        # Lub re.sub('[^A-Za-z0-9_]+', '_', col) – zastąpić znakami podkreślenia
        safe_col = re.sub(r"[^A-Za-z0-9_]+", "_", col)  
        new_columns.append(safe_col)
    
    df.columns = new_columns
    return df

def refill_dataset_for_client(
    conn,
    client_id,
    source_table="dataset_tab",
    target_table="dataset",
    batch_days=31
):
    cursor = conn.cursor()

    cursor.execute(f"DELETE FROM {target_table} WHERE INVO_CLNTNO = ?", (client_id,))
    conn.commit()

    cursor.execute(f"""
        SELECT MIN(DATE(DZIEN)), MAX(DATE(DZIEN))
        FROM {source_table}
        WHERE INVO_CLNTNO = ?
    """, (client_id,))
    min_date, max_date = cursor.fetchone()

    if not min_date or not max_date:
        print(f"[INFO] Brak danych dla klienta {client_id} w {source_table}.")
        return

    cursor.execute(f"PRAGMA table_info({source_table});")
    columns_info_raw = cursor.fetchall()
    columns_info = [(col[1], col[2] if col[2] else "TEXT") for col in columns_info_raw]

    current_date = pd.to_datetime(min_date)
    end_date = pd.to_datetime(max_date)

    total_inserted = 0

    while current_date <= end_date:
        next_date = current_date + pd.Timedelta(days=batch_days - 1)

        chunk_df = pd.read_sql_query(
            f"""
            SELECT *
            FROM {source_table}
            WHERE INVO_CLNTNO = ?
              AND DATE(DZIEN) >= DATE(?)
              AND DATE(DZIEN) <= DATE(?)
            ORDER BY DZIEN, INVO_NO
            """,
            conn,
            params=(client_id, current_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d"))
        )

        if not chunk_df.empty:
            processed_df = process_dataset_tab(chunk_df, columns_info)
            processed_df = remove_case_insensitive_duplicate_columns(processed_df)
            processed_df = sanitize_column_names(processed_df)

            float_cols = processed_df.select_dtypes(include=[np.float64, np.float32]).columns
            if len(float_cols) > 0:
                processed_df[float_cols] = processed_df[float_cols].round(4)
                for c in float_cols:
                    processed_df[c] = processed_df[c].astype("float32")

            processed_df = processed_df.where(pd.notna(processed_df), None)

            placeholders = ", ".join(["?" for _ in processed_df.columns])
            insert_sql = f"INSERT INTO {target_table} VALUES ({placeholders})"

            def _to_sqlite_safe(v):
                if pd.isna(v):
                    return None
                if isinstance(v, np.datetime64):
                    v = pd.to_datetime(v)
                if isinstance(v, pd.Timestamp):
                    return v.to_pydatetime()
                return v

            rows = [
                tuple(_to_sqlite_safe(v) for v in row)
                for row in processed_df.itertuples(index=False, name=None)
            ]

            cursor.executemany(insert_sql, rows)
            conn.commit()

            inserted_now = len(rows)
            total_inserted += inserted_now
            print(
                f"[INFO] dataset: client={client_id}, "
                f"{current_date.strftime('%Y-%m-%d')}..{next_date.strftime('%Y-%m-%d')}, "
                f"inserted={inserted_now}, total={total_inserted}"
            )

        current_date = next_date + pd.Timedelta(days=1)

    print(f"[OK] dataset rebuilt for client {client_id}, total rows inserted: {total_inserted}")

def validate_and_convert_dates(df, columns):
    invalid_values = []

    for col in columns:
        try:
            # Próba konwersji na datetime
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except Exception as e:
            # Przechwycenie błędów
            invalid_values.append((col, df[col].iloc[0], str(e)))

    return invalid_values

def process_dataset_tab(df, columns):
    # 1. Wyodrębnienie kolumn identyfikujących
    id_columns = ['INVO_NO', 'INVO_ADMNO', 'INVO_CLNTNO', 'INVO_DEBH_NO', 'INVO_DEBC_NO', 'INVO_INVDATE', 'DZIEN', 'INVO_DUEDATE', 'INVO_FINALPAYMENTDATE']

    # 2. Wybór kolumn liczbowych
    numeric_columns = [col for col, dtype in columns if dtype in ['NUM', 'REAL', 'INT']]
    numeric_df = df[numeric_columns]

    # 3. Walidacja kolumn datowych
    date_columns = [col for col, dtype in columns if dtype == 'DATETIME']
    invalid_values = validate_and_convert_dates(df, date_columns)

    # Wyświetlenie problematycznych wartości
    if invalid_values:
        print("Problematyczne wartości w kolumnach datowych:")
        for col, value, error in invalid_values:
            print(f"Kolumna: {col}, Wartość: {value}, Błąd: {error}")    

    # --- cache konwersji dat: raz i korzystamy niżej ---
    dt = {col: pd.to_datetime(df[col], errors='coerce') for col in date_columns if col in df.columns}

    # 5. „Przycięcie” pól zależnych (bez zmian u Ciebie)
    df['CLHS_NEXT_CHANGED_DATETIME'] = df['CLHS_NEXT_CHANGED_DATETIME'].where(
        pd.to_datetime(df['CLHS_NEXT_CHANGED_DATETIME']) > pd.to_datetime(df['DZIEN']),
        None
    )    
    df['INVO_MARKCODESPECDATE'] = df['INVO_MARKCODESPECDATE'].where(
        pd.to_datetime(df['INVO_MARKCODESPECDATE']) > pd.to_datetime(df['DZIEN']),
        None
    )
    df['INVO_MARKCODESPEC'] = df['INVO_MARKCODESPEC'].where(
        pd.to_datetime(df['INVO_MARKCODESPECDATE']) > pd.to_datetime(df['DZIEN']),
        None
    )

    # 4.1 Różnice dni względem INVO_INVDATE (jak u Ciebie, ale na cache dt)
    base_cols = [col for col, dtype in columns
                 if dtype == 'DATETIME' and col not in ('INVO_INVDATE', 'INVO_FINALPAYMENTDATE')]
    date_diff_df = pd.DataFrame(index=df.index)
    if 'INVO_INVDATE' in dt:
        invdate_ser = dt['INVO_INVDATE']
        for col in base_cols:
            if col in dt:
                date_diff_df[f'{col}_DAYS_DIFF'] = (dt[col] - invdate_ser).dt.days

        if 'INVO_FINALPAYMENTDATE' in dt:
            date_diff_df['INVO_FINALPAYMENTDATE_DAYS_DIFF'] = (dt['INVO_FINALPAYMENTDATE'] - invdate_ser).dt.days

        if 'DZIEN' in dt:
            date_diff_df['FROM_DZIEN_TO_INVO_INVDATE'] = (dt['DZIEN'] - invdate_ser).dt.days

    date_diff_df = date_diff_df.fillna(0).round(0).astype('int', copy=False)

    # 4.2 NOWE: różnice dni względem INVO_DUEDATE
    due_date_diff_df = pd.DataFrame(index=df.index)
    if 'INVO_DUEDATE' in dt:
        duedate_ser = dt['INVO_DUEDATE']
        due_cols = [col for col, dtype in columns
                    if dtype == 'DATETIME' and col not in ('INVO_DUEDATE', 'INVO_FINALPAYMENTDATE')]
        for col in due_cols:
            if col in dt:
                due_date_diff_df[f'{col}_DUE_DAYS_DIFF'] = (dt[col] - duedate_ser).dt.days

        if 'INVO_FINALPAYMENTDATE' in dt:
            due_date_diff_df['INVO_FINALPAYMENTDATE_DUE_DAYS_DIFF'] = (dt['INVO_FINALPAYMENTDATE'] - duedate_ser).dt.days

        if 'DZIEN' in dt:
            due_date_diff_df['FROM_DZIEN_TO_INVO_DUEDATE'] = (dt['DZIEN'] - duedate_ser).dt.days

    # jeżeli nie było DUEDATE, to due_date_diff_df zostanie pusty – to OK przy concat
    if not due_date_diff_df.empty:
        due_date_diff_df = due_date_diff_df.fillna(0).round(0).astype('int', copy=False)

    # 6. Transformacja wybranych dat (na cache dt, tylko te które mamy)
    def transform_date_column(date_column):
        transformed_df = pd.DataFrame(index=df.index)
        if date_column not in dt:
            return transformed_df  # brak kolumny – zwracamy pusty DF
        ser = dt[date_column]
        transformed_df[f'{date_column}_YEAR'] = ser.dt.year
        transformed_df[f'{date_column}_MONTH'] = ser.dt.month
        transformed_df[f'{date_column}_DAY'] = ser.dt.day
        transformed_df[f'{date_column}_WEEKDAY'] = ser.dt.weekday
        transformed_df[f'{date_column}_DAYS_FROM_YEAR_START'] = ser.dt.dayofyear
        transformed_df[f'{date_column}_DAYS_TO_YEAR_END'] = 365 - ser.dt.dayofyear
        transformed_df[f'{date_column}_DAYS_FROM_MONTH_START'] = ser.dt.day
        transformed_df[f'{date_column}_DAYS_TO_MONTH_END'] = ser.dt.days_in_month - ser.dt.day
        if date_column in ('INVO_DUEDATE', 'DZIEN'):
            transformed_df[f'{date_column}_IS_SATURDAY'] = (ser.dt.weekday == 5).astype(int)
            transformed_df[f'{date_column}_IS_SUNDAY'] = (ser.dt.weekday == 6).astype(int)
        return transformed_df

    transformed_dates_df = pd.concat(
        [transform_date_column(col) for col in
         ['DZIEN', 'INVO_INVDATE', 'INVO_DUEDATE', 'CLHS_CHANGED_DATETIME', 'CLHS_NEXT_CHANGED_DATETIME']],
        axis=1
    ).fillna(0).round(0).astype('int', copy=False)

    # 7. Końcowe kolumny płatności
    has_final = 'INVO_FINALPAYMENTDATE' in df.columns
    if has_final:
        final_payment_date = df['INVO_FINALPAYMENTDATE']
        final_payment_date_diff = date_diff_df['INVO_FINALPAYMENTDATE_DAYS_DIFF'] if 'INVO_FINALPAYMENTDATE_DAYS_DIFF' in date_diff_df.columns else None
        if 'INVO_FINALPAYMENTDATE_DAYS_DIFF' in date_diff_df.columns:
            date_diff_df = date_diff_df.drop(columns=['INVO_FINALPAYMENTDATE_DAYS_DIFF'])
        final_payment_due_date_diff = (
            due_date_diff_df['INVO_FINALPAYMENTDATE_DUE_DAYS_DIFF']
            if 'INVO_FINALPAYMENTDATE_DUE_DAYS_DIFF' in due_date_diff_df.columns else None
        )
        if 'INVO_FINALPAYMENTDATE_DUE_DAYS_DIFF' in due_date_diff_df.columns:
            due_date_diff_df = due_date_diff_df.drop(columns=['INVO_FINALPAYMENTDATE_DUE_DAYS_DIFF'])

    # 8. Sklejenie wszystkiego
    processed_df = pd.concat(
        [df[id_columns], transformed_dates_df, date_diff_df, due_date_diff_df, numeric_df],
        axis=1
    )
    if has_final:
        processed_df['INVO_FINALPAYMENTDATE'] = final_payment_date
    
    inv = pd.to_datetime(processed_df['INVO_INVDATE'], errors='coerce') \
          if 'INVO_INVDATE' in processed_df.columns else pd.Series(pd.NaT, index=processed_df.index)
    
    fin = pd.to_datetime(processed_df['INVO_FINALPAYMENTDATE'], errors='coerce') \
          if 'INVO_FINALPAYMENTDATE' in processed_df.columns else pd.Series(pd.NaT, index=processed_df.index)
    
    processed_df['INVO_FINALPAYMENTDATE_DAYS_DIFF'] = (fin - inv).dt.days.astype('float32')

    return processed_df

# Funkcja symulująca najbliższy dzień roboczy
def get_next_working_day(termin_platnosci, holidays):
    while termin_platnosci.weekday() >= 5 or termin_platnosci in holidays:
        termin_platnosci += timedelta(days=1)
    return termin_platnosci

# Funkcja do wstawienia danych z widoku do tabeli
def insert_data_from_view(cursor, view_name, table_name):
    insert_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM {view_name};
    """
    cursor.execute(insert_query)
    print(f"Dane zostały skopiowane z widoku '{view_name}' do tabeli '{table_name}'.")


def generate_days(start_date, end_date):
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    while current_date <= end_date:
        yield current_date  # zwracamy datetime
        current_date += timedelta(days=1)
        
# Adapter: datetime → string (dla SQLite)
def adapt_datetime(ts):
    return ts.strftime('%Y-%m-%d %H:%M:%S')  # Format akceptowany przez SQLite

# Konwerter: string → datetime (z SQLite)
def convert_datetime(ts):
    return datetime.strptime(ts.decode('utf-8'), '%Y-%m-%d %H:%M:%S')

def adapt_pandas_timestamp(ts):
    return ts.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')

def convert_pandas_timestamp(ts):
    return pd.Timestamp(ts.decode('utf-8'))

# Rejestracja adapterów
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter('DATETIME', convert_datetime)
sqlite3.register_adapter(pd.Timestamp, adapt_pandas_timestamp)
sqlite3.register_converter('DATETIME', convert_pandas_timestamp)

# Funkcja do pobrania listy kolumn z widoku
def get_columns_from_view(cursor, view_name):
    cursor.execute(f"PRAGMA table_info({view_name});")
    columns_info = cursor.fetchall()
    return [col[1] for col in columns_info]  # Pobieramy nazwy kolumn

# Funkcja do pobierania kolumn z widoku
def get_columns(cursor, view_name):
    cursor.execute(f"PRAGMA table_info({view_name});")
    return [(col[1], col[2]) for col in cursor.fetchall()]

def CreateDatabase(params, t0, create_many_dbs_from_one_excel=False, skip_raw_tables=False, build_derived=True):
    # skip_raw_tables=False, build_derived=True -> NORMAL (RAW+DERIVED)
    # skip_raw_tables=False, build_derived=False -> ETAP 1 (RAW only)
    # skip_raw_tables=True,  build_derived=True -> ETAP 2 (DERIVED only)    
    data_end_str = _to_date_str(t0)
    if create_many_dbs_from_one_excel:
        # --- jeden plik z wieloma klientami ---
        file_path = params['multi_client_excel_path']
        df_invo_all, df_debc_all, df_clhs_all, df_dcmo_all = load_excel_sheets(file_path, params)

        print("przed client_ids = df_invo_all['INVO_CLNTNO'].dropna().unique()")
        # ------------------------------------------------------------------
        # 1️⃣ wszyscy klienci z Excela (jak dotychczas)
        # ------------------------------------------------------------------
        all_client_ids = (
            df_invo_all['INVO_CLNTNO']
            .dropna()
            .unique()
            .tolist()
        )
        
        # ------------------------------------------------------------------
        # 2️⃣ liczenie i sortowanie (TYLKO do logu + kolejności)
        # ------------------------------------------------------------------
        _counts = (
            df_invo_all
            .dropna(subset=['INVO_CLNTNO'])
            .groupby('INVO_CLNTNO', as_index=False)
            .size()
            .rename(columns={'size': 'cnt'})
        )
        
        _counts_sorted = _counts.sort_values('cnt', ascending=True)
        
        print("Kolejność przetwarzania (od najmniejszej liczby wierszy INVO):")
        for rank, (cid, cnt) in enumerate(
            zip(_counts_sorted['INVO_CLNTNO'], _counts_sorted['cnt']),
            start=1
        ):
            print(f"{rank:>2}. klient {int(cid)} → {int(cnt)} wierszy INVO")
        
        # ------------------------------------------------------------------
        # 3️⃣ zawężenie listy kategorii (NOWA LOGIKA)
        # ------------------------------------------------------------------
        client_ids = all_client_ids
        
        # 3a) jeżeli podano klientów (CLI / override config)
        explicit_clients = params.get("train_client_id")
        if explicit_clients:
            explicit_clients = set(explicit_clients)
            client_ids = [cid for cid in client_ids if cid in explicit_clients]
        
        # 3b) usunięcie exclude_ids (jak wcześniej)
        exclude_ids = set(params.get("exclude_ids", []))
        client_ids = [cid for cid in client_ids if cid not in exclude_ids]
        
        # ------------------------------------------------------------------
        # 4️⃣ zachowanie kolejności wynikającej z liczby rekordów
        # ------------------------------------------------------------------
        client_ids = [
            cid for cid in _counts_sorted['INVO_CLNTNO']
            if cid in client_ids
        ]
        
        print(f"[INFO] Finalna lista klientów do przetworzenia: {client_ids}")
        
        # czytelny wydruk: kolejność + ile rekordów ma każdy klient
        print("Kolejność przetwarzania (od najmniejszej liczby wierszy INVO):")
        for rank, (cid, cnt) in enumerate(zip(_counts_sorted['INVO_CLNTNO'],
                                              _counts_sorted['cnt']), start=1):
            print(f"{rank:>2}. klient {int(cid)} → {int(cnt)} wierszy INVO")     
        print("po client_ids = df_invo_all['INVO_CLNTNO'].dropna().unique() order by _counts.sort_values('cnt', ascending=True)['INVO_CLNTNO']")

        files_for_client = None  # w tym trybie niepotrzebne
    else:
        # --- wiele plików: po jednym Excelu na klienta ---
        suffix = params["train_client_id"]

        # Ujednolicenie do listy
        if isinstance(suffix, (list, tuple, np.ndarray)):
            client_ids = list(suffix)
        else:
            client_ids = [suffix]

        # znajdź pliki per‑klient
        files_for_client = {}
        for cid in client_ids:
            matches = glob.glob(
                fr"C:\Users\OE00SG\CashPredictor\dev3\co i jak baza\Bartek_*_{cid}.xlsx"
            )
            if not matches:
                raise FileNotFoundError(
                    fr"Nie znaleziono pliku dopasowanego do wzorca: 'Bartek_*_{cid}.xlsx'"
                )
            files_for_client[cid] = matches[0]

        # jeśli to był tylko jeden klient → możesz od razu wczytać,
        # ale i tak wczytamy w pętli per‑klient (patrz niżej),
        # żeby obsłużyć także przypadek wielu klientów.
        df_invo_all = df_debc_all = df_clhs_all = df_dcmo_all = None
        
    for client_id in client_ids:
        print("client_id: ", client_id)
    
        # wczytujemy dane:
        if create_many_dbs_from_one_excel:
            # już wczytane wyżej do *_all
            df_invo = df_invo_all[df_invo_all['INVO_CLNTNO'] == client_id].copy()
            df_debc = df_debc_all[df_debc_all['DEBC_CLNTNO'] == client_id].copy()
            df_clhs = df_clhs_all[df_clhs_all['CLHS_CLNTNO'] == client_id].copy()
            df_dcmo = df_dcmo_all[df_dcmo_all['DEBC_CLNTNO'] == client_id].copy()  # jeśli tak masz w danych
        else:
            # osobny plik dla każdego klienta
            file_path = files_for_client[client_id]
            df_invo, df_debc, df_clhs, df_dcmo = load_excel_sheets(file_path, params)
    
        db_path = params['database_template'].format(client_id=client_id)
        conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cursor = conn.cursor()
    
        od_dnia = pd.to_datetime(params['od_dnia'])
        dcmo_year = int(od_dnia.year)
        dcmo_month = int(od_dnia.month)
    
        # Filtrowanie df_invo: INVO_INVDATE >= od_dnia
        if df_invo is not None and not df_invo.empty and 'INVO_INVDATE' in df_invo.columns:
            df_invo['INVO_INVDATE'] = pd.to_datetime(df_invo['INVO_INVDATE'], errors='coerce')
            df_invo = df_invo[df_invo['INVO_INVDATE'].notna() & (df_invo['INVO_INVDATE'] >= od_dnia)]
    
        # Filtrowanie df_clhs: CLHS_DATCHANGED >= od_dnia (format YYYYMMDD, np. 20210712)
        if df_clhs is not None and not df_clhs.empty and 'CLHS_DATCHANGED' in df_clhs.columns:
            df_clhs['CLHS_DATCHANGED'] = pd.to_datetime(df_clhs['CLHS_DATCHANGED'].astype(str), format='%Y%m%d', errors='coerce')
            df_clhs = df_clhs[df_clhs['CLHS_DATCHANGED'].notna() & (df_clhs['CLHS_DATCHANGED'] >= od_dnia)]
    
        # Filtrowanie df_dcmo: (YEAR, MONTH) >= od_dnia
        if df_dcmo is not None and not df_dcmo.empty and 'DCMO_YEAR' in df_dcmo.columns and 'DCMO_MONTH' in df_dcmo.columns:
            df_dcmo['DCMO_YEAR'] = pd.to_numeric(df_dcmo['DCMO_YEAR'], errors='coerce')
            df_dcmo['DCMO_MONTH'] = pd.to_numeric(df_dcmo['DCMO_MONTH'], errors='coerce')
        
            df_dcmo = df_dcmo[
                (df_dcmo['DCMO_YEAR'] > dcmo_year) |
                ((df_dcmo['DCMO_YEAR'] == dcmo_year) & (df_dcmo['DCMO_MONTH'] >= dcmo_month))
            ]
    
        # --- RAW tables: invo/debc/clhs/dcmo ---
        if not skip_raw_tables:
            cursor.execute("DELETE FROM invo WHERE INVO_CLNTNO = ?", (client_id,))
            cursor.execute("DELETE FROM debc WHERE DEBC_CLNTNO = ?", (client_id,))
            cursor.execute("DELETE FROM clhs WHERE CLHS_CLNTNO = ?", (client_id,))
            cursor.execute("DELETE FROM dcmo WHERE DEBC_CLNTNO = ?", (client_id,))
            
            df_invo.to_sql('invo', conn, if_exists='append', index=False)
            df_debc.to_sql('debc', conn, if_exists='append', index=False)
            df_clhs.to_sql('clhs', conn, if_exists='append', index=False)
            df_dcmo.to_sql('dcmo', conn, if_exists='append', index=False)
        
            conn.commit()
        else:
            logging.info("[CreateDatabase] skip_raw_tables=True -> nie ruszam invo/debc/clhs/dcmo")

        # --- stop after RAW if requested (ETAP 1) ---
        if not build_derived:
            logging.info("[CreateDatabase] build_derived=False -> kończę po imporcie RAW (bez widoków/datasetów).")
            conn.commit()
            conn.close()
            continue
        
    
        #tabela do świąt
        # Ścieżka do pliku Excel
        create_and_import_swieta(conn, params, file_path = r'C:\Users\OE00SG\CashPredictor\dev3\co i jak baza\Arkusz w C  Users OQ38TT jupiter dev3 co i jak baza co i jak baza.xls')
        
        # Wstawianie danych z widoku do tabeli
        cursor.execute("DELETE FROM INVO_CLHS_JOINED_TAB WHERE INVO_CLNTNO = ?", (client_id,))
        insert_query = """
        INSERT INTO INVO_CLHS_JOINED_TAB
        SELECT * FROM INVO_CLHS_JOINED
        WHERE INVO_CLNTNO = ?;
        """
        cursor.execute(insert_query, (client_id,))
        print("Dane zostały skopiowane z widoku do tabeli 'INVO_CLHS_JOINED_TAB'.")

        remove_duplicates_query = """
            WITH d AS (
              SELECT
                rowid AS rid,
                ROW_NUMBER() OVER (
                  PARTITION BY
                    INVO_NO, INVO_CLNTNO, INVO_ADMNO, INVO_DEBH_NO, INVO_DEBC_NO, CLHS_CHANGED_DATETIME
                  ORDER BY RANDOM()          -- losowy wybór zwycięzcy
                ) AS rn
              FROM INVO_CLHS_JOINED_TAB
            )
            DELETE FROM INVO_CLHS_JOINED_TAB
            WHERE rowid IN (SELECT rid FROM d WHERE rn > 1);
        """
        cursor.execute(remove_duplicates_query)        
        
        # Zatwierdzenie zmian i zamknięcie połączenia
        conn.commit()
        
        print("Operacja zakończona sukcesem.")
        
        # Pobranie danych z invo_view
        query_invo = f"""
        SELECT 
            INVO_NO,
            INVO_ADMNO,
            INVO_CLNTNO,
            INVO_DEBH_NO,
            INVO_DEBC_NO,
            INVO_INVDATE,
            INVO_FINALPAYMENTDATE
        FROM invo_view
        WHERE INVO_INVDATE IS NOT NULL
        and date(INVO_INVDATE) >= {params['od_dnia']}
        AND INVO_CLNTNO = {client_id};
        """
        cursor.execute(query_invo)
        rows = cursor.fetchall()
        
        # Dynamiczne wstawianie danych do tabeli z obliczeniem ROK i MIESIAC
        data_to_insert = []
        for row in rows:
            (invo_no, invo_admno, invo_clntno, invo_debh_no, invo_debc_no,
             invo_invdate, invo_finalpaymentdate) = row
        
            # 1) Sprowadź wszystko do str 'YYYY-MM-DD' (albo None)
            invdate_s = pd.to_datetime(invo_invdate, errors='coerce')
            if pd.isna(invdate_s):
                # Bez daty wystawienia nie ma sensu generować dni – pomiń rekord
                continue
            invdate_s = invdate_s.strftime("%Y-%m-%d")
        
            if invo_finalpaymentdate is not None and not pd.isna(invo_finalpaymentdate):
                finalpay_s = pd.to_datetime(invo_finalpaymentdate, errors='coerce')
                finalpay_s = None if pd.isna(finalpay_s) else finalpay_s.strftime("%Y-%m-%d")
            else:
                finalpay_s = None
        
            # 2) Jeśli brak finalnej daty płatności → użyj invdate + 120 dni
            end_for_days_s = (
                finalpay_s if finalpay_s
                else (pd.to_datetime(invdate_s) + pd.Timedelta(days=120)).strftime("%Y-%m-%d")
            )
        
            # 3) generate_days oczekuje STR → OK
            days = list(generate_days(invdate_s, end_for_days_s))
        
            # 4) Przy insertach do SQLite trzymaj typy proste (date/datetime → date)
            invdate_d  = pd.to_datetime(invdate_s).date()
            finalpay_d = (pd.to_datetime(finalpay_s).date() if finalpay_s else None)
        
            for day in days:
                # generate_days zwraca datetime → rzutujemy na date
                day_d = day.date()
                data_to_insert.append((
                    invo_no, invo_admno, invo_clntno, invo_debh_no, invo_debc_no,
                    invdate_d, day_d, day_d.year, day_d.month, finalpay_d
                ))
        
        # Wstawianie danych do tabeli
        cursor.execute("DELETE FROM group_cust_inv_days WHERE INVO_CLNTNO = ?", (client_id,))

        cursor.executemany("""
        INSERT INTO group_cust_inv_days (
            INVO_NO, INVO_ADMNO, INVO_CLNTNO, INVO_DEBH_NO, INVO_DEBC_NO, INVO_INVDATE, DZIEN, ROK, MIESIAC, INVO_FINALPAYMENTDATE
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data_to_insert)

        # Zatwierdzenie zmian
        conn.commit()
        
        print("Tabela 'group_cust_inv_days' została pomyślnie wypełniona.")
        print(datetime.now().strftime("%H:%M:%S"))
    
        # Pobranie danych z group_cust_inv_days z grupowaniem
        grouped_query = params['grouped_query']
        
        # Wczytanie danych do DataFrame
        df = pd.read_sql_query(grouped_query, conn, params=(client_id,))
        
        # Zapisanie DataFrame do tabeli SQLite (struktura tabeli tworzona automatycznie)
        cursor.execute("DELETE FROM grouped_client_days WHERE INVO_CLNTNO = ?", (client_id,))
        df.to_sql("grouped_client_days", conn, if_exists="append", index=False)

        # Zatwierdzenie zmian i zamknięcie połączenia
        conn.commit()
    
        # Definicja kolumn DCMO
        dcmo_columns = [
            "DCMO_PAIDAMOUNT", "DCMO_PAIDAMOUNTINVDATE", "DCMO_PAIDAMOUNTDUEDATE",
            "DCMO_PAIDAMOUNTBEFOREDUEDATE", "DCMO_PAIDAMOUNTAFTDUEDAT30", "DCMO_PAIDAMOUNTAFTDUEDAT60",
            "DCMO_PAIDAMOUNTAFTDUEDAT90", "DCMO_PAIDAMOUNTAFTDUEDAT120", "DCMO_PAIDAMOUNTAFTDUEDATG120",
            "DCMO_PAIDNINVBEFDUEDATE", "DCMO_PAIDNINVAFTDUEDATE30", "DCMO_PAIDNINVAFTDUEDATE60",
            "DCMO_PAIDNINVAFTDUEDATE90", "DCMO_PAIDNINVAFTDUEDATE120", "DCMO_PAIDNINVAFTDUEDATEG120",
            "DCMO_TOTASALESDEBTNOTES", "DCMO_TOTNDEBTNOTES", "DCMO_TOTASALESCREDNOTES",
            "DCMO_TOTNCREDNOTES", "DCMO_AVGBALANCEOUTSTANDING", "DCMO_BALANCEHI",
            "DCMO_NDUNNINGLETTERS", "DCMO_BALANCELO", "DCMO_BALANCE",
            "DCMO_NINVOICESOUTSTANDING", "DCMO_PAIDAMOUNTDISPUTES", "DCMO_PAIDAMOUNTPARTIALPAYM"
        ]
        offsets = [1, 2, 3, 6, 12]

        print("Rozpoczynam partiami wstawianie do 'extended_grouped_client_days_tab'...")
    
        # 1. Pobieramy listę (ROK, MIESIAC) występujących w widoku
        cursor.execute("""
            SELECT DISTINCT ROK, MIESIAC
            FROM extended_grouped_client_days
            ORDER BY ROK, MIESIAC
        """)
        year_month_rows = cursor.fetchall()

        cursor.execute("DELETE FROM extended_grouped_client_days_tab WHERE INVO_CLNTNO = ?", (client_id,))
        
        # 2. Iterujemy po każdej parze (ROK, MIESIAC)
        for (year, month) in year_month_rows:
            insert_sql = f"""
            INSERT INTO extended_grouped_client_days_tab
            SELECT *
            FROM extended_grouped_client_days
            WHERE ROK = {year}
              AND MIESIAC = {month}
              AND date(DZIEN) >= {params['od_dnia']}
              AND INVO_CLNTNO = {client_id};
            """
            cursor.execute(insert_sql)
            conn.commit()  # commit po każdej partii
        
            # Podgląd liczby wstawionych wierszy
            inserted_count = cursor.rowcount  # w SQLite rowcount czasem bywa -1, zależy od wersji
            print(f"Wstawiono partię: ROK={year}, MIESIAC={month} (rowcount={inserted_count})")
        
        print("Wszystkie partie zostały zaimportowane.")
        print(datetime.now().strftime("%H:%M:%S"))
        
        # Zatwierdzenie zmian
        conn.commit()
        print("Proces zakończony.")
        
        print("Rozpoczynam partiami wstawianie do 'summary_client_days_tab'...")
        
        # 3. Znajdujemy, od którego (ROK, MIESIAC) kontynuować.
        #    Szukamy w summary_client_days_tab największego ROK,MIESIAC wstawionego
        #    lub None, jeśli pusto.
        
        
        cursor.execute("""
            SELECT ROK, MIESIAC
            FROM summary_client_days_tab
            AND INVO_CLNTNO = ?
            ORDER BY ROK DESC, MIESIAC DESC
            LIMIT 1
        """, (client_id))
        last_row = cursor.fetchone()
        if last_row:
            (last_inserted_year, last_inserted_month) = last_row
            print(f"Ostatnio wstawiony miesiąc: ROK={last_inserted_year}, MIESIAC={last_inserted_month}")
        else:
            last_inserted_year, last_inserted_month = None, None
            print("Tabela summary_client_days_tab jest pusta. Zaczynam od początku.")
            print(datetime.now().strftime("%H:%M:%S"))
        
        # 4. Pobieramy listę (ROK, MIESIAC) z mniejszej tabeli, np. 'extended_grouped_client_days'
        #    (zamiast large view)
        cursor.execute("""
            SELECT DISTINCT ROK, MIESIAC
            FROM extended_grouped_client_days
            where INVO_CLNTNO = ?
            ORDER BY ROK, MIESIAC
        """, (client_id))
        all_rows = cursor.fetchall()
        
        inserted_count = 0
        cursor.execute("DELETE FROM summary_client_days_tab WHERE INVO_CLNTNO = ?", (client_id,))

        for (year, month) in all_rows:
            # Sprawdzamy, czy to (ROK, MIESIAC) jest > ostatnio wstawionego
            # Konwertujemy do "liczby" = ROK*12 + MIESIAC
            if last_inserted_year is not None and last_inserted_month is not None:
                last_val = last_inserted_year * 12 + last_inserted_month
                cur_val  = year * 12 + month
                
                # Jeśli cur_val <= last_val, to już wstawione -> pomijamy
                if cur_val <= last_val:
                    # Komentarz:
                    # print(f"Pomijam ROK={year}, MIESIAC={month}, bo <= {last_inserted_year},{last_inserted_month}")
                    continue
        
            # 5. Wstawiamy partię z widoku summary_client_days_view
            insert_sql = f"""
            INSERT INTO summary_client_days_tab
            SELECT *
            FROM summary_client_days_view
            WHERE ROK = {year}
              AND MIESIAC = {month}
              AND INVO_CLNTNO = {client_id};
            """
            cursor.execute(insert_sql)
            conn.commit()
            
            # W SQLite rowcount może być -1
            row_count = cursor.rowcount if cursor.rowcount != -1 else 0
            inserted_count += row_count
            print(f"Wstawiono ROK={year}, MIESIAC={month} -> {row_count} wierszy (narastająco={inserted_count})")
        
            # aktualizujemy last_inserted_year, last_inserted_month
            last_inserted_year, last_inserted_month = year, month
        
        print(f"Wszystkie partie zostały zaimportowane. Łącznie wstawiono (teraz) = {inserted_count}.")
        print(datetime.now().strftime("%H:%M:%S"))

        cursor.execute(params['idx_invo_debcno_invdate_etc'])
        cursor.execute(params['idx_invo_debcno_invdate'])
        cursor.execute(params['idx_summary_days_ids_dzien'])
        cursor.execute(params['idx_ext_days_ids_dzien'])
        cursor.execute(params['idx_clhs_joined_keys_datetime'])
        conn.commit()
        
        # Pobranie świąt (jeśli istnieją)
        cursor.execute("SELECT data FROM swieta")
        holidays = {pd.Timestamp(row[0]) for row in cursor.fetchall()}
        
        # Parametry wejściowe
        z_ilu_dni_values = [14, 30, 60, 90]
        maksymalna_data = pd.Timestamp(data_end_str)
        log_triggered = False
        inserted_count = 0

        cursor.execute("DELETE FROM statystyki_faktur WHERE INVO_CLNTNO = ?", (client_id,))
        for z_ilu_dni in z_ilu_dni_values:
            # Pobranie danych klientów i dni
            query = f"""
                SELECT DISTINCT INVO_ADMNO, INVO_CLNTNO, INVO_DEBH_NO, INVO_DEBC_NO, DZIEN
                FROM group_cust_inv_days
                WHERE DZIEN <= '{maksymalna_data.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            customer_days = pd.read_sql_query(query, conn)

            
            # Iteracja po klientach i dniach
            for _, row in customer_days.iterrows():
                invo_admno = row['INVO_ADMNO']
                invo_clntno = row['INVO_CLNTNO']
                invo_debh_no = row['INVO_DEBH_NO']
                invo_debc_no = row['INVO_DEBC_NO']
                na_dzien = pd.Timestamp(row['DZIEN'])
                
                end_day   = na_dzien.strftime('%Y-%m-%d')
                start_day = (na_dzien - pd.Timedelta(days=z_ilu_dni)).strftime('%Y-%m-%d')

                # Pobranie faktur dla klienta
                query_faktury_tpl = """
                    WITH invo_base AS (
                      SELECT
                          INVO_ADMNO,
                          INVO_CLNTNO,
                          INVO_DEBH_NO,
                          INVO_DEBC_NO,
                          INVO_AINITIALH,
                          INVO_INVDATE,
                          CASE
                            WHEN INVO_FINALPAYMENTDATE IS NOT NULL
                             AND DATE(INVO_FINALPAYMENTDATE) < DATE('{end_day}')
                            THEN INVO_FINALPAYMENTDATE
                            ELSE NULL
                          END AS INVO_FINALPAYMENTDATE,
                          INVO_DUEDATE
                      FROM invo_view
                      WHERE INVO_ADMNO   = {admno}
                        AND INVO_CLNTNO  = {clntno}
                        AND INVO_DEBH_NO = {debh_no}
                        AND INVO_DEBC_NO = {debc_no}
                    )
                    SELECT
                        INVO_ADMNO,
                        INVO_CLNTNO,
                        INVO_DEBH_NO,
                        INVO_DEBC_NO,
                        INVO_AINITIALH,
                        INVO_INVDATE,
                        INVO_FINALPAYMENTDATE,
                        INVO_DUEDATE
                    FROM invo_base
                    WHERE
                          (INVO_DUEDATE IS NOT NULL
                           AND DATE(INVO_DUEDATE) BETWEEN DATE('{start_day}') AND DATE('{end_day}'))
                       OR (INVO_FINALPAYMENTDATE IS NOT NULL
                           AND DATE(INVO_FINALPAYMENTDATE) BETWEEN DATE('{start_day}') AND DATE('{end_day}'))
                """

                query_faktury = query_faktury_tpl.format(
                    end_day   = end_day,
                    start_day = start_day,
                    admno     = int(invo_admno),
                    clntno    = int(invo_clntno),
                    debh_no   = int(invo_debh_no),
                    debc_no   = int(invo_debc_no),
                )       
        
                # Pomiar czasu wczytywania faktur
                start_read_time = time.time()
                faktury = pd.read_sql_query(query_faktury, conn)
                read_duration = time.time() - start_read_time
        
                # Symulacja przeliczenia terminów płatności
                faktury['INVO_DUEDATE'] = faktury['INVO_DUEDATE'].apply(
                    lambda x: get_next_working_day(pd.Timestamp(x), holidays) if pd.notna(x) else x
                )
        
                faktury['INVO_FINALPAYMENTDATE'] = pd.to_datetime(faktury['INVO_FINALPAYMENTDATE'], errors='coerce')
                faktury['INVO_DUEDATE'] = pd.to_datetime(faktury['INVO_DUEDATE'], errors='coerce')
                faktury['INVO_INVDATE'] = pd.to_datetime(faktury['INVO_INVDATE'], errors='coerce')
                
                paid = faktury['INVO_FINALPAYMENTDATE'].notna()
                unpaid = faktury['INVO_FINALPAYMENTDATE'].isna()        
                
                # Definicje scenariuszy
                scenariusze = {
                    'zapłacone_przed_terminem': faktury[
                        (faktury['INVO_FINALPAYMENTDATE'].notna()) & 
                        (faktury['INVO_FINALPAYMENTDATE'] <= faktury['INVO_DUEDATE']) & 
                        (faktury['INVO_FINALPAYMENTDATE'] < na_dzien)
                    ],
                    'zapłacone_po_terminie': faktury[
                        (faktury['INVO_FINALPAYMENTDATE'].notna()) & 
                        (faktury['INVO_FINALPAYMENTDATE'] > faktury['INVO_DUEDATE']) &
                        (faktury['INVO_FINALPAYMENTDATE'] < na_dzien) 
                    ],
                    'niezapłacone_po_terminie': faktury[
                        unpaid &
                        (na_dzien > faktury['INVO_DUEDATE'])
                    ],
                    'niezapłacone_przed_terminem': faktury[
                        unpaid &
                        (na_dzien <= faktury['INVO_DUEDATE'])
                    ],
                    'wszystkie_faktury': faktury
                }
        
                # Obliczanie statystyk dla każdego scenariusza
                for scenariusz, df in scenariusze.items():
                    if df.empty:
                        continue
                
                    # Kopia, żeby uniknąć SettingWithCopyWarning
                    dfc = df.copy()
                
                    # Kwoty (bezpieczne)
                    dfc['INVO_AINITIALH'] = pd.to_numeric(dfc['INVO_AINITIALH'], errors='coerce')
                    sum_invo_ainitialh = float(dfc['INVO_AINITIALH'].sum(skipna=True))
                    avg_invo_ainitialh = float(dfc['INVO_AINITIALH'].mean(skipna=True))
                    stdev_invo_ainitialh = float(dfc['INVO_AINITIALH'].std(skipna=True))
                    ile_faktur = int(len(dfc))
                
                    # Maski płatności znanych na na_dzien
                    paid_mask = dfc['INVO_FINALPAYMENTDATE'].notna()
                    unpaid_mask = ~paid_mask
                
                    # Domyślnie ustawiamy NULL-e (NaN -> SQLite zapisze jako NULL)
                    avg_dni_do_zaplaty = np.nan
                    stdev_dni_do_zaplaty = np.nan
                    avg_dni_opoznienia = np.nan
                    stdev_dni_opoznienia = np.nan
                
                    if scenariusz.startswith('zapłacone'):
                        # Prawdziwe metryki, legalne (bo FINALPAYMENTDATE <= na_dzien)
                        days_to_pay = (dfc.loc[paid_mask, 'INVO_FINALPAYMENTDATE'] - dfc.loc[paid_mask, 'INVO_INVDATE']).dt.days
                        days_late = (dfc.loc[paid_mask, 'INVO_FINALPAYMENTDATE'] - dfc.loc[paid_mask, 'INVO_DUEDATE']).dt.days
                
                        avg_dni_do_zaplaty = float(days_to_pay.mean()) if not days_to_pay.empty else np.nan
                        stdev_dni_do_zaplaty = float(days_to_pay.std()) if len(days_to_pay) > 1 else np.nan
                
                        avg_dni_opoznienia = float(days_late.mean()) if not days_late.empty else np.nan
                        stdev_dni_opoznienia = float(days_late.std()) if len(days_late) > 1 else np.nan
                
                    elif scenariusz.startswith('niezapłacone'):
                        # PROXY metryki "as-of" dla niezapłaconych (bez leak'u)
                        # "dni do zapłaty" -> wiek faktury do na_dzien
                        age_today = (na_dzien - dfc.loc[unpaid_mask, 'INVO_INVDATE']).dt.days
                
                        # "dni opóźnienia" -> ile dni po terminie na na_dzien, obcięte do >=0
                        late_today = (na_dzien - dfc.loc[unpaid_mask, 'INVO_DUEDATE']).dt.days
                        late_today = late_today.clip(lower=0)
                
                        avg_dni_do_zaplaty = float(age_today.mean()) if not age_today.empty else np.nan
                        stdev_dni_do_zaplaty = float(age_today.std()) if len(age_today) > 1 else np.nan
                
                        avg_dni_opoznienia = float(late_today.mean()) if not late_today.empty else np.nan
                        stdev_dni_opoznienia = float(late_today.std()) if len(late_today) > 1 else np.nan
                
                    else:  # 'wszystkie_faktury'
                        # Mieszany wariant: dla zapłaconych prawdziwe, dla niezapłaconych proxy as-of
                        days_to_pay_paid = (dfc.loc[paid_mask, 'INVO_FINALPAYMENTDATE'] - dfc.loc[paid_mask, 'INVO_INVDATE']).dt.days
                        days_to_pay_unpaid = (na_dzien - dfc.loc[unpaid_mask, 'INVO_INVDATE']).dt.days
                        days_to_pay = pd.concat([days_to_pay_paid, days_to_pay_unpaid], ignore_index=True)
                
                        days_late_paid = (dfc.loc[paid_mask, 'INVO_FINALPAYMENTDATE'] - dfc.loc[paid_mask, 'INVO_DUEDATE']).dt.days
                        days_late_unpaid = (na_dzien - dfc.loc[unpaid_mask, 'INVO_DUEDATE']).dt.days
                        days_late_unpaid = days_late_unpaid.clip(lower=0)
                        days_late = pd.concat([days_late_paid, days_late_unpaid], ignore_index=True)
                
                        avg_dni_do_zaplaty = float(days_to_pay.mean()) if not days_to_pay.empty else np.nan
                        stdev_dni_do_zaplaty = float(days_to_pay.std()) if len(days_to_pay) > 1 else np.nan
                
                        avg_dni_opoznienia = float(days_late.mean()) if not days_late.empty else np.nan
                        stdev_dni_opoznienia = float(days_late.std()) if len(days_late) > 1 else np.nan

                    # Wstawianie wyników do tabeli
                    cursor.execute("""
                        INSERT INTO statystyki_faktur (
                            INVO_ADMNO, INVO_CLNTNO, INVO_DEBH_NO, INVO_DEBC_NO, DZIEN,
                            SCENARIUSZ, AVG_DNI_DO_ZAPLATY, STDEV_DNI_DO_ZAPLATY,
                            AVG_DNI_OPOZNIENIA, STDEV_DNI_OPOZNIENIA,
                            SUM_INVO_AINITIALH, AVG_INVO_AINITIALH, STDEV_INVO_AINITIALH,
                            ILE_FAKTUR, PARAM_Z_ILU_DNI, INSERT_TIMESTAMP
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        invo_admno, invo_clntno, invo_debh_no, invo_debc_no,
                        na_dzien.strftime('%Y-%m-%d %H:%M:%S'),
                        scenariusz,
                        avg_dni_do_zaplaty, stdev_dni_do_zaplaty,
                        avg_dni_opoznienia, stdev_dni_opoznienia,
                        sum_invo_ainitialh, avg_invo_ainitialh, stdev_invo_ainitialh,
                        ile_faktur, z_ilu_dni, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
                
                    # Aktualizacja licznika i logowanie co 10 minut
                    inserted_count += 1
                    current_time = datetime.now()
                    if current_time.minute % 10 == 0 and not log_triggered:
                        log_triggered = True
                        cursor.execute("SELECT * FROM statystyki_faktur ORDER BY INSERT_TIMESTAMP DESC LIMIT 1")
                        recent_rows = cursor.fetchall()
                        print(f"[{current_time}] Inserted rows so far: {inserted_count}")
                        print(f"[{current_time}] Remaining rows to insert: {len(customer_days) * len(z_ilu_dni_values) - inserted_count}")
                        print(f"[{current_time}] Last row in statystyki_faktur: {recent_rows}")
                    elif current_time.minute % 10 != 0:
                        log_triggered = False
        
            # Zapis do bazy na końcu pętli
            # conn.commit()
        conn.commit()
        print("Statystyki faktur zostały obliczone i zapisane.")
        print(datetime.now().strftime("%H:%M:%S"))

        # Pobranie minimalnej i maksymalnej daty z widoku
        cursor.execute("SELECT MIN(dzien), MAX(dzien) FROM group_cust_inv_days;")
        min_date, max_date = cursor.fetchone()
        print("min_date, max_date",min_date, max_date)
        
        
        if not min_date or not max_date:
            print("Brak danych w widoku, wychodzę.")
            conn.close()
            continue
        else:
            print(f"Zakres dat w widoku: {min_date} - {max_date}")
        
        
        NUM_SHARDS = params['NUM_SHARDS']
        batch_size = 1000
        total_inserted_all = 0

        cursor.execute("DELETE FROM dataset_tab WHERE INVO_CLNTNO = ?", (client_id,))
        conn.commit()

        for shard in range(NUM_SHARDS + 1):
            if shard < NUM_SHARDS:
                shard_condition = f"""
                    (
                        g.INVO_DEBC_NO IS NOT NULL
                        AND g.INVO_DEBC_NO NOT GLOB '*[^0-9]*'
                        AND (CAST(g.INVO_DEBC_NO AS INTEGER) % {NUM_SHARDS}) = {shard}
                    )
                """
            else:
                shard_condition = """
                    (
                        g.INVO_DEBC_NO IS NULL
                        OR g.INVO_DEBC_NO GLOB '*[^0-9]*'
                    )
                """

            client_filter = """
                AND g.INVO_CLNTNO = ?
                AND DATE(g.DZIEN) >= DATE(?)
                AND DATE(g.DZIEN) <= DATE(?)
            """

            stats_pivot_sql = params['query_stats_pivot_select'] \
                .replace("{shard_condition}", shard_condition) \
                .replace("{client_filter}", client_filter)

            dataset_raw_sql = params['query_dataset_raw_select'] \
                .replace("{stats_pivot_select}", stats_pivot_sql) \
                .replace("{shard_condition}", shard_condition) \
                .replace("{client_filter}", client_filter)

            current_date = pd.to_datetime(min_date)
            total_inserted_shard = 0

            while current_date <= pd.to_datetime(data_end_str):
                next_date = current_date + pd.Timedelta(days=batch_size - 1)

                query_batch_insert = f"""
                INSERT INTO dataset_tab
                {dataset_raw_sql}
                """

                cursor.execute(
                    query_batch_insert,
                    (
                        client_id,
                        current_date.strftime('%Y-%m-%d'),
                        next_date.strftime('%Y-%m-%d'),
                        client_id,
                        current_date.strftime('%Y-%m-%d'),
                        next_date.strftime('%Y-%m-%d'),
                    )
                )
                conn.commit()

                batch_inserted = cursor.rowcount if cursor.rowcount != -1 else 0
                total_inserted_shard += batch_inserted
                total_inserted_all += batch_inserted

                print(
                    f"Dla client={client_id}, shard={shard} zaimportowano "
                    f"{batch_inserted} rekordów dla dni "
                    f"{current_date.strftime('%Y-%m-%d')} - {next_date.strftime('%Y-%m-%d')}."
                )
                print(datetime.now().strftime("%H:%M:%S"))

                current_date = next_date + pd.Timedelta(days=1)

            print(
                f"[INFO] Client={client_id}, shard={shard} zakończony. "
                f"Łącznie w shardzie: {total_inserted_shard}"
            )

        cursor.execute("DELETE FROM dataset WHERE INVO_CLNTNO = ?", (client_id,))
        conn.commit()

        refill_dataset_for_client(
            conn,
            client_id=client_id,
            source_table="dataset_tab",
            target_table="dataset",
            batch_days=params.get("dataset_batch_days", 31)
        )
        print(datetime.now().strftime("%H:%M:%S"))
        
    
        create_aggregated_data_temp(conn, data_end_str)
    
        # (na końcu)
        conn.commit()
        conn.close()
