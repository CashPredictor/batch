import sqlite3
import pandas as pd
import numpy as np
import json
import datetime
import logging
import time
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def convert_timestamps_in_df(df):
    """
    Konwertuje wartości typu datetime i pandas.Timestamp na format string zgodny z SQLite.
    """
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
    return df

@contextmanager
def get_connection(database_path):
    conn = None
    try:
        conn = sqlite3.connect(
            database_path,
            timeout=30,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        yield conn
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            conn.close()

def create_and_insert_processed_table(conn, processed_df, target_table_name, original_table_name=None, batch_size=500000, create_indexes=False, index_columns=None):
    """
    Tworzy tabelę na podstawie struktury DataFrame i opcjonalnie oryginalnej tabeli w SQLite,
    a następnie wstawia dane partiami (batch insert).
    
    :param conn: Połączenie z bazą danych SQLite.
    :param processed_df: DataFrame z przetworzonymi danymi.
    :param target_table_name: Nazwa tabeli docelowej.
    :param original_table_name: Opcjonalna nazwa oryginalnej tabeli, z której pobieramy typy kolumn.
    :param batch_size: Liczba wierszy w jednej partii wstawiania (domyślnie 500000).
    """
    cursor = conn.cursor()
    
    # Pobierz typy danych z oryginalnej tabeli, jeśli podano jej nazwę
    original_column_types = {}
    if original_table_name:
        try:
            original_column_types = get_column_types(cursor, original_table_name)
        except Exception as e:
            logging.warning(f"Could not fetch column types from original table '{original_table_name}': {e}")
    
    # Mapowanie typów danych dla kolumn
    columns_sql = []
    for col in processed_df.columns:
        if col in original_column_types:
            col_type = original_column_types[col]  # Użyj typu z oryginalnej tabeli
        else:
            # Przypisz typ danych na podstawie danych w DataFrame
            if pd.api.types.is_integer_dtype(processed_df[col]):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(processed_df[col]):
                col_type = "REAL"
            elif pd.api.types.is_datetime64_any_dtype(processed_df[col]):
                col_type = "DATETIME"
            else:
                col_type = "TEXT"  # Domyślny typ TEXT dla nowych kolumn
        columns_sql.append(f"{col} {col_type}")
    
    # Tworzenie tabeli docelowej
    sanitized_table_name = sanitize_table_name(target_table_name)
    if sanitized_table_name == "first_model_tab":
        if cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (sanitized_table_name,)).fetchone():
            cursor.execute(f"DELETE FROM {sanitized_table_name};")
            conn.commit()
    cursor.execute(f"DROP TABLE IF EXISTS {sanitized_table_name};")
    create_table_query = f"CREATE TABLE {sanitized_table_name} ({', '.join(columns_sql)});"
    cursor.execute(create_table_query)

    if create_indexes and index_columns:
        for col in index_columns:
            idx_name = sanitize_table_name(f"idx_{sanitized_table_name}_{col}")
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {sanitized_table_name}({col});"
            cursor.execute(sql)
        logging.info(f"Utworzono indeksy na kolumnach: {index_columns}")    
    
    # Konwersja wartości dat/czasu na format zgodny z SQLite
    processed_df = convert_timestamps_in_df(processed_df)
    
    # Przygotowanie zapytania INSERT z placeholderami
    placeholders = ", ".join(["?" for _ in processed_df.columns])
    insert_query = f"INSERT INTO {sanitized_table_name} VALUES ({placeholders});"
    
    total_rows = len(processed_df)
    print(f"[INFO] Rozpoczynam wstawianie {total_rows} wierszy do tabeli '{sanitized_table_name}' przy użyciu batch_size={batch_size}.")
    
    if total_rows <= batch_size:
        # Jeśli rekordów nie więcej niż batch_size – jednorazowo
        data_tuples = list(processed_df.itertuples(index=False, name=None))
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
    else:
        # Wstawianie w partiach
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_df = processed_df.iloc[start:end]
            data_tuples = list(batch_df.itertuples(index=False, name=None))
            cursor.executemany(insert_query, data_tuples)
            conn.commit()  # commit po każdej partii
            print(f"[INFO] Wstawiono partie wierszy: {start} - {end}.")
    
    print(f"Tabela '{sanitized_table_name}' została utworzona i dane zostały zapisane.")

def insert_into_existing_table(
    conn,
    processed_df,
    target_table_name,
    batch_size=500000
):
    """
    Dopisuje dane do istniejącej tabeli.
    NIE robi DROP TABLE.
    NIE robi CREATE TABLE.
    Zakłada, że schemat został już przygotowany wcześniej (np. przez db_init).
    """
    cursor = conn.cursor()
    sanitized_table_name = sanitize_table_name(target_table_name)
    # 1. Sprawdzenie, czy tabela istnieje
    exists = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
        (sanitized_table_name,)
    ).fetchone()
    if not exists:
        raise RuntimeError(
            f"Tabela '{sanitized_table_name}' nie istnieje. "
            f"Najpierw utwórz ją w db_init lub innym etapie inicjalizacji."
        )
    # 2. Pobranie kolumn z istniejącej tabeli
    cursor.execute(f"PRAGMA table_info({sanitized_table_name});")
    table_info = cursor.fetchall()
    table_columns = [row[1] for row in table_info]
    if not table_columns:
        raise RuntimeError(f"Nie udało się pobrać kolumn tabeli '{sanitized_table_name}'.")
    # 3. Upewniamy się, że DF ma wszystkie kolumny wymagane przez tabelę
    df = processed_df.copy()
    for col in table_columns:
        if col not in df.columns:
            df[col] = None
    # 4. Ograniczamy się do kolumn tabeli i w tej samej kolejności
    df = df[table_columns]
    # 5. Zamiana NaN/NaT na None
    df = df.where(pd.notna(df), None)
    placeholders = ", ".join(["?" for _ in table_columns])
    insert_sql = f"""
        INSERT INTO {sanitized_table_name} ({', '.join(table_columns)})
        VALUES ({placeholders})
    """
    def _to_sqlite_safe(v):
        if pd.isna(v):
            return None
        if isinstance(v, np.datetime64):
            v = pd.to_datetime(v)
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        return v
    total_inserted = 0
    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start:start + batch_size]
        rows = [
            tuple(_to_sqlite_safe(v) for v in row)
            for row in chunk.itertuples(index=False, name=None)
        ]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        total_inserted += len(rows)
    logging.info(
        f"[insert_into_existing_table] Dopisano {total_inserted} wierszy do tabeli '{sanitized_table_name}'."
    )

def get_column_types(cursor, table_name):
    """
    Pobiera typy danych kolumn z tabeli SQLite.
    """
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor.fetchall()
    return {col[1]: col[2] for col in columns_info}  # Słownik {nazwa_kolumny: typ_danych}

def get_sql_type(pandas_dtype):
    """
    Mapuje typ danych z Pandas na typy SQLite.
    """
    if pd.api.types.is_integer_dtype(pandas_dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        return "INTEGER"
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        return "DATETIME"
    elif pd.api.types.is_object_dtype(pandas_dtype):
        return "TEXT"
    else:
        raise ValueError(f"Unhandled pandas dtype: {pandas_dtype}")

def read_data_from_all_db(query, params, currently_testing):
    print('jestem')
    database_path='CashPredictor_X.db'
    try:
        with get_connection(database_path) as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        logging.error(f"Failed to read data from database: {e}")
        raise
        
def read_data_from_db(query, params, currently_testing=False, chunk_size=1_000_000):
    if currently_testing:
        database_path = params['test_database']
    else:
        database_path = params['database']
    try:
        with get_connection(database_path) as conn:
            chunks = pd.read_sql_query(query, conn, params=params, chunksize=chunk_size)
            all_chunks = []
            for chunk in chunks:
                float_cols = chunk.select_dtypes(include=["float64"]).columns
                chunk[float_cols] = chunk[float_cols].astype("float32")
                all_chunks.append(chunk)
            df = pd.concat(all_chunks, ignore_index=True)
            return df
    except Exception as e:
        logging.error(f"Failed to read data from database: {e}")
        raise

def sanitize_table_name(table_name):
    sanitized_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
    print(f"Sanitized table name: Original='{table_name}', Sanitized='{sanitized_name}'")
    return sanitized_name

def standardize_dataframe_types(df, date_columns=None):
    """
    Wymusza standaryzację typów danych w DataFrame.
    
    :param df: DataFrame do standaryzacji.
    :param date_columns: Lista kolumn, które powinny być konwertowane na datetime64[ns].
    :return: Zaktualizowany DataFrame.
    """
    if date_columns:
        for col in date_columns:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col])
    return df

def write_output_to_db(df, table_name, params, original_table_name=None, currently_testing=False, create_indexes=False, index_columns=None, insert_only=False):
    logging.info(f"Starting to write DataFrame to table: {table_name}")

    if currently_testing:
        database_path=params['test_database']
    else:
        database_path=params['database']
        
    logging.info(f"Attempting to write DataFrame to table: {table_name}")
    with get_connection(database_path) as conn:
        try:
            if insert_only:
                insert_into_existing_table(conn=conn, processed_df=df, target_table_name=table_name, batch_size=params.get("data_limit", 500000))
            else:
                create_and_insert_processed_table(conn, df, table_name, original_table_name, params['data_limit'], create_indexes=create_indexes, index_columns=index_columns)           
        except Exception as e:
            print(f"Failed to write DataFrame to table {table_name}: {e}")
            conn.rollback()
            raise
    logging.info(f"Finished writing DataFrame to table: {table_name}")
