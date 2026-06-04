import duckdb
import hashlib
import multiprocessing
import psutil
import os


def load_env_file(path=".env"):
    """Wczytuje proste wpisy KEY=VALUE z pliku .env, jesli istnieje."""
    if not os.path.exists(path):
        return

    with open(path, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Brak wymaganej zmiennej w .env: {name}")
    return value


load_env_file()

# Parametry serwera sa pobierane z .env albo ze zmiennych srodowiskowych.
SERVER = required_env("DB_SERVER")
USER = required_env("DB_USER")
PASSWORD = required_env("DB_PASSWORD")


def sql_server_conn_str(db_name):
    return (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={SERVER};Database={db_name};"
        f"UID={USER};PWD={PASSWORD};"
        f"TrustServerCertificate=yes;"
    )


def anonymize_db_name(db_name):
    prefix = "CDN" if db_name.upper().startswith("CDN") else "DB"
    digest = hashlib.sha256(db_name.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{digest}"


def worker_etl(db_name):
    """Funkcja wykonywana przez kazdy z 12 rdzeni osobno."""
    process_name = multiprocessing.current_process().name
    pid = os.getpid()
    anon_db_name = anonymize_db_name(db_name)

    try:
        # Kazdy proces musi miec wlasna instancje DuckDB.
        con = duckdb.connect()
        con.execute("LOAD odbc; LOAD odbc_scanner;")

        # Connection string nadal uzywa prawdziwej nazwy bazy.
        conn_str = sql_server_conn_str(db_name)
        sql_conn_str = conn_str.replace("'", "''")

        output_file = f"data_raw/{anon_db_name}.parquet"
        os.makedirs("data_raw", exist_ok=True)

        print(f"[{process_name} | PID {pid}] Startuje baze: {anon_db_name}")

        con.execute(f"""
            COPY (
                SELECT *, '{anon_db_name}' as source_db
                FROM odbc_query('{sql_conn_str}', 'SELECT * FROM [CDN].[TraVat]')
            ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """)

        print(f"[{process_name} | PID {pid}] ZAKONCZONO: {anon_db_name}")
        con.close()

    except Exception as e:
        error_message = str(e).replace(db_name, anon_db_name)
        print(f"[{process_name} | PID {pid}] BLAD w {anon_db_name}: {error_message}")
        print(f"[BLAD] Baza {anon_db_name}: {error_message}")


if __name__ == "__main__":
    # --- FAZA DISCOVERY ---
    # Pobieramy liste baz raz, w procesie glownym.
    master_con = duckdb.connect()
    master_con.execute("LOAD odbc; LOAD odbc_scanner;")

    master_conn_str = sql_server_conn_str("master")

    db_query = "SELECT name FROM odbc_query(?, 'SELECT name FROM sys.databases WHERE name LIKE ''CDN%'' AND name NOT LIKE ''CDN_KNF%'' AND state_desc = ''ONLINE''')"
    res = master_con.execute(db_query, [master_conn_str]).fetchall()

    # Filtrujemy tylko bazy klientow.
    all_databases = [r[0] for r in res if r[0] not in ("master", "tempdb", "model", "msdb")]
    master_con.close()

    print(f"Rozpoczynam ELT dla {len(all_databases)} baz na 12 rdzeniach...")

    # --- FAZA PARALLEL ---
    # Pool(12) rozdziela liste baz na 12 dostepnych procesorow.
    with multiprocessing.Pool(processes=12) as pool:
        pool.map(worker_etl, all_databases)

    print("\n=== PROCES ELT ZAKONCZONY SUKCESEM ===")
