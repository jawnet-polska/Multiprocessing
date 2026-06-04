import duckdb
import os
import pyarrow as pa


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
        f"Server={SERVER};"
        f"Database={db_name};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
        f"TrustServerCertificate=yes;"
    )


# Connection string dla bazy master (do pobrania listy baz).
MASTER_CONN_STR = sql_server_conn_str("master")


def run_final_arrow_test():
    # Inicjalizacja DuckDB w pamieci.
    con = duckdb.connect()

    try:
        print("--- KROK 1: Ladowanie silnika kolumnowego ---")
        # Instalujemy i ladujemy niezbedne moduly do obslugi ODBC i Arrow.
        con.execute("INSTALL odbc; INSTALL odbc_scanner;")
        con.execute("LOAD odbc; LOAD odbc_scanner;")

        print("--- KROK 2: Pobieranie listy baz danych ---")
        # Wykorzystujemy odbc_query do bezpiecznego odczytu listy baz.
        db_query = """
            SELECT name
            FROM odbc_query(?, 'SELECT name FROM sys.databases WHERE state_desc = ''ONLINE''')
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        """
        databases = con.execute(db_query, [MASTER_CONN_STR]).fetchall()
        db_list = [row[0] for row in databases]

        print(f"Znaleziono {len(db_list)} baz klientow.")

        if db_list:
            # Testujemy na pierwszej znalezionej bazie.
            target_db = db_list[0]
            print(f"\n--- KROK 3: Test strumienia Apache Arrow dla: {target_db} ---")

            # Tworzymy connection string dedykowany dla konkretnej bazy.
            specific_conn = sql_server_conn_str(target_db)

            # Pobieramy nazwe pierwszej tabeli ze schematu CDN.
            table_name_query = f"""
                SELECT table_name
                FROM odbc_query('{specific_conn}',
                'SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''CDN''')
            """
            table_res = con.execute(table_name_query).fetchone()

            if table_res:
                table_name = table_res[0]
                print(f"Otwieram strumien danych dla tabeli: [CDN].[{table_name}]")

                # fetch_record_batch(10000) tworzy RecordBatchReader.
                # Dane nie sa wczytywane w calosci do RAM.
                reader = con.execute(f"""
                    SELECT * FROM odbc_query(?, 'SELECT TOP 100 * FROM [CDN].[{table_name}]')
                """, [specific_conn]).to_arrow_reader(10000)

                print("\n[WYNIK TESTU ARROW]:")
                print(f"Typ obiektu: {type(reader)}")
                print(f"Schemat (kolumny): {reader.schema.names}")

                # Proba odczytu pierwszej paczki danych ze strumienia.
                try:
                    paczka = reader.read_next_batch()
                    print(f"Sukces! Odczytano paczke o rozmiarze: {len(paczka)} wierszy.")
                    print("Mechanizm Zero-Copy MSSQL -> Arrow jest drozny.")
                except StopIteration:
                    print("Strumien zostal otwarty, ale tabela wydaje sie pusta.")

            else:
                print(f"Nie znaleziono tabel w schemacie CDN w bazie {target_db}.")

    except Exception as e:
        print(f"\n[BLAD KRYTYCZNY]: {e}")
    finally:
        con.close()
        print("\n--- Polaczenie zamkniete ---")


if __name__ == "__main__":
    run_pure_arrow_test = run_final_arrow_test()
