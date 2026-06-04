import duckdb

def run_diagnostic():
    # 1. Inicjalizacja połączenia lokalnego DuckDB
    con = duckdb.connect()
    
    try:
        print("--- KROK 1: Ładowanie rozszerzeń ---")
        con.execute("LOAD odbc; LOAD odbc_scanner; LOAD httpfs;")
        print("[OK] Rozszerzenia załadowane.")

        # 2. Parametry połączenia (Dostosuj te dane!)
        # Driver 18 jest domyślny dla nowych instalacji, 17 dla starszych
        server = "ceo" 
        user = "sa"
        password = "Informatyk#59400"
        master_conn_str = f"Driver={{ODBC Driver 18 for SQL Server}};Server={server};Database=master;UID={user};PWD={password};TrustServerCertificate=yes;"

        print(f"\n--- KROK 2: Próba ATTACH do serwera {server} ---")
        con.execute(f"ATTACH '{master_conn_str}' AS mssql_server (TYPE ODBC);")
        print("[OK] Serwer MSSQL podpięty jako wirtualna baza 'mssql_server'.")

        # 3. Dynamiczne pobieranie listy baz danych
        print("\n--- KROK 3: Pobieranie listy baz klientów ---")
        databases = con.execute("""
            SELECT name 
            FROM mssql_server.sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
              AND state_desc = 'ONLINE'
        """).df()
        
        if databases.empty:
            print("[!] Nie znaleziono baz spełniających kryteria.")
            return

        print(f"Znaleziono {len(databases)} baz. Lista:")
        print(databases['name'].tolist())

        # 4. Test Filter Pushdown (Kluczowe dla wydajności 16 rdzeni)
        # Wybieramy pierwszą bazę z brzegu do testu
        target_db = databases['name'].iloc[0]
        print(f"\n--- KROK 4: Test optymalizatora na bazie: {target_db} ---")
        
        # EXPLAIN pokaże nam, czy filtr WHERE zostanie wysłany do MSSQL
        # Zmień 'TwojaTabela' i 'TwojaKolumna' na istniejące w Twojej bazie
        test_query = f"EXPLAIN SELECT * FROM mssql_server.{target_db}.CDN.ZapisyKPR WHERE KPR_KPRID > 100"
        
        # Uwaga: Jeśli nie znasz nazw tabel, odkomentuj poniższe, aby je wylistować:
        # tables = con.execute(f"SELECT table_name FROM mssql_server.{target_db}.information_schema.tables").df()
        # print(tables)

        try:
            explanation = con.execute(test_query).fetchone()[1]
            print("Plan zapytania (szukaj frazy 'Filters' lub 'pushdown'):")
            print(explanation)
        except Exception as e:
            print(f"Pomiinięto EXPLAIN (brak tabeli do testu): {e}")

    except Exception as e:
        print(f"\n[BŁĄD KRYTYCZNY]: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    run_diagnostic()