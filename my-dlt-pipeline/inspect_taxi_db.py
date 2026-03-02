import duckdb

conn = duckdb.connect('nyc_taxi_pipeline.duckdb')

print("Tables:")
tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'nyc_taxi_data';").fetchall()
for table in tables:
    print(f"  {table[0]}.{table[1]}")

if tables:
    print("\nRow counts:")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}.{table[1]};").fetchone()[0]
        print(f"  {table[1]}: {count:,}")

conn.close()
