import duckdb

conn = duckdb.connect('open_library_pipeline.duckdb')

print("Tables:")
tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema != 'information_schema';").fetchall()
for table in tables:
    print(f"  {table[0]}.{table[1]}")

print("\nBooks table schema:")
schema = conn.execute("DESCRIBE open_library_pipeline_dataset.books;").fetchall()
for col in schema:
    print(f"  {col[0]}: {col[1]}")

print("\nRow counts:")
for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}.{table[1]};").fetchone()[0]
    print(f"  {table[0]}.{table[1]}: {count}")

conn.close()
