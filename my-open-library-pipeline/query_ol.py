import duckdb

conn = duckdb.connect("open_library_pipeline.duckdb")

print("=== TABLES ===")
print(conn.execute("SHOW TABLES").df().to_string())

print("\n=== BOOKS ===")
print(conn.execute("SELECT bibkey, title, url FROM open_library_data.books").df())

print("\n=== AUTHORS ===")
print(conn.execute("SELECT * FROM open_library_data.books__authors LIMIT 10").df())
