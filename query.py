import duckdb

conn = duckdb.connect("ny_taxi_pipeline.duckdb")

# Lihat semua tabel
print("=== TABLES ===")
print(conn.execute("SHOW TABLES").df())

# Jumlah baris
print("\n=== ROW COUNT ===")
print(conn.execute("SELECT COUNT(*) as total_rows FROM taxi_data.rides").df())

# Preview data
print("\n=== SAMPLE DATA ===")
print(conn.execute("SELECT * FROM taxi_data.rides LIMIT 3").df())

# Nama kolom
print("\n=== COLUMNS ===")
print(conn.execute("DESCRIBE taxi_data.rides").df())
