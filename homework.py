import duckdb

conn = duckdb.connect("ny_taxi_pipeline.duckdb")

# Q1: Start date dan end date
print("=== Q1: Date Range ===")
print(conn.execute("""
    SELECT 
        MIN(trip_pickup_date_time) AS start_date,
        MAX(trip_pickup_date_time) AS end_date
    FROM taxi_data.rides
""").df())

# Q2: Proporsi pembayaran dengan credit card
print("\n=== Q2: Payment Type Proportion ===")
print(conn.execute("""
    SELECT 
        payment_type,
        COUNT(*) AS count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM taxi_data.rides
    GROUP BY payment_type
    ORDER BY count DESC
""").df())

# Q3: Total tips
print("\n=== Q3: Total Tips ===")
print(conn.execute("""
    SELECT ROUND(SUM(tip_amt), 2) AS total_tips
    FROM taxi_data.rides
""").df())
