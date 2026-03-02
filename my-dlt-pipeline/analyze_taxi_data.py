"""Answer questions about NYC taxi trip data."""

import duckdb

conn = duckdb.connect('nyc_taxi_pipeline.duckdb', read_only=True)

print("=" * 80)
print("NYC TAXI TRIP DATA ANALYSIS")
print("=" * 80)

# Total records
total = conn.execute("SELECT COUNT(*) FROM nyc_taxi_data.taxi_trips;").fetchone()[0]
print(f"\n1. Total number of taxi trips: {total:,}")

# Schema
print("\n2. Table schema:")
schema = conn.execute("DESCRIBE nyc_taxi_data.taxi_trips;").fetchall()
for col in schema:
    print(f"   {col[0]}: {col[1]}")

# Date range
print("\n3. Date range:")
date_range = conn.execute("""
    SELECT 
        MIN(trip_pickup_date_time) as earliest,
        MAX(trip_pickup_date_time) as latest
    FROM nyc_taxi_data.taxi_trips;
""").fetchone()
print(f"   Earliest trip: {date_range[0]}")
print(f"   Latest trip: {date_range[1]}")

# Payment types
print("\n4. Payment type distribution:")
payment_types = conn.execute("""
    SELECT payment_type, COUNT(*) as count
    FROM nyc_taxi_data.taxi_trips
    GROUP BY payment_type
    ORDER BY count DESC;
""").fetchall()
for pt in payment_types:
    print(f"   {pt[0]}: {pt[1]:,}")

# Average fare
print("\n5. Average fare amount:")
avg_fare = conn.execute("SELECT AVG(fare_amt) FROM nyc_taxi_data.taxi_trips;").fetchone()[0]
print(f"   ${avg_fare:.2f}")

# Top vendors
print("\n6. Vendor distribution:")
vendors = conn.execute("""
    SELECT vendor_name, COUNT(*) as count
    FROM nyc_taxi_data.taxi_trips
    GROUP BY vendor_name
    ORDER BY count DESC;
""").fetchall()
for v in vendors:
    print(f"   {v[0]}: {v[1]:,}")

# Passenger count
print("\n7. Average passenger count:")
avg_passengers = conn.execute("SELECT AVG(passenger_count) FROM nyc_taxi_data.taxi_trips;").fetchone()[0]
print(f"   {avg_passengers:.2f}")

# Trip distance stats
print("\n8. Trip distance statistics:")
distance_stats = conn.execute("""
    SELECT 
        MIN(trip_distance) as min_dist,
        AVG(trip_distance) as avg_dist,
        MAX(trip_distance) as max_dist
    FROM nyc_taxi_data.taxi_trips;
""").fetchone()
print(f"   Min: {distance_stats[0]:.2f} miles")
print(f"   Avg: {distance_stats[1]:.2f} miles")
print(f"   Max: {distance_stats[2]:.2f} miles")

conn.close()
print("\n" + "=" * 80)
