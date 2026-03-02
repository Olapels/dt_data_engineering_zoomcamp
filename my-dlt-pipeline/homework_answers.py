"""Answer homework questions about NYC taxi data."""

import duckdb

conn = duckdb.connect('nyc_taxi_pipeline.duckdb', read_only=True)

# Question 1: Start and end date
print("Question 1: What is the start date and end date of the dataset?")
dates = conn.execute("""
    SELECT 
        DATE(MIN(trip_pickup_date_time)) as start_date,
        DATE(MAX(trip_pickup_date_time)) as end_date
    FROM nyc_taxi_data.taxi_trips;
""").fetchone()
print(f"Answer: {dates[0]} to {dates[1]}\n")

# Question 2: Proportion of credit card payments
print("Question 2: What proportion of trips are paid with credit card?")
credit_stats = conn.execute("""
    SELECT 
        COUNT(*) FILTER (WHERE LOWER(payment_type) = 'credit') as credit_count,
        COUNT(*) as total_count
    FROM nyc_taxi_data.taxi_trips;
""").fetchone()
proportion = (credit_stats[0] / credit_stats[1]) * 100
print(f"Answer: {proportion:.2f}%\n")

# Question 3: Total tips
print("Question 3: What is the total amount of money generated in tips?")
total_tips = conn.execute("""
    SELECT SUM(tip_amt)
    FROM nyc_taxi_data.taxi_trips;
""").fetchone()[0]
print(f"Answer: ${total_tips:,.2f}\n")

conn.close()
