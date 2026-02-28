"""@bruin
name: ingestion.trips
type: python
image: python:3.11

materialization:
  type: table
  strategy: append

connection: duckdb-default

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged
  - name: pickup_location_id
    type: integer
    description: Pickup location ID
  - name: dropoff_location_id
    type: integer
    description: Dropoff location ID
  - name: fare_amount
    type: float
    description: Fare amount
  - name: taxi_type
    type: string
    description: Type of taxi
  - name: payment_type
    type: integer
    description: Payment type ID
@bruin"""

import os
import json
import pandas as pd

def materialize():
    # 1. Capture Bruin Environment Variables
    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"])
    end_date = pd.to_datetime(os.environ["BRUIN_END_DATE"])
    
    # Parse variables with a safe fallback
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # 2. Generate the list of months to iterate through
    # 'MS' (Month Start) ensures we get every month overlapping the range
    month_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    all_dfs = []

    for taxi_type in taxi_types:
        for date in month_range:
            year = date.year
            month = f"{date.month:02d}"
            
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            try:
                # 3. Read Parquet directly from URL
                df = pd.read_parquet(url)

                # 4. Handle Column Prefix Differences
                # Yellow taxis use 'tpep_', Green use 'lpep_'
                prefix = "tpep" if taxi_type == "yellow" else "lpep"
                pickup_col = f"{prefix}_pickup_datetime"
                dropoff_col = f"{prefix}_dropoff_datetime"

                # Standardize columns to match your @bruin definition
                prefix = "tpep" if taxi_type == "yellow" else "lpep"
                pickup_col = f"{prefix}_pickup_datetime"
                dropoff_col = f"{prefix}_dropoff_datetime"
                pickup_loc_col = "PULocationID"
                dropoff_loc_col = "DOLocationID"
                
                df = df[[
                    pickup_col, dropoff_col, pickup_loc_col, dropoff_loc_col,
                    'fare_amount', 'payment_type'
                ]].rename(columns={
                    pickup_col: "pickup_datetime",
                    dropoff_col: "dropoff_datetime",
                    pickup_loc_col: "pickup_location_id",
                    dropoff_loc_col: "dropoff_location_id"
                })
                
                df['taxi_type'] = taxi_type

                # 5. Strict Date Filtering
                # Parquet files are monthly; Bruin might request a specific day range
                mask = (df['pickup_datetime'] >= start_date) & (df['pickup_datetime'] <= end_date)
                all_dfs.append(df.loc[mask])

            except Exception as e:
                print(f"Skipping {taxi_type} for {year}-{month}: {e}")

    # 6. Final Concatenation
    if not all_dfs:
        # Return an empty dataframe with correct schema if no data found
        return pd.DataFrame(columns=[
            "pickup_datetime", "dropoff_datetime", "pickup_location_id",
            "dropoff_location_id", "fare_amount", "taxi_type", "payment_type"
        ])

    return pd.concat(all_dfs, ignore_index=True)