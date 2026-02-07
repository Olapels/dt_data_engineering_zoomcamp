# Homework 3 Assignment solutions

## Step 1 : creating external tables & materialized table stored in Big Query tables

```bash
--- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `beaming-might-485522-u0.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_course/yellow_tripdata_2024-*.parquet']
);
```

```bash
--- Create Regular Table (Materialized)
CREATE OR REPLACE TABLE `beaming-might-485522-u0.zoomcamp.yellow_tripdata_2024` AS
SELECT * FROM `beaming-might-485522-u0.zoomcamp.external_yellow_tripdata`;
```

## Question 1
```bash
--- sql query( count in thousand separator for readability)
SELECT FORMAT("%'d", COUNT(*)) AS total_row_counts
FROM zoomcamp.yellow_tripdata_2024
```

## Question 2
```bash 
--materialized table
SELECT FORMAT("%'d", COUNT(DISTINCT PULocationID)) AS total_row_counts
FROM zoomcamp.yellow_tripdata_2024
```

```bash
--external table
SELECT FORMAT("%'d", COUNT(DISTINCT PULocationID)) AS total_row_counts
FROM zoomcamp.ext_yellow_tripdata_2024
```

## Question 3
### (A)
```bash
SELECT PULocationID 
FROM zoomcamp.yellow_tripdata_2024;
```

### (B)
```bash
SELECT PULocationID, DOLocationID
FROM zoomcamp.yellow_tripdata_2024
```

## Question 4:
```bash
SELECT COUNT(*) 
FROM zoomcamp.yellow_tripdata_2024
WHERE fare_amount = 0;
```

## Question 6:
```bash
SELECT COUNT(DISTINCT VendorID) 
FROM zoomcamp.yellow_tripdata_2024 
WHERE DATE(tpep_dropoff_datetime) >  '2024-03-01' and DATE(tpep_dropoff_datetime) <= '2024-03-15' 
```

## Question 9:

```bash
SELECT COUNT(*) FROM zoomcamp.yellow_tripdata_2024
```