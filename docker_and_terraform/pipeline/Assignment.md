SQL queries:

question 3:
``` bash
SELECT COUNT(*) from green_trip  WHERE lpep_pickup_datetime between '2025-11-01' and '2025-12-01' 
AND trip_distance <=1;
```

question 4:
```bash
SELECT 
    lpep_pickup_datetime AS pickup_day,
    MAX(trip_distance) AS max_distance
FROM 
    green_trip
WHERE 
    trip_distance < 100
GROUP BY 
    pickup_day
ORDER BY 
    max_distance DESC
LIMIT 1;
```


question 5:
```bash
SELECT 
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_sum
FROM 
    green_trip t
JOIN 
    zones z ON t."PULocationID" = z."LocationID"
WHERE 
    CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_sum DESC
LIMIT 1;
```