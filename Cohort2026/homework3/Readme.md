## Create dataset & External Table

```
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp.yellow_taxi_ext`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://qwiklabs-gcp-03-f66f180b03dc-yellow-taxi-bucket/yellow_tripdata_2024-*.parquet'
  ]
);
```

# Create Materialized 
```
CREATE OR REPLACE TABLE `zoomcamp.yellow_taxi` AS
SELECT *
FROM `zoomcamp.yellow_taxi_ext`;
```

# Question 1. Counting records
```
SELECT count(*) FROM `zoomcamp.yellow_taxi_ext`
20332093
```

# 2. Question 2. Data read estimation
```
SELECT COUNT(DISTINCT PULocationID)
FROM `zoomcamp.yellow_taxi_ext`;
--> 0

SELECT COUNT(DISTINCT PULocationID)
FROM `zoomcamp.yellow_taxi`;
--> 155.12
```

# Question 3. Understanding columnar storage
```
SELECT PULocationID
FROM `zoomcamp.yellow_taxi_ext`;
SELECT PULocationID, DOLocationID
FROM `zoomcamp.yellow_taxi_ext`;
```
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed. <--
BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

# Question 4. Counting zero fare trips. How many records have a fare_amount of 0?
```
SELECT count(*)
FROM `zoomcamp.yellow_taxi_ext`
where fare_amount=0;
```

128,210
546,578
20,188,016
8,333 <--

# 5 Partitioning and clustering
```
CREATE OR REPLACE TABLE `zoomcamp.yellow_taxi_optimized` PARTITION BY DATE(tpep_dropoff_datetime) CLUSTER BY VendorID AS
SELECT *
FROM `zoomcamp.yellow_taxi_ext`;
```
Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

# Question 6. Partition benefits
```
SELECT DISTINCT VendorID
FROM `zoomcamp.yellow_taxi`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
310.24
```
```
SELECT DISTINCT VendorID
FROM `zoomcamp.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
26.84
```

# Question 7. External table storage
Where is the data stored in the External Table you created?

Big Query
Container Registry
GCP Bucket <--
Big Table

# Question 8. Clustering best practices
It is best practice in Big Query to always cluster your data:

True
False --> answer
It's optional if data is big and need so many filtering inside the table

# Question 9. Understanding table scans
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
0 bytes, because BigQuery uses table metadata to compute COUNT(*) on native tables without scanning the actual data.
