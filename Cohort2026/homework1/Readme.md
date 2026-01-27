# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run

Commands:
```
docker run --rm -it --entrypoint=bash python:3.13
# inside the container:
pip --version
```

Output:
```
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```
## Question 2. Understanding Docker networking and docker-compose

Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?
```
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data

```

Answer: db:5433 and db:5432

## Question 3. Counting short trips

```
q3 = """
SELECT
  COUNT(*) FILTER (WHERE trip_distance <= 1) AS up_to_1_mile,
  COUNT(*) FILTER (WHERE trip_distance > 1 AND trip_distance <= 3) AS between_1_and_3,
  COUNT(*) FILTER (WHERE trip_distance > 3 AND trip_distance <= 7) AS between_3_and_7,
  COUNT(*) FILTER (WHERE trip_distance > 7 AND trip_distance <= 10) AS between_7_and_10,
  COUNT(*) FILTER (WHERE trip_distance > 10) AS over_10
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01';
"""

pd.read_sql(q3, engine)

```
Output:
```
	up_to_1_mile	between_1_and_3	between_3_and_7	between_7_and_10	over_10
0	8007	23859	10047	2428	2550
```

## Question 4. Longest trip for each day
```
q4 = """
SELECT
  DATE(lpep_pickup_datetime) AS pickup_day,
  MAX(trip_distance) AS max_distance
FROM green_tripdata
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
"""

pd.read_sql(q4, engine)

```
Output:
```
pickup_day	max_distance
2025-11-14	88.03
```

## Question 5. Biggest pickup zone
```
q5 = """
SELECT
  z."Zone" AS pickup_zone,
  SUM(g.total_amount) AS total_amount_sum
FROM green_tripdata g
JOIN taxi_zone_lookup z
  ON g."PULocationID" = z."LocationID"
WHERE DATE(g.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 10;
"""

pd.read_sql(q5, engine)


```
Output:
```
	pickup_zone	total_amount_sum
0	East Harlem North	9281.92
1	East Harlem South	6696.13
2	Central Park	2378.79
3	Washington Heights South	2139.05
4	Morningside Heights	2100.59
5	Jamaica	1998.11
6	Fort Greene	1780.41
7	Downtown Brooklyn/MetroTech	1499.02
8	Forest Hills	1423.75
```

## Question 6. Largest Tip
```
q6 = """
SELECT
  z_do."Zone" AS dropoff_zone,
  SUM(g.tip_amount) AS total_tip
FROM green_tripdata g
JOIN taxi_zone_lookup z_pu
  ON g."PULocationID" = z_pu."LocationID"
JOIN taxi_zone_lookup z_do
  ON g."DOLocationID" = z_do."LocationID"
WHERE z_pu."Zone" = 'East Harlem North'
  AND g.lpep_pickup_datetime >= '2025-11-01'
  AND g.lpep_pickup_datetime < '2025-12-01'
  AND z_do."Zone" IN (
      'JFK Airport',
      'Yorkville West',
      'East Harlem North',
      'LaGuardia Airport'
  )
GROUP BY z_do."Zone"
ORDER BY total_tip DESC;

"""

pd.read_sql(q6, engine)

```
Output:
```
dropoff_zone	total_tip
Yorkville West	2403.17
LaGuardia Airport	1835.52
East Harlem North	604.10
JFK Airport	307.66
```

## Question 7. Terraform Workflow 

Which of the following sequences, respectively, describes the workflow for: Downloading the provider plugins and setting up backend, Generating proposed changes and auto-executing the plan Remove all resources managed by terraform 
Answers: 
```
terraform import, terraform apply -y, terraform destroy 
```