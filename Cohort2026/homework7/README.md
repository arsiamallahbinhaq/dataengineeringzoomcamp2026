## Homework
In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

green_tripdata_2025-10.parquet

## Setup
We'll use the same infrastructure from the workshop.

Follow the setup instructions: build the Docker image, start the services:
```
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on localhost:9092
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on localhost:5432 (user: postgres, password: postgres)

If you previously ran the workshop and have old containers/volumes, do a clean start:
```
docker compose down -v
docker compose build
docker compose up -d
```
Note: the container names (like workshop-redpanda-1) assume the directory is called workshop. If you renamed it, adjust accordingly.

## Question 1. Redpanda version
Run rpk version inside the Redpanda container:
```
docker exec -it workshop-redpanda-1 rpk version
```
What version of Redpanda are you running?
```
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

## Question 2. Sending data to Redpanda

Create a topic called green-trips:
```
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```
Now write a producer to send the green taxi data to this topic.

How long did it take to send the data?

- 10 seconds --> closest answer
- 60 seconds
- 120 seconds
- 300 seconds

Script: [producer.py](https://github.com/arsiamallahbinhaq/dataengineeringzoomcamp2026/blob/main/Cohort2026/homework7/producer.py)

Answer:
```
python producer.py
49416
took 4.72 seconds
```

## Question 3. Consumer - trip distance
Write a Kafka consumer that reads all messages from the green-trips topic (set auto_offset_reset='earliest').

Count how many trips have a trip_distance greater than 5.0 kilometers.

How many trips have trip_distance > 5?

- 6506
- 7506
- 8506 --> answer
- 9506

Answer: 
```
python consumers_q3.py
Trips with distance > 5: 8506
```
Script:
[consumer_q3.py](https://github.com/arsiamallahbinhaq/dataengineeringzoomcamp2026/blob/main/Cohort2026/homework7/consumer_q3.py)

## Question 4. Tumbling window - pickup location
Create a Flink job that reads from green-trips and uses a 5-minute tumbling window to count trips per PULocationID.

Write the results to a PostgreSQL table with columns: window_start, PULocationID, num_trips.

After the job processes all data, query the results:

```
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
Which PULocationID had the most trips in a single 5-minute window?
```

- 42
- 74 --> answer
- 75
- 166

Script: [q4_answer.py](https://github.com/arsiamallahbinhaq/dataengineeringzoomcamp2026/blob/main/Cohort2026/homework7/q4_answer.py)

```
SELECT PULocationID, num_trips
FROM aggregated_pickup
ORDER BY num_trips DESC
LIMIT 3;
```
```
 pulocationid | num_trips 
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
(3 rows)
```

Command:
```
docker compose exec -d jobmanager ./bin/flink run \
  -py /opt/src/jobs/aggregated_pickup_job.py \
  --pyFiles /opt/src \
  -d
```

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap on PULocationID, using lpep_pickup_datetime as the event time with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other. When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the PULocationID with the longest session (most trips in a single session).

How many trips were in the longest session?

- 12
- 31
- 51
- 81 --> Answer

Script: 
[q5_answer.py](https://github.com/arsiamallahbinhaq/dataengineeringzoomcamp2026/blob/main/Cohort2026/homework7/q5_answer.py)

```
cd ../../data-engineering-zoomcamp/07-streaming/workshop && \
docker compose down && docker compose up -d && sleep 15 && \
docker compose exec postgres psql -U postgres -c "DROP TABLE IF EXISTS session_pickup; CREATE TABLE session_pickup (session_start TIMESTAMP(3), session_end TIMESTAMP(3), PULocationID INT, num_trips BIGINT, PRIMARY KEY (session_start, session_end, PULocationID));" && \
uv run python src/producers/q2_producer.py && \
docker compose exec -d jobmanager ./bin/flink run -py /opt/src/jobs/session_pickup_job.py --pyFiles /opt/src -d && \
sleep 30 && \
docker compose exec postgres psql -U postgres -c "SELECT PULocationID, num_trips FROM session_pickup ORDER BY num_trips DESC LIMIT 3;"
```

## Question 6. Tumbling window - largest tip
Create a Flink job that uses a 1-hour tumbling window to compute the total tip_amount per hour (across all locations).

Which hour had the highest total tip amount?

- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

Script:
[q6_answer.py](https://github.com/arsiamallahbinhaq/dataengineeringzoomcamp2026/blob/main/Cohort2026/homework7/q6_answer.py)

Command:
```
docker compose exec -d jobmanager ./bin/flink run \
  -py /opt/src/jobs/tumbling_tips_job.py \
  --pyFiles /opt/src \
  -d
```
