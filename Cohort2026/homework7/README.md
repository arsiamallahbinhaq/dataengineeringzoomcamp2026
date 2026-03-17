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