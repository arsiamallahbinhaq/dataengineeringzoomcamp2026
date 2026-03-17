import json
import time
import pandas as pd
from kafka import KafkaProducer

# Load data (LOCAL FILE lebih disarankan)
df = pd.read_parquet('green_tripdata_2025-10.parquet')

# Select required columns
df = df[[
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]]

# Convert datetime → string
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'green-trips'

t0 = time.time()

# Send data
for record in df.to_dict(orient='records'):
    producer.send(topic_name, value=record)

producer.flush()

t1 = time.time()

print(len(df))
print(f'took {(t1 - t0):.2f} seconds')