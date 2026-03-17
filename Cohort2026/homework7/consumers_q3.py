from kafka import KafkaConsumer
import json

def main():
    count = 0

    consumer = KafkaConsumer(
        'green-trips',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='green-rides-counter',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for message in consumer:
            ride = message.value

            if ride['trip_distance'] > 5:
                count += 1

    except KeyboardInterrupt:
        pass

    print(f'Trips with distance > 5: {count}')


if __name__ == '__main__':
    main()