from confluent_kafka import Consumer
from config import CONSUMER_CONFIG, TOPIC_NAME

import json

consumer = Consumer(CONSUMER_CONFIG)
consumer.subscribe([TOPIC_NAME])

print("Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        value = json.loads(msg.value().decode("utf-8"))

        print(f"Received message:")
        print(f"  Key: {key}")
        print(f"  Value: {value}")
        print(f"  Topic: {msg.topic()}")
        print(f"  Partition: {msg.partition()}")
        print(f"  Offset: {msg.offset()}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
