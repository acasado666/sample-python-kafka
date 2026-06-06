import json

from confluent_kafka import Consumer
from config import CONSUMER_CONFIG, TOPIC_NAME


# consumer = Consumer(CONSUMER_CONFIG)
#
# consumer.subscribe([TOPIC_NAME])
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print("🟢 Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        decodeValue = msg.value().decode("utf-8")
        orderJson = json.loads(decodeValue)

        print(f"Received message order with key details:")
        print(f"  Key: {key}")
        print(f"  Value: {orderJson}")
        print(f"  Topic: {msg.topic()}")
        print(f"  Partition: {msg.partition()}")
        print(f"  Offset: {msg.offset()}")

        print(f"📦 Received order: {orderJson['quantity']} x {orderJson['item']} from {orderJson['user']}")

except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
