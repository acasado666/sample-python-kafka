import json
import time
import uuid
import random

from confluent_kafka import Producer
# from config import PRODUCER_CONFIG, TOPIC_NAME

# producer = Producer(PRODUCER_CONFIG)
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(f"✅ Message delivered to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


customers = ["Antonio Kode", "John Doe", "Jane Smith"]

for i in range(20):
    order = {
        "order_id": str(uuid.uuid4()),
        "customer": random.choice(customers),
        "amount": 25.50 + i,
        "quantity": random.randint(1, 5),
        "created_at": time.time(),
    }

    value = json.dumps(order).encode("utf-8")

    producer.produce(
        topic=TOPIC_NAME,
        key=str(order["order_id"]),
        value=json.dumps(order),
        callback=delivery_report,
    )

    producer.poll(0)
    time.sleep(1)

producer.flush()
