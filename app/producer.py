from confluent_kafka import Producer
from config import PRODUCER_CONFIG, TOPIC_NAME
import json
import time

producer = Producer(PRODUCER_CONFIG)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

for i in range(10):
    order = {
        "order_id": i,
        "customer": "Antonio",
        "amount": 25.50 + i,
        "created_at": time.time(),
    }

    producer.produce(
        topic=TOPIC_NAME,
        key=str(order["order_id"]),
        value=json.dumps(order),
        callback=delivery_report,
    )

    producer.poll(0)
    time.sleep(1)

producer.flush()
