KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders"

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
}

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "orders-consumer-group",
    "auto.offset.reset": "earliest",
}