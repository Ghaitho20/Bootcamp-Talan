import json
import os
import requests
from kafka import KafkaProducer

# ========================
# CONFIGURATION
# ========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "paris-trees-stream")
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", 20))
OFFSET = int(os.getenv("OFFSET", 0))  # will be passed by Airflow

# ========================
# INITIALISE KAFKA PRODUCER
# ========================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Kafka producer started... fetching batch at offset {OFFSET}")

# ========================
# FETCH ONE BATCH
# ========================
url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/les-arbres/records?limit={BATCH_LIMIT}&offset={OFFSET}"
try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    records = data.get("results", [])
except requests.RequestException as e:
    print(f"Error fetching data: {e}")
    records = []

if not records:
    print("No more records, resetting offset to 0")
    OFFSET = 0
else:
    for record in records:
        producer.send(TOPIC_NAME, record)

    producer.flush()
    print(f"Batch sent to Kafka | offset={OFFSET} | {len(records)} records")
    OFFSET += 1  # increment offset for next run

# ========================
# Print next offset (Airflow will save it)
# ========================
print(f"NEXT_OFFSET={OFFSET}")
