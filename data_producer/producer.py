import json
import time
import requests
import os
from kafka import KafkaProducer

# ========================
# CONFIGURATION
# ========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "paris-trees-stream")
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", 20))
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 60))  # 1 minutes

# ========================
# INITIALISATION KAFKA
# ========================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Kafka producer started...")

# ========================
# BOUCLE PRINCIPALE
# ========================
offset = 0  # démarrage de l'offset

while True:
    url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/les-arbres/records?limit={BATCH_LIMIT}&offset={offset}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        records = data.get("results", [])  # <-- ici on récupère seulement la liste "results"
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        time.sleep(FETCH_INTERVAL)
        continue

    if not records:
        print("No more records, resetting offset to 0")
        offset = 0
        time.sleep(FETCH_INTERVAL)
        continue

    # envoyer chaque record dans Kafka
    for record in records:
        producer.send(TOPIC_NAME, record)

    producer.flush()
    print(f"Batch sent to Kafka | offset={offset} | {len(records)} records")

    offset += 1  # incrémenter l'offset pour le batch suivant
    time.sleep(FETCH_INTERVAL)
