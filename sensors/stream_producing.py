import pandas as pd
import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INTERVAL_SECONDS = 15
KAFKA_TOPIC = 'sensor-data'

print("Sensors are working!")

df = pd.read_csv('sensor_data.csv')

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

existing_topics = admin_client.list_topics()

if KAFKA_TOPIC not in existing_topics:
    topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

while True:
    for _, row in df.iterrows():
        data = row.to_dict()

        # Replace the original 'time' with the current UTC time
        data['event_time'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Drop old 'time' column 
        data.pop('time', None)
        
        try:
            producer.send(KAFKA_TOPIC, value=data)
            print(f"Sent: {data}")
            producer.flush()
        except Exception as e:
            print(f"Failed to produce message: {data} → Error: {e}")
        time.sleep(INTERVAL_SECONDS)  

