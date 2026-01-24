import csv
import json
from kafka import KafkaProducer

KAFKA_BROKER= "localhost:9092"
TOPIC_NAME = "walmart-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
    )
    
with open("/Users/mularoe/Desktop/DATASETS/walmart/walmart.csv", "r") as file:
     reader = csv.DictReader(file)
     for row in reader:
     	producer.send(TOPIC_NAME, value=row)
print("Sent:", row)

producer.flush()
producer.close()     	    
