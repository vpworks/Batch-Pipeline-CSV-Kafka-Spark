# Batch-Pipeline-CSV-Kafka-Spark
This GitHub repo contains the producer and consumer scripts that were used for creating the pipeline.

## About Producer Script
1) Data Extraction from CSV.
2) Serializing CSV rows to JSON messages.
3) Publishing messages to Kafka topic.
4) Kafka broker (at broker address) stores topic and its messages.
5) 
### Uses
Used for data ingestion: extracts and prepares data so Spark can consume it.

### Ingestion Modules
- json
- csv
- kafka
  - Class -> KafkaProducer
