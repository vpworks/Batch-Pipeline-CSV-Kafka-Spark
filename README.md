# Batch-Pipeline-CSV-Kafka-Spark
This GitHub repo contains the producer and consumer scripts that were used for creating the pipeline.

## About Producer Script
1) Data Extraction from CSV.
2) Serializing CSV rows to JSON messages.
3) Publishing messages to Kafka topic.
4) Kafka broker (at broker address) stores topic and its messages.
 
### Uses
Used for data ingestion: extracts and prepares data so Spark can consume it.

### Ingestion Modules
- json
- csv
- kafka
  - Class -> KafkaProducer

## About Consumer Script
1) Reads messages from Kafka Topic.
2) Deserializes messages to JSON strings.
3) Parses JSON strings to Spark structured columns with StringType() datatype.
4) Applies type conversion on columns with non-string values.
5) Applies transformations ->
   - Column derivation
   - Flag creation
   - Aggregations
6) Writes data as CSV to local storage

## Uses
Used for data processing, transformation and loading.

## Modules used
- pyspark.sql
- pyspark.sql.functions
- pyspark.sql.types
