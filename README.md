
# Real-Time Stock Volatility Insight Pipeline with Contextual Enrichment 

This project implements a robust, real-time data pipeline that ingests and enriches stock trade data with contextual weather information. The pipeline leverages modern data engineering tools and practices.

## Tech Stack
Apache Kafka: Real-time data streaming platform for ingesting stock trade data

Apache Spark: Distributed processing engine for data transformation and enrichment

Delta Lake: ACID-compliant storage layer on S3 for reliable data lake operations

LocalStack: Local AWS S3 emulation for development and testing

Trino: SQL query engine for analyzing Delta Lake data

Apache Airflow: Workflow orchestration for automated pipeline execution

Grafana: Real-time dashboards and visualization

Docker Compose: Container orchestration for local development

## Data Flow
Producer → Generates stock trade events with price, volume, and timestamp

Kafka → Streams events through stock-trades topic

Spark Streaming → Enriches trades with weather data and calculates metrics

Delta Lake → Stores enriched data in S3 with ACID guarantees

Trino → Provides SQL interface for querying Delta tables

Grafana → Visualizes real-time metrics and historical trends
## Complete Data Pipeline
Start All Services
```
docker-compose up -d
```
create s3 bucket
```
# Create bucket using docker exec
docker exec -it localstack awslocal s3 mb s3://test-bucket

# Verify bucket was created
docker exec -it localstack awslocal s3 ls
```
Create Kafka Topic
```
# Create topic using docker exec
docker exec -it kafka kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Verify topic was created
docker exec -it kafka kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```
Run Your Python Producer and Consumer
```
# Access Jupyter notebook
# Open browser at http://localhost:8888
# In a notebook cell:
%run /home/jovyan/work/simple_producer.py
%run /home/jovyan/work/simple_spark_consumer.py
```
or using airflow 
```
# Access airflow at http://localhost:8081
trigger kafka_spark_pipeline DAG 

```
Register Delta Table in Trino
```
# Connect to Trino
docker exec -it trino trino

-- Set session property to enable legacy behavior
SET SESSION delta.legacy_create_table_with_existing_location_enabled = true;

-- Now create the table
CREATE TABLE IF NOT EXISTS delta.default.my_table (
    id VARCHAR,
    name VARCHAR,
    value VARCHAR
)
WITH (
    location = 's3://test-bucket/delta-tables/my_table'
);
-- Verify table exists
SHOW TABLES FROM delta.default;

-- Query data
SELECT * FROM delta.default.my_table;

```
Configure Grafana Data Source
1.	Open Grafana: http://localhost:3000
2.	Login: admin/admin
3.	Go to Configuration → Data Sources
4.	Click Add data source
5.	Search for Trino
6.	Configure:
   
•	Name: trino-datasource

•	URL: http://trino:8080

•	Click Save & test


Create Visualizations in Grafana

Go to Explore and test these queries:

Query 1: View All Data

```
SELECT * FROM delta.default.my_table
```
Query 2: Summary Statistics

```
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT name) as unique_names,
    MIN(CAST(value AS INTEGER)) as min_value,
    MAX(CAST(value AS INTEGER)) as max_value,
    AVG(CAST(value AS INTEGER)) as avg_value
FROM delta.default.my_table
```
Create a Dashboard
1.	Click Dashboards → New Dashboard
2.	Add panels:
Panel 1:
```
 Total Records (Stat)
SELECT COUNT(*) as count FROM delta.default.my_table
```
Panel 2: Data Table
```
SELECT id, name, value, processed_time 
FROM delta.default.my_table 
ORDER BY processed_time DESC
```
Panel 3: Values by Name (Bar Chart)
```
SELECT 
    name, 
    SUM(CAST(value AS INTEGER)) as total_value
FROM delta.default.my_table
GROUP BY name

```
