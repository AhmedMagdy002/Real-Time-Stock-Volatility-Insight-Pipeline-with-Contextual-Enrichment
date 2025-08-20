# Real-Time Stock Volatility Insight Pipeline with Contextual Enrichment

This project implements a modern real-time and batch data pipeline for financial tick data, enriched with contextual weather information to simulate decision-support analytics. It demonstrates streaming ingestion, medallion data lakehouse design (Bronze → Silver → Gold), workflow orchestration, validations, and visualization on a production-like stack.

##  Tech Stack
- **Apache Kafka**: Real-time data ingestion platform for stock ticks
- **Apache Spark (Structured Streaming + Batch)**: Distributed processing for ingestion, enrichment, and aggregation
- **Delta Lake**: ACID-compliant storage on S3 (via Localstack) for Bronze/Silver/Gold layers
- **LocalStack**: AWS S3 emulator for local development and testing
- **Apache Airflow**: Orchestration for hourly and daily batch jobs, validations, and business quality checks
- **Trino**: Fast SQL query engine for Delta Lake data
- **Grafana**: Dashboards for real-time and historical trends (ticks, OHLC, weather)
- **Docker Compose**: Containerized orchestration for local deployment

## 🔄 Data Flow (Medallion Architecture)

- **Producer**: Generates simulated trades (multi-exchange stock symbols with price + volume).
- **Bronze (raw)**: Raw Kafka messages stored as Delta (audit trail, immutable).
- **Silver (curated)**: Trades enriched with weather data (fetched every 10min for cities).
- **Gold (analytics)**: Hourly/Daily aggregates with OHLC stats, trade volume, and weather context.
- **Airflow**: Orchestrates batch aggregation, validation, and quality checks.
- **Trino**: Provides interactive SQL queries across all Delta tables.
- **Grafana**: Dashboards for real-time and aggregated insights.

## 🚀 Getting Started

### 1. Start All Services
```bash
docker-compose up -d
```

### 2. Create S3 Bucket
```bash
docker exec -it localstack awslocal s3 mb s3://test-bucket
docker exec -it localstack awslocal s3 ls
```

### 3. Create Kafka Topic
```bash
docker exec -it kafka kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it kafka kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

### 4. Run Data Producers
- **Stock tick producer** (`producer.py`): Pushes simulated trades into Kafka.
- **Silver enrichment job** (`silver_streaming.py`): Reads Bronze, enriches with current weather, writes to Silver.
- *(Optional)*: Ingest weather events via a separate weather producer for a more realistic pipeline.

### 5. Airflow Pipelines
Open Airflow at: [http://localhost:8081](http://localhost:8081)

**DAGs available**:
- **fintech_hourly_processing**:
  - Runs every hour at HH:05
  - Tasks: `hourly_processor` → `validate_hourly.py` → `hourly_quality_checks.py`
- **fintech_daily_processing**:
  - Runs daily at 02:00 AM
  - Tasks: `daily_processor` → `validate_daily.py` → `daily_quality_checks.py`

**Validations & Checks**:
- Schema validation ensures required fields exist.
- Business quality checks enforce:
  - Prices > 0
  - Volumes > 0
  - Weather enrichment coverage ≥ 80%

### 🔍 Query With Trino
Start Trino CLI:
```bash
docker exec -it trino trino
```
```sql
-- Set the legacy session property
SET SESSION delta.legacy_create_table_with_existing_location_enabled = true;

-- Create schema if it does not exist
CREATE SCHEMA IF NOT EXISTS delta.default
WITH (location = 's3://test-bucket/schemas/default/');

-- Verify schema exists
SHOW SCHEMAS FROM delta;

-- Create streaming trades table
CREATE TABLE IF NOT EXISTS delta.default.streaming_trades (
    symbol VARCHAR,
    price DOUBLE,
    volume INTEGER,
    timestamp TIMESTAMP,
    exchange VARCHAR,
    temperature DOUBLE,
    humidity INTEGER,
    condition VARCHAR,
    date DATE,
    processing_time BIGINT
)
WITH (
    location = 's3://test-bucket/delta-tables/streaming_trades'
);

-- Create hourly summaries table
CREATE TABLE IF NOT EXISTS delta.default.hourly_summaries (
    symbol VARCHAR,
    exchange VARCHAR,
    hour VARCHAR,
    date DATE,
    condition VARCHAR,
    avg_price DOUBLE,
    min_price DOUBLE,
    max_price DOUBLE,
    total_volume BIGINT,
    trade_count INTEGER,
    avg_temperature DOUBLE,
    avg_humidity DOUBLE,
    batch_processed_at VARCHAR
)
WITH (
    location = 's3://test-bucket/delta-tables/hourly_summaries'
);

-- Create daily summaries table  
CREATE TABLE IF NOT EXISTS delta.default.daily_summaries (
    symbol VARCHAR,
    exchange VARCHAR,
    date DATE,
    avg_price DOUBLE,
    min_price DOUBLE,
    max_price DOUBLE,
    total_volume BIGINT,
    trade_count INTEGER,
    avg_temperature DOUBLE,
    avg_humidity DOUBLE,
    batch_processed_at VARCHAR
)
WITH (
    location = 's3://test-bucket/delta-tables/daily_summaries'
);

-- Verify all tables exist
SHOW TABLES FROM delta.default;

-- Test queries
SELECT * FROM delta.default.streaming_trades LIMIT 5;
SELECT * FROM delta.default.hourly_summaries LIMIT 5;
SELECT * FROM delta.default.daily_summaries LIMIT 5;
```
### 📊 Grafana Dashboards
Access Grafana at: [http://localhost:3000](http://localhost:3000)  
Login: `admin` / `admin`

**Configure Data Source**:
- Add new datasource → Type: Trino
- Name: `Trino-delta`
- URL: `http://trino:8080` → Save & Test



## ✅ Features Highlighted
- **End-to-End Simulation**: Streaming + batch like in real fintech systems
- **Medallion Architecture**: Bronze (raw), Silver (enriched), Gold (query-ready)
- **Workflow Orchestration with Airflow**: Scheduling, retries, SLA, validations, quality checks
- **Data Quality Validation**: Schema checks and business rules
- **ACID Data Lake**: Delta Lake ensures correctness and replayability
- **SQL Analytics**: Trino queries across Delta tables

