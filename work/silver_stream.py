# scripts/silver_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, date_format, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType
import threading, time, requests

# Mapping exchanges to cities
exchange_city_map = {
    "XNAS": "New York",
    "XNYS": "New York",
    "XLON": "London",
    "XTKS": "Tokyo"
}
cities = list(set(exchange_city_map.values()))

# Global weather cache updated by background thread
weather_cache = {}
API_KEY = ""
POLL_INTERVAL = 600  # 10 minutes

def fetch_all_weather():
    """Fetch weather for all configured cities every 10 minutes"""
    while True:
        print(" Refreshing weather cache for all cities...")
        for city in cities:
            try:
                url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
                r = requests.get(url, timeout=5).json()
                weather_cache[city] = {
                    "temperature": float(r["main"]["temp"]),
                    "humidity": int(r["main"]["humidity"]),
                    "condition": r["weather"][0]["description"],
                    "fetched_at": time.time()
                }
                print(f"✅ Weather updated for {city}: {weather_cache[city]}")
            except Exception as e:
                weather_cache[city] = {"temperature": None, "humidity": None, "condition": "unknown"}
                print(f"⚠️ Failed to fetch weather for {city}: {e}")
        time.sleep(POLL_INTERVAL)

# Start the background weather fetch loop in a thread
threading.Thread(target=fetch_all_weather, daemon=True).start()

# ✅ Spark Setup
spark = SparkSession.builder \
    .appName("SilverEnrichment") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("volume", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("exchange", StringType())

# Ingest from Bronze trades
bronze_stream = spark.readStream.format("delta").load("s3a://test-bucket/delta-tables/bronze_trades")

parsed_stream = bronze_stream \
    .select(from_json(col("raw_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

# Enrichment function
from pyspark.sql import DataFrame

def enrich_and_store(batch_df: DataFrame, batch_id: int):
    if batch_df.count() == 0:
        return

    print(f" Processing batch {batch_id} with {batch_df.count()} trades")

    enriched = batch_df \
        .withColumn("temperature", lit(None)) \
        .withColumn("humidity", lit(None)) \
        .withColumn("condition", lit(None))

    for ex, city in exchange_city_map.items():
        wd = weather_cache.get(city, {"temperature": None, "humidity": None, "condition": "unknown"})
        enriched = enriched.withColumn(
            "temperature", when(col("exchange") == ex, lit(wd["temperature"])).otherwise(col("temperature"))
        ).withColumn(
            "humidity", when(col("exchange") == ex, lit(wd["humidity"])).otherwise(col("humidity"))
        ).withColumn(
            "condition", when(col("exchange") == ex, lit(wd["condition"])).otherwise(col("condition"))
        )

    enriched.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("date") \
        .save("s3a://test-bucket/delta-tables/silver_trades")

    print(f"✅ Batch {batch_id} enriched and written to Silver")

silver_query = parsed_stream.writeStream \
    .foreachBatch(enrich_and_store) \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://test-bucket/checkpoints/silver") \
    .start()

print("✨ Silver enrichment query started (joining with weather cache)...")
silver_query.awaitTermination()
