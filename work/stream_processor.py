# streaming_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, date_format, avg, sum as spark_sum, count
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
import requests
import time

# Weather enrichment
exchange_city_map = {
    "XNAS": "New York",
    "XNYS": "New York",
    "XLON": "London",
    "XTKS": "Tokyo"
}

weather_cache = {}
CACHE_TTL_SECONDS = 600

def fetch_weather_cached(city):
    now = time.time()
    cached = weather_cache.get(city)
    
    if cached and now - cached["fetched_at"] < CACHE_TTL_SECONDS:
        return cached["data"]
    
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid=34debbcec75c0e6f7351fcda60b141be&units=metric"
        response = requests.get(url, timeout=5)
        data = response.json()
        weather = {
            "temperature": float(data["main"]["temp"]),
            "humidity": int(data["main"]["humidity"]),
            "condition": data["weather"][0]["description"]
        }
        weather_cache[city] = {"fetched_at": now, "data": weather}
        return weather
    except Exception as e:
        print(f"⚠️ Weather fetch failed for {city}: {e}")
        return {"temperature": None, "humidity": None, "condition": "unknown"}

# Initialize Spark
spark = SparkSession.builder \
    .appName("FinTechStreamingPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(" Spark Streaming session created")

# Schema for incoming data
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("volume", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("exchange", StringType())

print(" Starting streaming from Kafka...")

# STREAMING: Read from Kafka continuously
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse streaming data
parsed_stream = streaming_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("processing_time", lit(int(time.time())))

# Enrichment function for streaming batches
def enrich_and_store_stream(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    print(f"🌊 Processing streaming batch {batch_id} with {batch_df.count()} records")
    
    # Get weather data for unique exchanges
    exchanges = [row['exchange'] for row in batch_df.select("exchange").distinct().collect()]
    weather_data = {}
    
    for ex in exchanges:
        city = exchange_city_map.get(ex, "New York")
        print(f" Fetching weather for {ex} ({city})")
        weather_data[ex] = fetch_weather_cached(city)
    
    # SIMPLIFIED: Only add weather columns (no city)
    enriched_df = batch_df
    for col_name in ["temperature", "humidity", "condition"]:
        enriched_df = enriched_df.withColumn(col_name, lit(None))
    
    for exchange, weather in weather_data.items():
        enriched_df = enriched_df \
            .withColumn("temperature", when(col("exchange") == exchange, lit(weather.get("temperature"))).otherwise(col("temperature"))) \
            .withColumn("humidity", when(col("exchange") == exchange, lit(weather.get("humidity"))).otherwise(col("humidity"))) \
            .withColumn("condition", when(col("exchange") == exchange, lit(weather.get("condition"))).otherwise(col("condition")))
    
    #  ADD DATE PARTITIONING COLUMN
    enriched_df = enriched_df \
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    
    # WRITE WITH DATE PARTITIONING ONLY
    enriched_df.write.format("delta") \
        .mode("append") \
        .partitionBy("date") \
        .save("s3a://test-bucket/delta-tables/streaming_trades")
    
    print(f" Batch {batch_id} written to streaming storage with date partitioning")

# Start the streaming query
streaming_query = parsed_stream.writeStream \
    .foreachBatch(enrich_and_store_stream) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "s3a://test-bucket/checkpoints/streaming") \
    .start()

print(" Streaming query started! Processing every 5 seconds...")
print(" Run the producer in another terminal to see real-time processing")
print(" Weather data will be fetched for multiple cities based on exchanges")

# Keep streaming running
try:
    streaming_query.awaitTermination()
except KeyboardInterrupt:
    print("\n Stopping streaming...")
    streaming_query.stop()
    print(" Streaming stopped")