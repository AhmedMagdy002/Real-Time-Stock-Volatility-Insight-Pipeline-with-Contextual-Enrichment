# scripts/bronze_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("BronzeIngestionPipeline") \
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

# Kafka stream → just ingest raw payloads
bronze_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Store raw JSON string as is
bronze_df = bronze_stream.selectExpr(
    "CAST(value AS STRING) as raw_payload",
    "timestamp"  # <-- keep Kafka event timestamp too
).withColumn("ingestion_time", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
 .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

bronze_query = bronze_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3a://test-bucket/checkpoints/bronze") \
    .partitionBy("date") \
    .outputMode("append") \
    .start("s3a://test-bucket/delta-tables/bronze_trades")

print("🔥 Bronze stream started...")
bronze_query.awaitTermination()