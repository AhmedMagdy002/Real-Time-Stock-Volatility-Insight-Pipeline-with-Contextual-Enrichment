# scripts/hourly_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, avg, sum as spark_sum, count, max as spark_max, min as spark_min, lit
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("FinTechHourlyProcessor") \
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def run_hourly_processing():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Last completed hour (so at 10:05 → process 09:00–09:59)
        now = datetime.now()
        previous_hour = now.replace(minute=0, second=0, microsecond=0) #- timedelta(hours=1)
        target_hour = previous_hour.strftime("%Y-%m-%d %H")
        target_date = previous_hour.strftime("%Y-%m-%d")

        logger.info(f"🕐 Processing hourly data for: {target_hour}")

        #  Read from Silver trades (not streaming_trades anymore)
        streaming_data = spark.read.format("delta") \
            .load("s3a://test-bucket/delta-tables/silver_trades") \
            .filter(col("date") == target_date) \
            .filter(date_format(col("timestamp"), "yyyy-MM-dd HH") == target_hour)

        record_count = streaming_data.count()

        if record_count == 0:
            logger.warning(f" No records found for hour: {target_hour}")
            return

        logger.info(f"📈 Processing {record_count} records for hour: {target_hour}")

        # Hourly aggregation
        hourly_summary = streaming_data \
            .withColumn("hour", lit(target_hour)) \
            .groupBy("symbol", "exchange", "hour", "date", "condition") \
            .agg(
                avg("price").alias("avg_price"),
                spark_min("price").alias("min_price"),
                spark_max("price").alias("max_price"),
                spark_sum("volume").alias("total_volume"),
                count("*").alias("trade_count"),
                avg("temperature").alias("avg_temperature"),
                avg("humidity").alias("avg_humidity")
            ) \
            .withColumn("batch_processed_at", lit(datetime.now().isoformat()))

        hourly_summary.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"hour = '{target_hour}'") \
            .save("s3a://test-bucket/delta-tables/hourly_summaries")

        logger.info(f" Hourly summary updated for {target_hour}")

    except Exception as e:
        logger.error(f" Hourly processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_hourly_processing()