# scripts/daily_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, max as spark_max, min as spark_min, lit
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("FinTechDailyProcessor") \
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

def run_daily_processing():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Process yesterday
        #yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")

        logger.info(f"📅 Processing daily data for: {yesterday}")

        #  Read from Silver table
        streaming_data = spark.read.format("delta") \
            .load("s3a://test-bucket/delta-tables/silver_trades") \
            .filter(col("date") == yesterday)

        record_count = streaming_data.count()

        if record_count == 0:
            logger.warning(f" No records found for date: {yesterday}")
            return

        logger.info(f" Processing {record_count} records for date: {yesterday}")

        # Daily aggregation
        daily_summary = streaming_data \
            .groupBy("symbol", "exchange", "date") \
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

        daily_summary.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"date = '{yesterday}'") \
            .save("s3a://test-bucket/delta-tables/daily_summaries")

        logger.info(f" Daily summary updated for {yesterday}")

    except Exception as e:
        logger.error(f" Daily processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_daily_processing()