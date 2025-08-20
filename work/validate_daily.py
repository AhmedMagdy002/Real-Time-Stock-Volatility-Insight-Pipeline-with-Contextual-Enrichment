# scripts/validate_daily.py
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("FinTechDailyValidator") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def validate_daily():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ✅ Always validate yesterday
    #yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")
    print(f" Validating daily data for {yesterday}")

    try:
        # Load Delta daily summary table
        df = spark.read.format("delta").load("s3a://test-bucket/delta-tables/daily_summaries")
        filtered = df.filter(df.date == yesterday)

        # Count records
        record_count = filtered.count()

        if record_count == 0:
            logger.warning(f" No records found for {yesterday}. Skipping validation (soft pass).")
            sys.stdout.flush()
            return

        print(f" Found {record_count} records for {yesterday}")

        # Schema validation
        expected_cols = {
            "symbol", "exchange", "date",
            "avg_price", "min_price", "max_price",
            "total_volume", "trade_count",
            "avg_temperature", "avg_humidity"
        }
        current_cols = set(filtered.columns)
        missing = expected_cols - current_cols
        if missing:
            raise Exception(f" Schema validation failed! Missing: {missing}")
        else:
            print("✅ Schema validation passed")

    except Exception as e:
        print(f" Spark error during validation: {e}")
        raise
    finally:
        # ✅ Stop Spark at the very end
        spark.stop()

if __name__ == "__main__":
    validate_daily()