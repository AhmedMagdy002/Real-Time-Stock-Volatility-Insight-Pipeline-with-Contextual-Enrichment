# scripts/validate_hourly.py
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("FinTechHourlyValidator") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def validate_hourly():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ✅ Calculate last completed hour (not the current partial one)
    now = datetime.now()
    previous_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    target_hour = previous_hour.strftime("%Y-%m-%d %H")

    print(f"🔍 Validating hourly data for {target_hour}")

    try:
        # Load Delta hourly summary table
        df = spark.read.format("delta").load("s3a://test-bucket/delta-tables/hourly_summaries")
        filtered = df.filter(df.hour == target_hour)

        # Count records
        record_count = filtered.count()

        if record_count == 0:
            print(f"⚠️ No records found for {target_hour}. Skipping validation (soft pass).")
            return  # ✅ Soft pass when no data

        print(f"✅ Found {record_count} records for {target_hour}")

        # Schema validation
        expected_cols = {
            "symbol", "exchange", "hour", "date", "condition",
            "avg_price", "min_price", "max_price",
            "total_volume", "trade_count", "avg_temperature", "avg_humidity"
        }
        current_cols = set(filtered.columns)
        missing = expected_cols - current_cols
        if missing:
            raise Exception(f"❌ Schema validation failed! Missing columns: {missing}")
        else:
            print("✅ Schema validation passed")

    except Exception as e:
        print(f"❌ Spark error during validation: {e}")
        raise
    finally:
        # ✅ Only stop Spark at the very end
        spark.stop()

if __name__ == "__main__":
    validate_hourly()