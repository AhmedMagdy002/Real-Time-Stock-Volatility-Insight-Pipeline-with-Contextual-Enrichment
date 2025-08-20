# scripts/hourly_quality_checks.py
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("HourlyExtraChecks") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def run_quality_checks():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load("s3a://test-bucket/delta-tables/hourly_summaries")

    # 1️⃣ Price must be positive
    bad_prices = df.filter("avg_price <= 0").count()
    if bad_prices > 0:
        raise ValueError(f" Found {bad_prices} records with invalid avg_price <= 0")

    # 2️⃣ Volume must be positive
    bad_volume = df.filter("total_volume <= 0").count()
    if bad_volume > 0:
        raise ValueError(f" Found {bad_volume} records with non-positive total_volume")

    # 3️⃣ Weather coverage: >= 80% should have temperature
    total = df.count()
    null_weather = df.filter("avg_temperature IS NULL").count()
    if total > 0 and (null_weather / total) > 0.2:
        raise ValueError(f" More than 20% records missing weather: {null_weather}/{total}")

    print(" Hourly quality checks passed!")
    spark.stop()

if __name__ == "__main__":
    run_quality_checks()