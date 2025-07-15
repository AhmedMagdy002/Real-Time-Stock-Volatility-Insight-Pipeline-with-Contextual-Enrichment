from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Initialize Spark with Delta Lake
builder = SparkSession.builder \
    .appName("TestDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create sample data
data = [
    {"timestamp": "2025-07-09T00:00:00Z", "symbol": "AAPL", "price": 150.25, "volume": 100},
    {"timestamp": "2025-07-09T00:01:00Z", "symbol": "TSLA", "price": 300.50, "volume": 200}
]
df = spark.createDataFrame(data)

# Write to Delta Lake
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://stock-bucket/delta/stock_trades")

# Read and verify
df_read = spark.read.format("delta").load("s3a://stock-bucket/delta/stock_trades")
df_read.show()

spark.stop()