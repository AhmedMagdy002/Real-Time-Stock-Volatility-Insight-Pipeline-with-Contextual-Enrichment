from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("DeltaWriteTest") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ✅ Create DataFrame
data = [(1, "AAPL"), (2, "MSFT"), (3, "GOOG")]
df = spark.createDataFrame(data, ["id", "ticker"])

# ✅ Write to Delta Lake in S3 bucket via LocalStack
df.write.format("delta").mode("overwrite").save("s3a://stock-data/delta/stock_tickers")

spark.stop()
