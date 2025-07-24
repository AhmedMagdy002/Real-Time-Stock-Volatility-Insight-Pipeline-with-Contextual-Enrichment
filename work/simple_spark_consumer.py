from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("SimpleTest") \
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

print("✅ Spark session created")

# Read from Kafka
print("📖 Reading from Kafka...")
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and select fields
parsed_df = df.select(
    col("value").cast("string")
).select(
    get_json_object(col("value"), "$.id").alias("id"),
    get_json_object(col("value"), "$.name").alias("name"),
    get_json_object(col("value"), "$.value").alias("value")
)

# Show data
print("\n📊 Data from Kafka:")
parsed_df.show()

# Write to Delta Lake on S3
output_path = "s3a://test-bucket/delta-tables/my_table"

print(f"\n💾 Writing to Delta Lake at: {output_path}")
parsed_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

print("✅ Data saved to Delta Lake!")

# Read back from Delta to verify
print("\n📖 Reading back from Delta Lake:")
delta_df = spark.read.format("delta").load(output_path)
delta_df.show()

print("\n✅ Test completed successfully!")
spark.stop()