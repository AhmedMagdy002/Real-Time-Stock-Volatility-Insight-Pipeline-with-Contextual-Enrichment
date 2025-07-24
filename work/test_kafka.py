from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaTest").getOrCreate()

try:
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "test-topic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    print(f"✅ Success! Found {df.count()} messages in Kafka")
    df.show(5)
except Exception as e:
    print(f"❌ Error: {e}")

spark.stop()