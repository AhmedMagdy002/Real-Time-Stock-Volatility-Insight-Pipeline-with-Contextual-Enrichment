from pyspark.sql import SparkSession

# Install boto3 if needed
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3", "-q"])

import boto3

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://localstack:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

# Create bucket
try:
    s3.create_bucket(Bucket='test-bucket')
    print("✅ Bucket created!")
except Exception as e:
    if 'BucketAlreadyExists' in str(e):
        print("ℹ️ Bucket already exists")
    else:
        print(f"Error: {e}")

# Create Spark session
spark = SparkSession.builder \
    .appName("MyJob") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Your processing logic
df = spark.range(100).selectExpr("id", "id * 2 as doubled")
df.write.mode("overwrite").parquet("s3a://test-bucket/output.parquet")

print("✅ Job completed!")

# List files
print("\nFiles in bucket:")
for obj in s3.list_objects_v2(Bucket='test-bucket').get('Contents', []):
    print(f"  - {obj['Key']}")

spark.stop()