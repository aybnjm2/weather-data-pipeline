from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Weather_Data_Quality") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("s3a://silver/weather_clean/")

# Count all errors across tests
errors = 0
errors += df.filter((col("temperature_celsius") < -50) | (col("temperature_celsius") > 60)).count()
errors += df.filter(col("latitude").isNull() | col("longitude").isNull()).count()
errors += df.filter((col("humidity_percent") < 0) | (col("humidity_percent") > 100)).count()
errors += df.groupBy("city", "local_time").count().filter(col("count") > 1).count()

if errors > 0:
    raise Exception(f"DATA QUALITY FAILED: {errors} records failed the validation checks.")

spark.stop()