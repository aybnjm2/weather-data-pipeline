from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

spark = SparkSession.builder \
    .appName("Weather_Bronze_to_Silver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_bronze = spark.read.json("s3a://bronze/weather_raw/forecast/")

df_silver = df_bronze.select(
    col("_airbyte_data.location.name").alias("city"),
    col("_airbyte_data.location.country").alias("country"),
    col("_airbyte_data.location.lat").cast("float").alias("latitude"),
    col("_airbyte_data.location.lon").cast("float").alias("longitude"),
    to_timestamp(col("_airbyte_data.location.localtime"), "yyyy-MM-dd HH:mm").alias("local_time"),
    col("_airbyte_data.current.temperature").cast("float").alias("temperature_celsius"),
    col("_airbyte_data.current.humidity").cast("int").alias("humidity_percent"),
    col("_airbyte_data.current.wind_speed").cast("float").alias("wind_speed_kmh"),
    col("_airbyte_data.current.weather_descriptions").getItem(0).alias("weather_description")
).withColumn("processed_at", current_timestamp()) \
 .dropDuplicates(["city", "local_time"]) \
 .dropna(subset=["city", "temperature_celsius"])

df_silver.write.mode("overwrite").parquet("s3a://silver/weather_clean/")
spark.stop()