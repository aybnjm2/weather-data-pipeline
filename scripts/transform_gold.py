from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, lower

spark = SparkSession.builder \
    .appName("Weather_Silver_to_Gold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_silver = spark.read.parquet("s3a://silver/weather_clean/")

df_gold = df_silver \
    .withColumn("observation_date", to_date(col("local_time"))) \
    .withColumn("observation_time", date_format(col("local_time"), "HH:mm:ss")) \
    .withColumn("temperature_category", 
                when(col("temperature_celsius") < 10, "Froid")
                .when((col("temperature_celsius") >= 10) & (col("temperature_celsius") <= 25), "Modéré")
                .otherwise("Chaud")) \
    .withColumn("is_raining", lower(col("weather_description")).rlike("rain|drizzle|shower")) \
    .select(
        "city", "country", "latitude", "longitude", 
        "observation_date", "observation_time", 
        "temperature_celsius", "temperature_category", 
        "humidity_percent", "wind_speed_kmh", 
        "weather_description", "is_raining"
    )

df_gold.write.jdbc(
    url="jdbc:postgresql://postgres:5432/weather",
    table="weather_gold",
    mode="overwrite",
    properties={"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}
)

spark.stop()