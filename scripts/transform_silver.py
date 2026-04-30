from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, explode
import os

# init SparkSession plus connecteurs S3 (MinIO)
spark = SparkSession.builder \
    .appName("Weatherstack_Bronze_to_Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# desactiver les logs INFO pour voir plus clair
spark.sparkContext.setLogLevel("WARN")

print("debut du traitement silver")

# 2. Lecture des données Bronze (generees par Airbyte)
# Modifiez le chemin selon la structure exacte générée par Airbyte dans le bucket 'bronze'
bronze_path = "s3a://bronze/weather_raw/forecast/"

try:
    df_bronze = spark.read.json(bronze_path)
except Exception as e:
    print(f"Error reading from {bronze_path}")
    print(str(e))
    print(e._jvm_exception.getMessage() if hasattr(e, '_jvm_exception') else "No JVM info")
    raise e

# 3. aplatissement (Flattening) et Sélection
df_flattened = df_bronze.select(
    col("_airbyte_data.location.name").alias("city"),
    col("_airbyte_data.location.country").alias("country"),
    col("_airbyte_data.location.lat").cast("float").alias("latitude"),
    col("_airbyte_data.location.lon").cast("float").alias("longitude"),
    col("_airbyte_data.location.localtime").alias("local_time"),
    col("_airbyte_data.current.temperature").cast("float").alias("temperature_celsius"),
    col("_airbyte_data.current.humidity").cast("int").alias("humidity_percent"),
    col("_airbyte_data.current.wind_speed").cast("float").alias("wind_speed_kmh"),
    col("_airbyte_data.current.weather_descriptions").getItem(0).alias("weather_description")
)

# 4. nettoyage et typage (transformation)
df_silver = df_flattened \
    .withColumn("local_time", to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm")) \
    .withColumn("processed_at", current_timestamp()) \
    .dropDuplicates(["city", "local_time"]) \
    .dropna(subset=["city", "temperature_celsius"]) # supprimer si ces valeurs vitales sont nulles

print("aperçu des données nettoyees (Silver) :")
df_silver.show(5)

# 5. ecriture vers la couche Silver dans MinIO au format Parquet
silver_path = "s3a://silver/weather_clean/"

print(f"ecriture des donnees dans {silver_path} ...")
df_silver.write \
    .mode("overwrite") \
    .parquet(silver_path)

print("traitement Silver termine avec succes")

spark.stop()