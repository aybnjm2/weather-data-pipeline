from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv(override=True)

# creation session spark avec config MinIO
spark = SparkSession.builder \
    .appName("BronzeToSilver_Weather") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("Spark Session initialisee avec succes avec MinIO")