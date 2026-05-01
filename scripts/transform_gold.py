from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, lower

# 1. Initialisation de Spark
# Remarque: On ajoute le driver PostgreSQL en plus des drivers AWS/S3
spark = SparkSession.builder \
    .appName("Weatherstack_Silver_to_Gold") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("debut du traitement Gold")

# 2. Lecture des données Silver (Format Parquet)
silver_path = "s3a://silver/weather_clean/"

try:
    df_silver = spark.read.parquet(silver_path)
except Exception as e:
    print(f"Erreur de lecture Silver: {e}")
    spark.stop()
    exit(1)

# 3. Enrichissement (Transformations Métier)
df_gold = df_silver \
    .withColumn("observation_date", to_date(col("local_time"))) \
    .withColumn("observation_time", date_format(col("local_time"), "HH:mm:ss")) \
    .withColumn("temperature_category", 
                when(col("temperature_celsius") < 10, "Froid")
                .when((col("temperature_celsius") >= 10) & (col("temperature_celsius") <= 25), "Modéré")
                .otherwise("Chaud")) \
    .withColumn("is_raining", 
                when(lower(col("weather_description")).like("%rain%") | 
                     lower(col("weather_description")).like("%drizzle%") | 
                     lower(col("weather_description")).like("%shower%"), True)
                .otherwise(False))

# On réorganise les colonnes pour que ce soit joli pour Power BI
df_gold = df_gold.select(
    "city", "country", "latitude", "longitude", 
    "observation_date", "observation_time", 
    "temperature_celsius", "temperature_category", 
    "humidity_percent", "wind_speed_kmh", 
    "weather_description", "is_raining"
)

print("aperçu des donnees enrichies (Gold) :")
df_gold.show(5)

# 4. ecriture dans PostgreSQL
DB_URL = "jdbc:postgresql://postgres:5432/weather"
DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

print("Ecriture des donnees dans PostgreSQL (Table: weather_gold) ...")

# l'option "overwrite" ecrase et recree la table à chaque execution append pour accumulation
df_gold.write.jdbc(
    url=DB_URL,
    table="weather_gold",
    mode="overwrite",
    properties=DB_PROPERTIES
)

print("traitement Gold termine avec succes")
spark.stop()