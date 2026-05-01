from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialisation
spark = SparkSession.builder \
    .appName("Weatherstack_Data_Quality") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("debut des tests de qualite des donnees (Couche Silver)")

# 2 lecture donnees silver
silver_path = "s3a://silver/weather_clean/"
df = spark.read.parquet(silver_path)
total_rows = df.count()
print(f"Nombre total de lignes a tester : {total_rows}")

errors = 0  # compteur erreurs

# test 1 => temp abberante
invalid_temp = df.filter((col("temperature_celsius") < -50) | (col("temperature_celsius") > 60)).count()
if invalid_temp > 0:
    print(f"ECHEC TEST 1 : {invalid_temp} ligne(s) ont une température aberrante.")
    errors += 1
else:
    print("SUCCES TEST 1 : Toutes les temperatures sont coherentes.")

# test 2 => coordonnees GPS manquantes
missing_gps = df.filter(col("latitude").isNull() | col("longitude").isNull()).count()
if missing_gps > 0:
    print(f"ECHEC TEST 2 : {missing_gps} une ou plusieurs lignes n ont pas de coordonnees GPS.")
    errors += 1
else:
    print("SUCCES TEST 2 : Aucun point GPS manquant.")

# test 3 => humidite entre 0 et 100 (hors limite pusique c est un pourcentage)
invalid_humidity = df.filter((col("humidity_percent") < 0) | (col("humidity_percent") > 100)).count()
if invalid_humidity > 0:
    print(f"ECHEC TEST 3 : {invalid_humidity} une ou plusieurs lignes ont une humidite incorrecte.")
    errors += 1
else:
    print("SUCCES TEST 3 : Les pourcentages d humidite sont valides.")

# test 4 => pas de doublons (même ville, même heure)
# Si on regroupe par ville et heure on ne devrait avoir quune ligne par groupe
duplicates = df.groupBy("city", "local_time").count().filter(col("count") > 1).count()
if duplicates > 0:
    print(f"ECHEC TEST 4 : {duplicates} doublons trouves (meme ville, meme heure).")
    errors += 1
else:
    print("SUCCES TEST 4 : Aucune donnee dupliquee.")

# 3. Bilan
print("\nBilan de la qualite des donnees")
if errors > 0:
    print(f"ATTENTION : Le dataset contient {errors} erreur(s) de qualite.")
    # Dans un vrai projet Airflow, on ferait un `raise Exception` ici pour bloquer la suite du pipeline
    # raise Exception("Data Quality Check Failed")
else:
    print("PARFAIT : Le dataset a passe tous les tests avec succes Pret pour la couche Gold.")

spark.stop()