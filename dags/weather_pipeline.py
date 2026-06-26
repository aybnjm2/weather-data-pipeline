from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ayb',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# s3a and Spark config —> all here so both driver and executor get them
SPARK_CONF = {
    "spark.driver.bindAddress": "0.0.0.0",

    # MinIO / S3A
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.executorEnv.AWS_EC2_METADATA_DISABLED": "true",
}

PKG_BASE = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
PKG_DB = f"{PKG_BASE},org.postgresql:postgresql:42.6.0"

def get_spark_operator(task_id, script_path, packages):
    return SparkSubmitOperator(
        task_id=task_id,
        application=script_path,
        conn_id="conn_spark",
        packages=packages,
        conf=SPARK_CONF,
        executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        verbose=True,
    )

with DAG(
    dag_id='weather_medallion_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start_pipeline')
    ingest_bronze = EmptyOperator(task_id='ingest_api_to_bronze')

    to_silver = get_spark_operator("transform_bronze_to_silver", "/opt/airflow/scripts/transform_silver.py", PKG_BASE)
    to_gold   = get_spark_operator("transform_silver_to_gold",   "/opt/airflow/scripts/transform_gold.py",   PKG_DB)
    quality   = get_spark_operator("data_quality",               "/opt/airflow/scripts/data_quality.py",     PKG_BASE)

    end = EmptyOperator(task_id='end_pipeline')

    start >> ingest_bronze >> to_silver >> to_gold >> quality >> end
