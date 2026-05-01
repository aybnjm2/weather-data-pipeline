from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# 1. conf par defaut du DAG
default_args = {
    'owner': 'ayb_nabil',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1), # date debut
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # Si ça plante, Airflow reessaie 1 fois
    'retry_delay': timedelta(minutes=2),
}

# 2. def du DAG
with DAG(
    dag_id='weather_medallion_pipeline',
    default_args=default_args,
    description='Pipeline complet: API -> Bronze -> Silver -> Quality -> Gold',
    schedule_interval='0 6 * * *', # execution tous les jours a 6h00
    catchup=False, # ne pas rattraper les jours passes si on lance le DAG aujourd hui
    tags=['weather', 'medallion_architecture']
) as dag:

    # packages necessaires pour Spark (S3 + PostgreSQL)
    SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0"

    # tache 0 => debut
    start = EmptyOperator(task_id='start_pipeline')

    # tache 1: ingestion Bronze(API)
    # EmptyOperator pour représenter action Airbyte
    # Dans un projet avancé, on utiliserait le AirbyteTriggerSyncOperator.
    ingest_bronze = EmptyOperator(task_id='ingest_api_to_bronze')

    # tache 2: transformation Silver (Nettoyage + Flattening)
    transform_silver = SparkSubmitOperator(
        task_id='transform_bronze_to_silver',
        conn_id='spark_default', # Connexion vers notre Spark Master
        application='/opt/airflow/scripts/transform_silver.py',
        packages=SPARK_PACKAGES,
        name='airflow_silver_layer'
    )

    # tache 3: data quality sur couche silver
    data_quality = SparkSubmitOperator(
        task_id='data_quality_check',
        conn_id='spark_default',
        application='/opt/airflow/scripts/data_quality.py',
        packages=SPARK_PACKAGES,
        name='airflow_data_quality'
    )

    # tache 4: transformation gold
    transform_gold = SparkSubmitOperator(
        task_id='transform_silver_to_gold',
        conn_id='spark_default',
        application='/opt/airflow/scripts/transform_gold.py',
        packages=SPARK_PACKAGES,
        name='airflow_gold_layer'
    )

    # tache 5: fin
    end = EmptyOperator(task_id='end_pipeline')

    # 3. ORCHESTRATION: ordre d execution
    start >> ingest_bronze >> transform_silver >> data_quality >> transform_gold >> end