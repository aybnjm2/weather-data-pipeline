# Weather Data Pipeline

A data engineering pipeline that ingests weather data from an external API and processes it through a medallion architecture (Bronze → Silver → Gold) using Apache Airflow, Apache Spark, and MinIO.

## What the Project Does

This pipeline fetches weather forecast data from the weatherstack API and transforms it through three layers:

- **Bronze Layer**: Raw JSON data ingested from the API
- **Silver Layer**: Cleaned, flattened, and type-converted data in Parquet format
- **Gold Layer**: Enriched business-ready data stored in PostgreSQL for analytics

The pipeline runs daily at 6:00 AM and includes data quality checks between Silver and Gold layers.

## Why the Project Is Useful

- **Medallion Architecture**: Implements proven data lakehouse patterns for clean data transformation
- **Scalable Processing**: Apache Spark handles large-scale data transformations
- **Data Quality**: Built-in validation checks ensure data integrity
- **Cloud-Native Storage**: MinIO provides S3-compatible object storage
- **Orchestrated Workflows**: Apache Airflow manages pipeline scheduling and dependencies

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Weather   │────▶│   Bronze    │────▶│   Silver    │
│   API       │     │   (JSON)    │     │  (Parquet)  │
└─────────────┘     └─────────────┘     └─────────────┘
                                                │
                                                ▼
                                        ┌─────────────┐
                                        │   Quality   │
                                        │   Checks    │
                                        └─────────────┘
                                                │
                                                ▼
                                        ┌─────────────┐
                                        │    Gold     │
                                        │(PostgreSQL) │
                                        └─────────────┘
```

### Components

| Component | Description | Port |
|-----------|-------------|------|
| Airflow Webserver | Pipeline orchestration UI | 8081 |
| Airflow Scheduler | Pipeline scheduling | - |
| Spark Master | Distributed processing master | 7077 |
| Spark Worker | Distributed processing worker | - |
| MinIO | S3-compatible object storage | 9000 (API), 9001 (Console) |
| PostgreSQL | Relational database for Gold layer | 5432 |

## How Users Can Get Started

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- At least 4GB RAM available

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd weather-data-pipeline
```

2. Create a `.env` file with the following variables:
```bash
# PostgreSQL
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

# MinIO
ACCESS_KEY=
SECRET_KEY=

# Airflow
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=
```

3. Start the infrastructure:
```bash
docker-compose up -d
```

4. Access the services:
   - Airflow UI: http://localhost:8081 (user/pass)
   - MinIO Console: http://localhost:9001 (user/pass)

### Running the Pipeline

The pipeline runs automatically at 6:00 AM daily. To trigger it manually:

1. Open Airflow UI at http://localhost:8081
2. Find the `weather_medallion_pipeline` DAG
3. Click the "Play" button to trigger a run

### Pipeline Tasks

The DAG executes these tasks in sequence:

1. `ingest_api_to_bronze` - Ingests raw data from weather API to Bronze layer
2. `transform_bronze_to_silver` - Cleans and flattens JSON to Parquet
3. `data_quality_check` - Validates data quality on Silver layer
4. `transform_silver_to_gold` - Enriches data and writes to PostgreSQL

## Project Structure

```
weather-data-pipeline/
├── dags/
│   └── weather_pipeline.py      # Airflow DAG definition
├── scripts/
│   ├── transform_silver.py      # Bronze → Silver transformation
│   ├── transform_gold.py        # Silver → Gold transformation
│   └── data_quality.py          # Data quality checks
├── minio_data/
│   ├── bronze/                   # Raw JSON data
│   ├── silver/                  # Cleaned Parquet data
│   └── gold/                    # PostgreSQL data
├── docker-compose.yml           # Infrastructure services
├── Dockerfile.airflow            # Airflow custom image
└── pyproject.toml               # Python dependencies
```

## Data Schema

### Silver Layer (Parquet)

| Column | Type | Description |
|--------|------|-------------|
| city | string | City name |
| country | string | Country code |
| latitude | float | GPS latitude |
| longitude | float | GPS longitude |
| local_time | timestamp | Local observation time |
| temperature_celsius | float | Temperature in °C |
| humidity_percent | int | Humidity percentage (0-100) |
| wind_speed_kmh | float | Wind speed in km/h |
| weather_description | string | Weather condition |
| processed_at | timestamp | Processing timestamp |

### Gold Layer (PostgreSQL)

| Column | Type | Description |
|--------|------|-------------|
| city | string | City name |
| country | string | Country code |
| latitude | float | GPS latitude |
| longitude | float | GPS longitude |
| observation_date | date | Date of observation |
| observation_time | string | Time of observation |
| temperature_celsius | float | Temperature in °C |
| temperature_category | string | Cold/Moderate/Hot |
| humidity_percent | int | Humidity percentage |
| wind_speed_kmh | float | Wind speed in km/h |
| weather_description | string | Weather condition |
| is_raining | boolean | Rain indicator |

## Where Users Can Get Help

- **Airflow Documentation**: https://airflow.apache.org/docs/
- **Spark Documentation**: https://spark.apache.org/docs/
- **MinIO Documentation**: https://min.io/docs/
- **PySpark API**: https://spark.apache.org/docs/latest/api/python/

## Who Maintains and Contributes

This project is maintained by **ayb**.

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

For detailed contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.