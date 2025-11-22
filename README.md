# E-commerce Data Pipeline
A simple end-to-end data pipeline for processing E-commerce sales data using Airflow, Minio, DuckDB, PostgreSQL, and Superset.

## Project Summary
* Store raw sales CSV files in minio
* Airflow DAGs extract, clean, and transform the data
* Save clean data in DuckDB
* Load final fact tables into PostgreSQL
* Build dashboards using Superset or Power BI

## Tech Stack
* Python, Pandas
* Apache Airflow
* MinIO (S3 storage)
* DuckDB
* PostgreSQL
* Apache Superset
* Docker Compose

## Pipeline Flow
1. Upload raw CSVs → MinIO
2. Airflow reads raw files
3. Python scripts clean & transform data
4. Save processed data → DuckDB
5. Load fact tables → PostgreSQL
6. Connect Superset / Power BI for dashboards

## Run the Project
```bash
git clone https://github.com/itzzvivek/E-commerce-Sales-Pipeline.git
cd E-commerce-Sales-Pipeline
docker compose up -d
