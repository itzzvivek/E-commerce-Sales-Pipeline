from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
from minio import Minio
from urllib.parse import unquote
from processed_data.amazon_sales import clean_amazon_sales


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "mini123")

minio_client = Minio(
    MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

BUCKET_NAME = "ecommerce-data"
RAW_FOLDER = "raw_data"
PROCESSED_FOLDER = "processed_data"

GITHUB_FILES = [
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Amazon%20Sale%20Report.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Cloud%20Warehouse%20Compersion%20Chart.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Expense%20IIGF.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/International%20sale%20Report.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/May-2022.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/P%20%20L%20March%202021.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Sale%20Report.csv",
]

def upload_github_files():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"Created bucket '{BUCKET_NAME}'")

    for url in GITHUB_FILES:
        file_name = unquote(url.split("/")[-1])
        local_path = f"/tmp/{file_name}"

        try:
            r = requests.get(url)
            r.raise_for_status()

            with open(local_path, "wb") as f:
                f.write(r.content)

            if os.path.getsize(local_path) == 0:
                print(f"⚠️ Skipping empty file: {file_name}")
                continue

            object_name = f"{RAW_FOLDER}/{file_name}"
            minio_client.fput_object(BUCKET_NAME, object_name, local_path)
            print(f"✅ Uploaded {file_name} → MinIO/{RAW_FOLDER}/")

        except Exception as e:
            print(f"❌ Failed to process {file_name}: {e}")

def transform_amazon_sales():
    input_path = f"s3a://{BUCKET_NAME}/{RAW_FOLDER}/Amazon Sale Report.csv"
    output_path = f"s3a://{BUCKET_NAME}/{PROCESSED_FOLDER}/amazon_sales.parquet"

    print("🚀 Starting transformation for Amazon Sale Report")
    clean_amazon_sales(input_path, output_path)
    print("✅ Transformation complete and uploaded to MinIO")

with DAG(
    dag_id="github_to_minio_pipeline",
    description="Pipeline: Pull GitHub CSV → MinIO → Transform → Parquet",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["github", "minio", "data_pipeline"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_github_files",
        python_callable=upload_github_files,
    )

    transform_task = PythonOperator(
        task_id="transform_amazon_sales",
        python_callable=transform_amazon_sales,
    )

    upload_task >> transform_task
