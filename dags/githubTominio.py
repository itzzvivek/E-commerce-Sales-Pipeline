from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from minio import Minio



MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "mini123")



minio_client = Minio(
    MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

GITHUB_FILES = [
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Amazon%20Sale%20Report.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Cloud%20Warehouse%20Compersion%20Chart.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Expense%20IIGF.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/International%20sale%20Report.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/May-2022.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/P%20%20L%20March%202021.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Sale%20Report.csv",
]

def getUpload():
    bucket_name = 'ecommerce-data'

    for url in GITHUB_FILES:
        file_name = url.split("/")[-1]
        local_path = f"/tmp/{file_name}"

        r = requests.get(url)
        r.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(r.content)

        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        minio_client.fput_object(bucket_name, file_name, local_path)
        print(f"Uploaded {file_name} to MinIO '{bucket_name}' bucket.")

with DAG(
    dag_id="github_to_minio_pipeline",
    description="A DAG to upload files from GitHub to MinIO",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce", "minio", "etl"],
) as dag:
    task1 = PythonOperator(
        task_id="upload_github_files",
        python_callable=getUpload
    )

    task1
