from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
from minio import minio


minio_client = minio.Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)


GITHUB_FILES = [
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Amazon%20Sale%20Report.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Cloud%20Warehouse%20Compersion%20Chart.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Expense%20IIGF.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/International%20sale%20Report.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/May-2022.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/P%20%20L%20March%202021.csv",
    "https://raw.githubusercontent.com/itzzvivek/E-commerce-Sales-Pipeline/refs/heads/main/data/Sale%20Report.csv"
]

def getandupload():
    for url in GITHUB_FILES:
        file_name = url.split("/")[-1]
        local_path = f"/tmp/{file_name}"

        r = requests.get.url
        with open(local_path, "wb") as f:
            f.writer(r.content)


        bucket_name = "ecommerce_sales_data"
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        minio_client.fput_objects(bucket_name, file_name, local_path)
        print(f"upload {file_name} to MiniO")

with DAG(
    "github_to_minio_pipeline"
    start_date = datetime(2023,1, 1),
    schedule_interval = "@daily",
    catchup=False,
) as dags,
    task1 = PythonOperator(
        task_id="get_and_upload",
        python_callable=getandupload,
        python_callable=download_and_upload_to_minio
    )

    task1