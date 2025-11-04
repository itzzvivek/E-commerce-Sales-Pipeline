from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
from urllib.parse import unquote
from minio import Minio


from processed_data.amazon_sales import clean_amazon_sales
from processed_data.cloud_warehouse import clean_cloud_warehouse
from processed_data.expense import clean_expense
from processed_data.international_sale_report import clean_international_sale
from processed_data.may_2022 import clean_may_2022
from processed_data.pl_march2021 import clean_pl_march2021
from processed_data.sale_report import clean_sales_report

from processed_data.validation import validation_parquet

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

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
        print(f"Created bucket: {BUCKET_NAME}")

    for url in GITHUB_FILES:
        file_name = unquote(url.split("/")[-1])
        local_path = f"/tmp/{file_name}"

        try:
            response = requests.get(url)
            response.raise_for_status()

            with open(local_path, "wb") as f:
                f.write(response.content)

            if os.path.getsize(local_path) == 0:
                print(f"Skipping empty file: {file_name}")
                continue

            object_name = f"{RAW_FOLDER}/{file_name}"
            minio_client.fput_object(BUCKET_NAME, object_name, local_path)
            print(f"Uploaded: {file_name} → {RAW_FOLDER}/")

        except Exception as e:
            print(f"Failed to upload {file_name}: {e}")


def run_transformation(clean_func, input_file, output_file):
    """Run a cleaning function with consistent MinIO configuration."""
    input_path = f"{RAW_FOLDER}/{input_file}"
    output_path = f"{PROCESSED_FOLDER}/{output_file}"

    print(f"Starting transformation: {clean_func.__name__}")
    clean_func(
        client=minio_client,
        bucket_name=BUCKET_NAME,    
        input_object=input_path,
        output_object=output_path
    )
    print(f"Transformation complete: {clean_func.__name__}")

def data_validation(input_file, **kwargs):
    input_path = f"{PROCESSED_FOLDER}/{input_file}"
    print(f"Validating data for: {input_file}")
    return validate_parquet(client=minio_client, bucket_name=BUCKET_NAME, input_object=input_path, **kwargs)


with DAG(
    dag_id="github_to_minio_pipeline",
    description="Ecommerce ETL Pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["github", "minio", "data_pipeline"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_github_files",
        python_callable=upload_github_files,
    )

    amazon_sales_task = PythonOperator(
        task_id="transform_amazon_sales",
        python_callable=run_transformation,
        op_args=[clean_amazon_sales, "Amazon Sale Report.csv", "amazon_sales.parquet"],
    )

    cloud_warehouse_task = PythonOperator(
        task_id="transform_cloud_warehouse",
        python_callable=run_transformation,
        op_args=[clean_cloud_warehouse, "Cloud Warehouse Compersion Chart.csv", "cloud_warehouse.parquet"],
    )

    expense_task = PythonOperator(
        task_id="transform_expense",
        python_callable=run_transformation,
        op_args=[clean_expense, "Expense IIGF.csv", "expense.parquet"],
    )

    international_sales_task = PythonOperator(
        task_id="transform_international_sale",
        python_callable=run_transformation,
        op_args=[clean_international_sale, "International sale Report.csv", "international_sales.parquet"],
    )

    may_2022_task = PythonOperator(
        task_id="transform_may_2022",
        python_callable=run_transformation,
        op_args=[clean_may_2022, "May-2022.csv", "may_2022.parquet"]
    )

    pl_march_2021_task = PythonOperator(
        task_id="transform_pl_march_2021.csv",
        python_callable=run_transformation,
        op_args=[clean_pl_march2021, "P  L March 2021.csv", "pl_march_2021.parquet"]
    )

    sale_report_task = PythonOperator(
        task_id = "transform_sale_report",
        python_callable=run_transformation,
        op_args=[clean_sales_report, "Sale Report.csv", "sale_report.parquet"]
    )


    validation_amazon = PythonOperator(
        task_id="validate_amazon_sales",
        python_callable=data_validation,
        op_args=["amazon_sales.parquet", None]
    )

    upload_task >> [amazon_sales_task, cloud_warehouse_task, expense_task, international_sales_task, may_2022_task, pl_march_2021_task, sale_report_task]
    amazon_sales_task >> validation_amazon