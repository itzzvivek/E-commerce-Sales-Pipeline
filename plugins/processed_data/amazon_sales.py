from minio import Minio
import pandas as pd
from io import BytesIO

def clean_amazon_sales(input_path, output_path, **kwargs):
    # --- MinIO Config ---
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "ecommerce-data"
    raw_file = "raw_data/Amazon Sale Report.csv"
    processed_file = "processed_data/amazon_sales_cleaned.parquet"

    # --- Step 1: Read CSV from MinIO ---
    data = client.get_object(bucket_name, raw_file)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

    print("📥 Data loaded from MinIO. Starting transformations...")

    # --- Step 2: Clean & Transform ---
    cols_to_drop = ["index", "Unnamed: 22"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0.0)

    df = df.fillna({
        "Courier Status": "Unknown",
        "fulfilled-by": "Unknown",
        "ship-city": "Unknown",
        "ship-state": "Unknown",
        "ship-country": "Unknown",
        "promotion-ids": "Unknown",
        "currency": "INR"
    })

    df["Status"] = (
        df["Status"]
        .replace({
            "Shipped - Delivered to Buyer": "Delivered",
            "Shipped": "Shipped",
            "Cancelled": "Cancelled"
        })
    )

    df["ship-city"] = df["ship-city"].str.title()
    df["ship-state"] = df["ship-state"].str.title()

    print("✨ Data cleaning completed.")

    # --- Step 3: Write cleaned Parquet to MinIO ---
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(
        bucket_name,
        processed_file,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"✅ Cleaned data saved as Parquet: s3://{bucket_name}/{processed_file}")


if __name__ == "__main__":
    clean_amazon_sales()
