from minio import Minio
import pandas as pd
from io import BytesIO

def clean_may_2022(input_path=None, output_path=None, **kwargs):
    client = Minio(
        kwargs.get("endpoint", "minio:9000"),
        access_key=kwargs.get("access_key", "minio"),
        secret_key=kwargs.get("secret_key", "minio123"),
        secure=kwargs.get("secure", False)
    )

    bucket_name = kwargs.get("bucket_name", "ecommerce-data")
    raw_file = input_path or "raw_data/May-2022.csv"
    processed_file = output_path or "processed_data/may-2022_cleaned.parquet"

    response = client.get_object(bucket_name, raw_file)
    df = pd.read_csv(BytesIO(response.read()))
    response.close()
    response.release_conn()

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w_]", "", regex=True)
    )

    df = df.dropna(how="all")
    if "order_id" in df.columns:
        df = df.dropna(subset=["order_id"])
    date_cols = [c for c in df.columns if "date" in c.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    num_cols = df.select_dtypes(include=["object"]).columns
    for col in num_cols:
        try:
            df[col] = pd.to_numeric(df[col], errors="ignore")
        except Exception:
            pass

    df = df.drop_duplicates()

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip().str.title()

    if "date" in df.columns:
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

    if {"quantity", "unit_price"}.issubset(df.columns):
        df["total_price"] = df["quantity"] * df["unit_price"]

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

    print(f"Cleaned data saved to MinIO at: {processed_file}")


if __name__ == "__main__":
    clean_may_2022()
