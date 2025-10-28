from minio import Minio
import pandas as pd
from io import BytesIO

def clean_international_sale(input_path=None, output_path=None, **kwargs):
    client = Minio(
        kwargs.get("endpoint", "minio:9000"),
        access_key=kwargs.get("access_key", "minio"),
        secret_key=kwargs.get("secret_key", "minio123"),
        secure=kwargs.get("secure", False)
    )

    bucket_name = kwargs.get("bucket_name", "ecommerce-data")
    raw_file = input_path or "raw_data/International sale Report.csv"
    processed_file = output_path or "processed_data/international_sale_cleaned.parquet"

    response = client.get_object(bucket_name, raw_file)
    df = pd.read_csv(BytesIO(response.read()))
    response.close()
    response.release_conn()

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)

    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    num_cols = [c for c in df.columns if any(k in c for k in ["price", "amount", "cost", "revenue", "profit"])]
    for col in num_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", "0")
            .astype(float)
        )

    for col in ["country", "region"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.title().str.strip()

    if {"unit_price", "quantity"} <= set(df.columns):
        df["total_sales"] = df["unit_price"] * df["quantity"]

    if {"total_sales", "cost"} <= set(df.columns):
        df["profit"] = df["total_sales"] - df["cost"]


    df.fillna({
        "country": "Unknown",
        "region": "Unknown",
        "quantity": 0,
        "unit_price": 0.0,
        "total_sales": 0.0
    }, inplace=True)


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

    print(f"Cleaned data uploaded to MinIO: {bucket_name}/{processed_file}")
    print(f"Final shape: {df.shape}")

if __name__ == "__main__":
    clean_international_sale()
