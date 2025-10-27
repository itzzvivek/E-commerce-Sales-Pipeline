from minio import Minio
import pandas as pd
import numpy as np
from io import BytesIO
import re


def clean_cloud_warehouse(input_path=None, output_path=None, **kwargs):
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "ecommerce-data"

    def normalize_path(path: str, default: str) -> str:
        """Remove s3a:// or s3:// prefix if present."""
        if not path:
            return default
        return re.sub(r"^s3a?://[^/]+/", "", path)

    input_path = normalize_path(input_path, "raw_data/Cloud Warehouse Compersion Chart.csv")
    output_path = normalize_path(output_path, "processed_data/cloud_warehouse_cleaned.parquet")

    data = client.get_object(bucket_name, input_path)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

    df.dropna(how="all", inplace=True)

    df.columns = ['index', 'shiprocket', 'shiprocket_price', 'increff_price']
    df = df[df['shiprocket'] != 'Heads']
    df.reset_index(drop=True, inplace=True)

    def clean_price(value):
        """Extracts numeric value from price strings like '₹1,234.50'."""
        if isinstance(value, str):
            value = value.strip().replace(',', '')
            match = re.search(r"\d+(\.\d+)?", value)
            if match:
                try:
                    return float(match.group())
                except ValueError:
                    return np.nan
        if pd.isna(value):
            return np.nan
        try:
            return float(value)
        except Exception:
            return np.nan

    df['shiprocket_price_clean'] = df['shiprocket_price'].apply(clean_price)
    df['increff_price_clean'] = df['increff_price'].apply(clean_price)

    df = df[(df['shiprocket_price_clean'].notna()) | (df['increff_price_clean'].notna())]

    df['price_difference'] = (
        df['increff_price_clean'].fillna(0) - df['shiprocket_price_clean'].fillna(0)
    ).round(2)

    df['increff_cheaper'] = df['price_difference'] < 0

    for col in ['shiprocket_price_clean', 'increff_price_clean']:
        valid = df[col].dropna()
        if not valid.empty:
            df[f'{col}_normalized'] = (
                (df[col] - valid.min()) / (valid.max() - valid.min())
            ).round(3)
        else:
            df[f'{col}_normalized'] = np.nan

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(
        bucket_name,
        output_path,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    print(f"✅ Cleaned Cloud Warehouse data uploaded to {bucket_name}/{output_path}")


if __name__ == "__main__":
    clean_cloud_warehouse()
