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
    raw_file = "raw_data/Cloud Warehouse Compersion Chart.csv"
    processed_file = "processed_data/cloud_warehouse_cleaned.parquet"

    data = client.get_object(bucket_name, raw_file)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

    df.dropna(how='all', inplace=True)
    df.columns = ['index', 'shiprocket', 'shiprocket_price', 'increff_price']
    df = df[df['shiprocket'] != 'Heads']
    df.reset_index(drop=True, inplace=True)

    def clean_price(value):
        if isinstance(value, str):
            match = re.search(r"[\d.]+", value.replace(',', ''))
            return float(match.group()) if match else np.nan
        return value

    df['shiprocket_price_clean'] = df['shiprocket_price'].apply(clean_price)
    df['increff_price_clean'] = df['increff_price'].apply(clean_price)
    df = df[(df['shiprocket_price_clean'].notna()) | (df['increff_price_clean'].notna())]
    df['price_difference'] = (df['increff_price_clean'] - df['shiprocket_price_clean']).round(2)
    df['increff_cheaper'] = np.where(df['price_difference'] < 0, True, False)

    for col in ['shiprocket_price_clean', 'increff_price_clean']:
        if df[col].notna().any():
            df[f'{col}_normalized'] = (
                (df[col] - df[col].min()) / (df[col].max() - df[col].min())
            ).round(3)

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

    print(f"✅ Cleaned Cloud Warehouse data uploaded to {bucket_name}/{processed_file}")


if __name__ == "__main__":
    clean_cloud_warehouse()
