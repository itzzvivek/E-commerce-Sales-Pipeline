import pandas as pd
from io import BytesIO

def clean_amazon_sales(client, bucket_name, input_object, output_object, **kwargs):
    data = client.get_object(bucket_name, input_object)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

    print("Data loaded from MinIO. Starting transformations...")

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

    df["Status"] = df["Status"].replace({
        "Shipped - Delivered to Buyer": "Delivered",
        "Shipped": "Shipped",
        "Cancelled": "Cancelled"
    })

    df["ship-city"] = df["ship-city"].str.title()
    df["ship-state"] = df["ship-state"].str.title()

    print("Data cleaning completed. Uploading processed file...")

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(
        bucket_name,
        output_object,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"Cleaned data saved to: s3://{bucket_name}/{output_object}")


if __name__ == "__main__":
    print("This script is intended to be run via Airflow DAG.")
