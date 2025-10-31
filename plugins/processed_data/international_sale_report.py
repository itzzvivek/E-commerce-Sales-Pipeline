import pandas as pd
from io import BytesIO

def clean_international_sale(client, bucket_name, input_object, output_object, **kwargs):
    data = client.get_object(bucket_name, input_object)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

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
        output_object,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"Cleaned data uploaded to MinIO: {bucket_name}/{processed_file}")
    print(f"Final shape: {df.shape}")

if __name__ == "__main__":
    clean_international_sale()
