import pandas as pd
from io import BytesIO

def clean_may_2022(client, bucket_name, input_object, output_object, **kwargs):
    data = client.get_object(bucket_name, input_object)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

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
        output_object,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"Cleaned data saved to MinIO at: {output_object}")


if __name__ == "__main__":
    clean_may_2022()
