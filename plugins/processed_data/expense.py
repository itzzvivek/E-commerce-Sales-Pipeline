from minio import Minio
import pandas as pd
from io import BytesIO

def clean_expense(input_path, output_path, **kwargs):
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key = "minio123",
        secure=False
    )

    bucket_name = "ecommerce-data"
    raw_file = "raw_data/Expense IIGF.csv"
    processed_file = "processed_data/expense_cleaned.parquet"

    data = client.get_object(bucket_name, raw_file)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()



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


if __name__ == "__main__":
    clean_expense()