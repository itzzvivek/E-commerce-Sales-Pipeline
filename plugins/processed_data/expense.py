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

    df.columns = ['index', 'received_date', 'received_amount', 'expense_name', 'expence_amount']

    received_df = df[['received_date', 'received_amount']].dropna().iloc[1:].copy()
    expense_df = df[['expense_name', 'expence_amount']].dropna().iloc[1:].copy()

    received_df['received_date'] = pd.to_datetime(received_df['received_date'], errors='coerce')
    received_df['received_amount'] = pd.to_numeric(received_df['received_amount'], errors='coerce')
    expense_df['expense_amount'] = pd.to_numeric(expense_df['expense_amount'], errors='coerce')

    total_received = received_df['received_amount'].sum()
    total_expense = expense_df['expense_amount'].sum()
    balance = total_received - total_expense

    print(f"💰 Total Received: {total_received:.2f}")
    print(f"💸 Total Expense: {total_expense:.2f}")
    print(f"📈 Balance: {balance:.2f}")

    daily_summary = (
        received_df.groupby('received_date')['received_amount']
        .sum()
        .reset_index()
        .rename(columns={'received_amount': 'total_received'})
    )

    expense_df['category'] = expense_df['expense_name'].apply(
        lambda x: 'Travel' if isinstance(x, str) and ('OLA' in x or 'Auto' in x) else
                  'Food' if isinstance(x, str) and ('Food' in x or 'Hotel' in x) else
                  'Other'
    )

    outputs = {
        "processed_data/expense_received.parquet": received_df,
        "processed_data/expense_summary.parquet": daily_summary,
        "processed_data/expense_details.parquet": expense_df,
    }

    for file_path, df in outputs.items():
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        client.put_object(
            bucket_name,
            file_path,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
    print(f"Expense data clened! ")


if __name__ == "__main__":
    clean_expense()