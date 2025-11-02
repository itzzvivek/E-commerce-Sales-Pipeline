import pandas as pd
from io import BytesIO

def clean_sales_report(client, bucket_name, input_object, output_object, **kwargs):
    data = client.get_object(bucket_name, input_object)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('-', '_')
        .str.replace('.', '')
    )


    if 'index' in df.columns:
        df = df.drop(columns=['index'])

    df = df.fillna({
        'sku_code': 'UNKNOWN',
        'design_no': 'UNKNOWN',
        'category': 'UNKNOWN',
        'size': 'UNKNOWN',
        'color': 'UNKNOWN',
        'stock': 0
    })

    df['stock'] = pd.to_numeric(df['stock'], errors='coerce').fillna(0).astype(int)
    df['sku_code'] = df['sku_code'].astype(str).str.upper()
    df['design_no'] = df['design_no'].astype(str).str.upper()

    if 'category' in df.columns:
        category_split = df['category'].str.split(':', n=1, expand=True)
        df['main_category'] = category_split[0].str.strip()
        df['sub_category'] = category_split[1].str.strip() if category_split.shape[1] > 1 else 'UNKNOWN'


    df['stock_status'] = df['stock'].apply(lambda x: 'In Stock' if x > 0 else 'Out of Stock')

    df = df.drop_duplicates(subset=['sku_code', 'size', 'color'])

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


if __name__ == "__main__":
    clean_sales_report()