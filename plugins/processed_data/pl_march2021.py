from minio import Minio
import pandas as pd
from io import BytesIO

def clean_pl_march2021(client, bucket_name, input_object, output_object, **kwargs):
    data = client.get_object(bucket_name, input_object)
    df = pd.read_csv(BytesIO(data.read()))
    data.close()
    data.release_conn()

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('-', '_')
    )

    if 'index' in df.columns:
        df = df.drop(columns=['index'])

    numeric_cols = [
        'weight', 'tp_1', 'tp_2', 'mrp_old', 'final_mrp_old',
        'ajio_mrp', 'amazon_mrp', 'amazon_fba_mrp', 'flipkart_mrp',
        'limeroad_mrp', 'myntra_mrp', 'paytm_mrp', 'snapdeal_mrp'
    ]


    for col in numeric_cols:
        df[col] = (
            df[col].astype(str)
            .str.replace(',', '')
            .str.replace('₹', '')
            .str.strip()
    )
    df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.fillna({col: 0 for col in numeric_cols})

    df['price_diff_amazon_flipkart'] = df['amazon_mrp'] - df['flipkart_mrp']
    df['avg_mrp'] = df[[
    'ajio_mrp', 'amazon_mrp', 'amazon_fba_mrp', 'flipkart_mrp',
    'limeroad_mrp', 'myntra_mrp', 'paytm_mrp', 'snapdeal_mrp'
    ]].mean(axis=1)

    df = df.drop_duplicates()

    df['sku'] = df['sku'].astype(str)
    df['style_id'] = df['style_id'].astype(str)
    df['catalog'] = df['catalog'].astype(str)
    df['category'] = df['category'].astype(str)


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
    clean_pl_march2021()