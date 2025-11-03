from io import BytesIO
import pandas as pd


def validation_parquet(client, bucket_name, input_object, **kwargs):
    pk = kwargs.get('pk')
    smaple_rows = kwargs.get('sample_rows', 5)

    obj = client.get_object(bucket_name, input_object)
    try:
        df = pd.read_parquet(BytesIO(obj.read()), engine='pyarrow')
    finally:
        obj.close()
        obj.release_conn()

    results = {}
    results["rows"] = len(df)
    results["columns"] = df.columns.tolist()
    results["null_counts"] = df.isnull().sum().to_dict()
    results["dtypes"] = df.dtypes.astype(str).to_dict()
    results["sample"] = df.head(sample_rows).to_dict(orient="records")

    return results