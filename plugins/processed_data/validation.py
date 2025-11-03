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

    if pk:
        if isinstance(pk, str):
            pk = [pk]
        dup_count = df.duplicated(subset=pk).sum()
        results["duplicate_pk_count"] = int(dup_count)
    else:
        results["duplicate_pk_count"] = None

    num_stats = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_numeric_dtype(dtype):
            s = df[col]
            num_stats = {
                "min": float(s.min()) if not s.empty else None,
                "max": float(s.max()) if not s.empty else None,
                "mean": float(s.mean()) if not s.empty else None,
                "nulls": int(s.isnull().sum()),
            }
    results["numeric_summary"] = num_stats
    
    return results