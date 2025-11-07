from io import BytesIO
import pandas as pd
import numpy as np

def validation_parquet(client, bucket_name, input_object, **kwargs):
    pk = kwargs.get('pk')
    sample_rows = kwargs.get('sample_rows', 5)

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

    sample = df.head(sample_rows).copy()
    for col in sample.columns:
        if pd.api.types.is_datetime64_any_dtype(sample[col]):
            sample[col] = sample[col].astype(str)
        else:
            sample[col] = sample[col].replace({np.nan: None})
    results["sample"] = sample.to_dict(orient="records")

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
            break
    results["numeric_summary"] = num_stats

    print("=== Validation summary ===")
    print(f"Rows: {results['rows']}")
    print(f"Columns: {len(results['columns'])}")
    print("Null counts (sample):")
    for k, v in list(results["null_counts"].items())[:10]:
        print(f"  {k}: {v}")
    if results["duplicate_pk_count"] is not None:
        print(f"Duplicate pk rows: {results['duplicate_pk_count']}")
    print("Numeric summary keys:", list(num_stats.keys()))
    print("Sample rows:", results["sample"][:1])

    return results
