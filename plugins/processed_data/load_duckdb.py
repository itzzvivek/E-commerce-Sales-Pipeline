from io import BytesIO
import pandas as pd
import duckdb

print("DuckDB version:", duckdb.__version__)

def load_parquet_to_duckdb(client, bucket_name, input_object, output_db="/opt/airflow/data/ecommerce.duckdb", table_name=None, **kwargs):
    if table_name is None:
        table_name = (
            input_object.split("/")[-1]
            .split(".")[0]
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
    
    obj = client.get_object(bucket_name, input_object)
    try:
        df = pd.read_parquet(BytesIO(obj.read()), engine='pyarrow')
    finally:
        obj.close()
        obj.release_conn()


    conn = duckdb.connect(output_db)
    conn.register("tmp_df", df)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df;")
    conn.unregister("tmp_df")
    conn.close()

    print(f"Loaded {len(df)} rows into DuckDB: {output_db} -> table `{table_name}`")
    return {
        "rows": len(df), 
        "table": table_name,
        "db": output_db
    }