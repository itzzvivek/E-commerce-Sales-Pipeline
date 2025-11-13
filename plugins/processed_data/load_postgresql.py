import duckdb
import pandas as pd
from sqlalchemy import create_engine

def load_duckdb_to_postgres(
    duckdb_path="/opt/airflow/data/ecommerce.duckdb",
    table_name=None,
    postgres_conn=None,
    postgres_table=None,
    if_exists="replace",
    **kwargs
):
    """
    Load a table from DuckDB into PostgreSQL.
    """

    if table_name is None:
        raise ValueError("`table_name` is required (name of table inside DuckDB).")

    if postgres_conn is None:
        postgres_conn = "postgresql+psycopg2://airflow:airflow@postgres:5432/ecommerce_db"

    conn = duckdb.connect(duckdb_path)

    df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    conn.close()

    if df.empty:
        print(f"⚠️ Table `{table_name}` in DuckDB is empty — skipping load.")
        return {"rows": 0, "table": table_name}

    engine = create_engine(postgres_conn)

    if postgres_table is None:
        postgres_table = table_name

    with engine.begin() as connection:
        df.to_sql(postgres_table, connection, index=False, if_exists=if_exists)

    print(f"✅ Loaded {len(df)} rows from DuckDB → PostgreSQL table `{postgres_table}`")

    return {
        "rows": len(df),
        "source_table": table_name,
        "target_table": postgres_table,
        "status": "success",
    }
