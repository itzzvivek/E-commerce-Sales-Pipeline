import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import col, when, lit, initcap
from utils.spark import get_spark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_amazon_sales(input_path: str, output_path: str):
    spark = get_spark("AmazonSalesCleaning")

    logger.info(f"Reading data from {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Drop unnecessary columns
    cols_to_drop = ["index", "Unnamed: 22"]
    df = df.drop(*[c for c in cols_to_drop if c in df.columns])

    # Correct data types
    df = (
        df.withColumn("Date", col("Date").cast("date"))
          .withColumn("ship-postal-code", col("ship-postal-code").cast("string"))
          .withColumn("Amount", col("Amount").cast("double"))
    )

    # Fill missing values
    df = df.fillna({
        "Courier Status": "Unknown",
        "fulfilled-by": "Unknown",
        "ship-city": "Unknown",
        "ship-state": "Unknown",
        "ship-country": "Unknown",
        "promotion-ids": "Unknown",
        "currency": "INR",
        "Amount": 0.0
    })

    # Standardize Status
    df = df.withColumn(
        "Status",
        when(col("Status") == "Shipped - Delivered to Buyer", lit("Delivered"))
        .when(col("Status") == "Shipped", lit("Shipped"))
        .when(col("Status") == "Cancelled", lit("Cancelled"))
        .otherwise(col("Status"))
    )

    # Capitalize city/state names
    df = (
        df.withColumn("ship-city", initcap(col("ship-city")))
          .withColumn("ship-state", initcap(col("ship-state")))
    )

    logger.info(f"Writing cleaned data to {output_path}")
    df.write.mode("overwrite").option("header", True).parquet(output_path)

    logger.info(f"✅ Amazon Sales Report cleaned and saved to {output_path}")
    spark.stop()


if __name__ == "__main__":
    bucket_name = os.getenv("MINIO_BUCKET", "ecommerce-data")
    input_file = f"s3a://{bucket_name}/raw_data/Amazon Sale Report.csv"
    output_file = f"s3a://{bucket_name}/processed_data/amazon_sales_cleaned"

    clean_amazon_sales(input_file, output_file)
