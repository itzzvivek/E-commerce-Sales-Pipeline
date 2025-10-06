import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark(app_name="AmazonSalesCleaning"):
    """
    Create a SparkSession configured for MinIO (S3A connector)
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "mini123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367")
        .getOrCreate()
    )

    logger.info("✅ Spark session created successfully.")
    return spark


def clean_amazon_sales(input_path: str, output_path: str):
    """
    Cleans and transforms the Amazon sales dataset.
    """
    spark = get_spark("AmazonSalesCleaning")
    logger.info(f"📥 Reading data from {input_path}")

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.show(5)
    logger.info(f"Data loaded with {df.count()} rows and {len(df.columns)} columns")

    cols_to_drop = ["index", "Unnamed: 22"]
    df = df.drop(*[c for c in cols_to_drop if c in df.columns])


    df = (
        df.withColumn("Date", col("Date").cast("date"))
          .withColumn("ship-postal-code", col("ship-postal-code").cast("string"))
          .withColumn("Amount", col("Amount").cast("double"))
    )


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


    df = df.withColumn(
        "Status",
        when(col("Status") == "Shipped - Delivered to Buyer", lit("Delivered"))
        .when(col("Status") == "Shipped", lit("Shipped"))
        .when(col("Status") == "Cancelled", lit("Cancelled"))
        .otherwise(col("Status"))
    )


    df = (
        df.withColumn("ship-city", initcap(col("ship-city")))
          .withColumn("ship-state", initcap(col("ship-state")))
    )

    logger.info(f"📤 Writing cleaned data to {output_path}")
    (
        df.write.mode("overwrite")
        .option("header", True)
        .parquet(output_path)
    )

    logger.info(f"✅ Amazon Sales Report cleaned & saved successfully at {output_path}")
    spark.stop()



if __name__ == "__main__":
    bucket_name = os.getenv("MINIO_BUCKET", "ecommerce-data")
    input_file = f"s3a://{bucket_name}/raw_data/Amazon Sale Report.csv"
    output_file = f"s3a://{bucket_name}/processed_data/amazon_sales_cleaned"

    clean_amazon_sales(input_file, output_file)
