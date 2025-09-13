from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap

def clean_amazon_sales(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("AmazonSalesCleaning").getOrCreate()

    # Configure MinIO (S3A)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
    hadoop_conf.set("fs.s3a.access.key", "minio")
    hadoop_conf.set("fs.s3a.secret.key", "minio123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # Read raw CSV from MinIO
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Drop unnecessary columns
    cols_to_drop = ["index", "Unnamed: 22"]
    df = df.drop(*[c for c in cols_to_drop if c in df.columns])

    # Cast columns to correct types
    df = df.withColumn("Date", col("Date").cast("date")) \
           .withColumn("ship-postal-code", col("ship-postal-code").cast("string")) \
           .withColumn("Amount", col("Amount").cast("double"))

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

    # Standardize Status column
    df = df.withColumn(
        "Status",
        when(col("Status") == "Shipped - Delivered to Buyer", lit("Delivered"))
        .when(col("Status") == "Shipped", lit("Shipped"))
        .when(col("Status") == "Cancelled", lit("Cancelled"))
        .otherwise(col("Status"))
    )

    # Format city/state names
    df = df.withColumn("ship-city", initcap(col("ship-city"))) \
           .withColumn("ship-state", initcap(col("ship-state")))

    # Write cleaned data back to MinIO
    df.write.mode("overwrite").option("header", True).parquet(output_path)

    print(f"✅ Amazon Sales Report cleaned and saved to {output_path}")


if __name__ == "__main__":
    bucket_name = "ecommerce-data"
    input_file = f"s3a://{bucket_name}/raw_data/Amazon Sale Report.csv"
    output_file = f"s3a://{bucket_name}/processed_data/amazon_sales_cleaned"

    clean_amazon_sales(input_file, output_file)
