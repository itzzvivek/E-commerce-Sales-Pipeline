from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap

def clean_amazon_sales(input_path: str, output_path: str):
    spark = (
    SparkSession.builder
    .appName("AmazonSalesCleaning")
    .config("spark.jars", "/home/itzzvivek/spark_jars/hadoop-aws-3.3.2.jar,/home/itzzvivek/spark_jars/aws-java-sdk-bundle-1.11.1026.jar")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.network.timeout", "60000")
    .config("spark.executor.heartbeatInterval", "10000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .getOrCreate()
)
    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    cols_to_drop = ["index", "Unnamed: 22"]
    df = df.drop(*[c for c in cols_to_drop if c in df.columns])

    df = df.withColumn("Date", col("Date").cast("date")) \
           .withColumn("ship-postal-code", col("ship-postal-code").cast("string")) \
           .withColumn("Amount", col("Amount").cast("double"))

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

    df = df.withColumn("ship-city", initcap(col("ship-city"))) \
           .withColumn("ship-state", initcap(col("ship-state")))

    df.write.mode("overwrite").option("header", True).parquet(output_path)

    print(f"✅ Amazon Sales Report cleaned and saved to {output_path}")


if __name__ == "__main__":
    bucket_name = "ecommerce-data"
    input_file = f"s3a://{bucket_name}/raw_data/Amazon Sale Report.csv"
    output_file = f"s3a://{bucket_name}/processed_data/amazon_sales_cleaned"

    clean_amazon_sales(input_file, output_file)
