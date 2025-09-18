from pyspark.sql import SparkSession
import os

def get_spark(app_name="DataCleaning"):
    """Create and return a SparkSession configured for MinIO."""
    jars = os.getenv("SPARK_JARS", "/home/itzzvivek/spark_jars/hadoop-aws-3.3.2.jar,/home/itzzvivek/spark_jars/aws-java-sdk-bundle-1.11.1026.jar")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jars)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    return spark
