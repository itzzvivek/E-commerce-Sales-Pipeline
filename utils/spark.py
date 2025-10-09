from pyspark.sql import SparkSession
import os

def get_spark(app_name="DataCleaning"):
    jars = os.getenv(
        "SPARK_JARS",
        ",".join([
            "/home/itzzvivek/spark_jars/hadoop-aws-3.3.6.jar",
            "/home/itzzvivek/spark_jars/hadoop-auth-3.3.6.jar",
            "/home/itzzvivek/spark_jars/hadoop-common-3.3.6.jar",
            "/home/itzzvivek/spark_jars/aws-java-sdk-bundle-1.12.367.jar"
        ])
    )

    access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jars)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )

    return spark
