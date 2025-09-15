from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, initcap

def Expense(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("ExpenseCleaning").getOrCreate()

    # Configure MinIO (S3A)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
    hadoop_conf.set("fs.s3a.access.key", "minio")
    hadoop_conf.set("fs.s3a.secret.key", "minio123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # Read raw CSV from MinIO
    df = spark.read.csv(input_path, header=True, inferSchema=True)