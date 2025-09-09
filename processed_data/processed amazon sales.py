from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("ecommerce_data_processing").getOrCreate()

# Minio(S3-Compatible)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "mini123")
hadoop_conf.set("fs.s3a.path.style.access", "true")


bucket_name = "ecommerce-data"
dataset = ["s3a://" + bucket_name + "/raw_data/Amazon Sale Report.csv"]


