from config import *
from pyspark.sql import SparkSession
import pyspark

bucket_name = 'neo'
mode = 'gold'

def create_spark_session() -> pyspark.sql.SparkSession:
    all_jars = f'jars/{ICEBERG_JAR}, jars/{HADOOP_JAR}, jars/{AWS_JAVA_SDK_JAR}'

    # Configure Spark with Iceberg and MinIO S3A support
    spark = SparkSession.builder \
        .appName(f"NASA_NEO_{mode.capitalize()}_Data_Transformation") \
        .config('spark.jars', all_jars) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{mode}_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{mode}_catalog.type", "hadoop") \
        .config(f"spark.sql.catalog.{mode}_catalog.warehouse", f"s3a://{bucket_name}/{mode}") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    return spark


cur_spark = create_spark_session()

table = cur_spark.sql(
    """
    SELECT * FROM silver_catalog.silver_neo_db.asteroids
    """
)

table.show()

s = str(10)