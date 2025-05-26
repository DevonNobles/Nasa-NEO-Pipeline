from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, expr, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, IntegerType
from pyspark.sql.types import  ArrayType, MapType
from config import *
import logging

logging.basicConfig(filename='NeoPipeline.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO connection details
minio_endpoint = "localhost:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
warehouse = "neo/silver"

all_jars = f'jars/{ICEBERG_JAR}, jars/{HADOOP_JAR}, jars/{AWS_JAVA_SDK_JAR}'

# Configure Spark with Iceberg and MinIO S3A support
spark = SparkSession.builder \
    .appName("NASA_NEO_Data_Transformation") \
    .config('spark.jars', all_jars) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.myminio", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.myminio.type", "hadoop") \
    .config("spark.sql.catalog.myminio.warehouse", f"s3a://{warehouse}/") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS myminio.nasa_neo")

# Verify the database was created
print("Listing databases in catalog:")
spark.sql("SHOW DATABASES IN myminio").show()

# nested structs for velocity
velocity_struct = StructType([
    StructField("kilometers_per_hour", StringType(), True),
    StructField("kilometers_per_second", StringType(), True),
    StructField("miles_per_hour", StringType(), True)
])

# Nested structs for miss distance
miss_distance_struct = StructType([
    StructField("astronomical", StringType(), True),
    StructField("kilometers", StringType(), True),
    StructField("lunar", StringType(), True),
    StructField("miles", StringType(), True)
])

# Nested structs for close approach data
close_approach_struct = StructType([
    StructField("close_approach_date", StringType(), True),
    StructField("close_approach_date_full", StringType(), True),
    StructField("epoch_date_close_approach", LongType(), True),
    StructField("miss_distance", miss_distance_struct, True),
    StructField("orbiting_body", StringType(), True),
    StructField("relative_velocity", velocity_struct, True)
])

# Nested structs for diameter measurements
diameter_dimension_struct = StructType([
    StructField("estimated_diameter_min", DoubleType(), True),
    StructField("estimated_diameter_max", DoubleType(), True)
])

# Nested struct for estimated diameter
estimated_diameter_struct = StructType([
    StructField("feet", diameter_dimension_struct, True),
    StructField("kilometers", diameter_dimension_struct, True),
    StructField("meters", diameter_dimension_struct, True),
    StructField("miles", diameter_dimension_struct, True)
])

# Links struct
links_struct = StructType([
    StructField("next", StringType(), True),
    StructField("previous", StringType(), True),
    StructField("self", StringType(), True)
])

# Asteroid object struct
asteroid_struct = StructType([
    StructField("absolute_magnitude_h", DoubleType(), True),
    StructField("close_approach_data", ArrayType(close_approach_struct), True),
    StructField("estimated_diameter", estimated_diameter_struct, True),
    StructField("id", StringType(), True),
    StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
    StructField("is_sentry_object", BooleanType(), True),
    StructField("links", links_struct, True),
    StructField("name", StringType(), True),
    StructField("nasa_jpl_url", StringType(), True),
    StructField("neo_reference_id", StringType(), True),
    StructField("sentry_data", StringType(), True)
])

# Main schema with dates as keys and arrays of asteroid objects as values
schema = StructType([
    StructField('links', links_struct),
    StructField('element_count', IntegerType()),
    StructField('near_earth_objects', MapType(StringType(), ArrayType(asteroid_struct)))
])

def json_to_iceberg_table(data: dict):
    # Create pyspark.DataFrame
    df_w_schema = spark.createDataFrame([data], schema=schema)

    # Explode the near_earth_objects map to get date and asteroids array
    date_df = df_w_schema.select(explode(expr("map_entries(near_earth_objects)"))).toDF("date_entry")

    # Extract date and asteroids array
    date_df = date_df.select(
        col("date_entry.key").alias("observation_date"),
        explode(col("date_entry.value")).alias("asteroid")
    )

    # Extract asteroid properties
    asteroid_df = date_df.select(
        col("observation_date"),
        col("asteroid.id").alias("asteroid_id"),
        col("asteroid.neo_reference_id"),
        col("asteroid.name"),
        col("asteroid.nasa_jpl_url"),
        col("asteroid.absolute_magnitude_h"),
        col("asteroid.estimated_diameter.kilometers.estimated_diameter_min").alias("diameter_min_km"),
        col("asteroid.estimated_diameter.kilometers.estimated_diameter_max").alias("diameter_max_km"),
        col("asteroid.estimated_diameter.meters.estimated_diameter_min").alias("diameter_min_m"),
        col("asteroid.estimated_diameter.meters.estimated_diameter_max").alias("diameter_max_m"),
        col("asteroid.estimated_diameter.miles.estimated_diameter_min").alias("diameter_min_mi"),
        col("asteroid.estimated_diameter.miles.estimated_diameter_max").alias("diameter_max_mi"),
        col("asteroid.estimated_diameter.feet.estimated_diameter_min").alias("diameter_min_ft"),
        col("asteroid.estimated_diameter.feet.estimated_diameter_max").alias("diameter_max_ft"),
        col("asteroid.is_potentially_hazardous_asteroid"),
        col("asteroid.is_sentry_object"),
        explode(col("asteroid.close_approach_data")).alias("approach")
    )

    # Extract approach data
    approach_df = asteroid_df.select(
        col("observation_date"),
        col("asteroid_id"),
        col("neo_reference_id"),
        col("name"),
        col("nasa_jpl_url"),
        col("absolute_magnitude_h"),
        col("diameter_min_km"),
        col("diameter_max_km"),
        col("diameter_min_m"),
        col("diameter_max_m"),
        col("diameter_min_mi"),
        col("diameter_max_mi"),
        col("diameter_min_ft"),
        col("diameter_max_ft"),
        col("is_potentially_hazardous_asteroid"),
        col("is_sentry_object"),
        col("approach.close_approach_date"),
        col("approach.close_approach_date_full"),
        col("approach.epoch_date_close_approach"),
        col("approach.relative_velocity.kilometers_per_second").alias("velocity_kps"),
        col("approach.relative_velocity.kilometers_per_hour").alias("velocity_kph"),
        col("approach.relative_velocity.miles_per_hour").alias("velocity_mph"),
        col("approach.miss_distance.astronomical").alias("miss_distance_au"),
        col("approach.miss_distance.lunar").alias("miss_distance_ld"),
        col("approach.miss_distance.kilometers").alias("miss_distance_km"),
        col("approach.miss_distance.miles").alias("miss_distance_mi"),
        col("approach.orbiting_body")
    )

    # Convert string columns to appropriate types
    final_df = approach_df.withColumn("observation_date", to_date(col("observation_date")))
    final_df = final_df.withColumn("close_approach_date", to_date(col("close_approach_date")))
    final_df = final_df.withColumn("velocity_kps", col("velocity_kps").cast(DoubleType()))
    final_df = final_df.withColumn("velocity_kph", col("velocity_kph").cast(DoubleType()))
    final_df = final_df.withColumn("velocity_mph", col("velocity_mph").cast(DoubleType()))
    final_df = final_df.withColumn("miss_distance_au", col("miss_distance_au").cast(DoubleType()))
    final_df = final_df.withColumn("miss_distance_ld", col("miss_distance_ld").cast(DoubleType()))
    final_df = final_df.withColumn("miss_distance_km", col("miss_distance_km").cast(DoubleType()))
    final_df = final_df.withColumn("miss_distance_mi", col("miss_distance_mi").cast(DoubleType()))

    return final_df