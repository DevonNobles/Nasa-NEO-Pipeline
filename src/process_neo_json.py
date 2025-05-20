from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, explode_outer, from_json, expr, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, IntegerType
from pyspark.sql.types import  ArrayType, MapType

spark = SparkSession.builder.appName("NASA NEO Data").getOrCreate()

# Create schema
# Nested structs for velocity
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

# Estimated diameter struct with various units
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
neo_struct = StructType([
    StructField("2025-05-03", ArrayType(asteroid_struct), True),
    StructField("2025-05-04", ArrayType(asteroid_struct), True),
    StructField("2025-05-05", ArrayType(asteroid_struct), True),
    StructField("2025-05-06", ArrayType(asteroid_struct), True),
    StructField("2025-05-07", ArrayType(asteroid_struct), True),
    StructField("2025-05-08", ArrayType(asteroid_struct), True),
    StructField("2025-05-09", ArrayType(asteroid_struct), True)
])

schema = StructType([
    StructField('links', links_struct),
    StructField('element_count', IntegerType()),
    StructField('near_earth_objects', MapType(StringType(),ArrayType(asteroid_struct)))
])

def process_neo_json(df):
    # Explode the near_earth_objects map to get date and asteroids array
    date_df = df.select(explode(expr("map_entries(near_earth_objects)"))).toDF("date_entry")

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
    final_df = asteroid_df.select(
        col("observation_date"),
        col("asteroid_id"),
        col("neo_reference_id"),
        col("name"),
        col("nasa_jpl_url"),
        col("absolute_magnitude_h"),
        col("diameter_min_km"),
        col("diameter_max_km"),
        col("is_potentially_hazardous_asteroid"),
        col("is_sentry_object"),
        col("approach.close_approach_date"),
        col("approach.close_approach_date_full"),
        col("approach.epoch_date_close_approach"),
        col("approach.relative_velocity.kilometers_per_second").alias("velocity_km_s"),
        col("approach.relative_velocity.kilometers_per_hour").alias("velocity_kph"),
        col("approach.relative_velocity.miles_per_hour").alias("velocity_mph"),
        col("approach.miss_distance.astronomical").alias("miss_distance_au"),
        col("approach.miss_distance.lunar").alias("miss_distance_ld"),
        col("approach.miss_distance.kilometers").alias("miss_distance_km"),
        col("approach.miss_distance.miles").alias("miss_distance_mi"),
        col("approach.orbiting_body")
    )

    return final_df
