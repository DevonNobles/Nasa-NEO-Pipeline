from pyspark.sql.types import *

# Spark schemas for NASA NEO data processing

class NeoSchemas:
    """Container for all NEO data schemas."""

    @staticmethod
    def velocity_schema() -> StructType:
        """Schema for velocity measurements."""
        return StructType([
            StructField("kilometers_per_hour", StringType(), True),
            StructField("kilometers_per_second", StringType(), True),
            StructField("miles_per_hour", StringType(), True)
        ])

    @staticmethod
    def miss_distance_schema() -> StructType:
        """Schema for miss distance measurements."""
        return StructType([
            StructField("astronomical", StringType(), True),
            StructField("kilometers", StringType(), True),
            StructField("lunar", StringType(), True),
            StructField("miles", StringType(), True)
        ])

    @staticmethod
    def close_approach_schema() -> StructType:
        """Schema for close approach data."""
        return StructType([
            StructField("close_approach_date", StringType(), True),
            StructField("close_approach_date_full", StringType(), True),
            StructField("epoch_date_close_approach", LongType(), True),
            StructField("miss_distance", NeoSchemas.miss_distance_schema(), True),
            StructField("orbiting_body", StringType(), True),
            StructField("relative_velocity", NeoSchemas.velocity_schema(), True)
        ])

    @staticmethod
    def diameter_dimension_schema() -> StructType:
        """Schema for diameter measurements in one unit."""
        return StructType([
            StructField("estimated_diameter_min", DoubleType(), True),
            StructField("estimated_diameter_max", DoubleType(), True)
        ])

    @staticmethod
    def estimated_diameter_schema() -> StructType:
        """Schema for all diameter measurements."""
        return StructType([
            StructField("feet", NeoSchemas.diameter_dimension_schema(), True),
            StructField("kilometers", NeoSchemas.diameter_dimension_schema(), True),
            StructField("meters", NeoSchemas.diameter_dimension_schema(), True),
            StructField("miles", NeoSchemas.diameter_dimension_schema(), True)
        ])

    @staticmethod
    def asteroid_schema() -> StructType:
        """Schema for individual asteroid objects."""
        return StructType([
            StructField("absolute_magnitude_h", DoubleType(), True),
            StructField("close_approach_data", ArrayType(NeoSchemas.close_approach_schema()), True),
            StructField("estimated_diameter", NeoSchemas.estimated_diameter_schema(), True),
            StructField("id", StringType(), True),
            StructField("is_potentially_hazardous_asteroid", BooleanType(), True),
            StructField("is_sentry_object", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("nasa_jpl_url", StringType(), True),
            StructField("neo_reference_id", StringType(), True),
            StructField("sentry_data", StringType(), True)
        ])

    @staticmethod
    def nasa_api_schema() -> StructType:
        """Complete schema for NASA NEO API response."""
        return StructType([
            StructField('element_count', IntegerType(), True),
            StructField('near_earth_objects', MapType(StringType(), ArrayType(NeoSchemas.asteroid_schema())), True)
        ])