import logging
from .schemas import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, col, expr, to_date
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)

# Data transformation functions for NASA NEO data

class DataTransformationError(Exception):
    """Raised when data transformation fails."""
    pass


class NeoTransformations:
    """NASA NEO data transformation utilities."""

    @staticmethod
    def json_to_structured_dataframe(data: dict, spark: SparkSession) -> DataFrame:
        """
        Transform raw NASA NEO JSON data to structured DataFrame.

        Args:
            data: Raw JSON data from NASA NEO API
            spark: Active Spark session

        Returns:
            Structured DataFrame with flattened asteroid data

        Raises:
            DataTransformationError: If transformation fails
        """
        logger.info("Starting JSON to DataFrame transformation")

        try:
            # Validate input data
            NeoTransformations._validate_input_data(data)

            # Create DataFrame with schema
            logger.debug("Creating DataFrame from JSON data")
            df_with_schema = spark.createDataFrame([data], schema=NeoSchemas.nasa_api_schema())

            # Transform in stages
            date_df = NeoTransformations._explode_date_entries(df_with_schema)
            asteroid_df = NeoTransformations._extract_asteroid_properties(date_df)
            approach_df = NeoTransformations._extract_approach_data(asteroid_df)
            final_df = NeoTransformations._convert_data_types(approach_df)

            record_count = final_df.count()
            logger.info(f"Successfully transformed JSON to DataFrame with {record_count} records")

            return final_df

        except Exception as e:
            logger.error(f"JSON to DataFrame transformation failed: {e}")
            raise DataTransformationError(f"Failed to transform JSON data: {e}")

    @staticmethod
    def _validate_input_data(data: dict) -> None:
        """Validate input JSON structure."""
        if not isinstance(data, dict):
            raise DataTransformationError("Input data must be a dictionary")

        if 'near_earth_objects' not in data:
            raise DataTransformationError("Missing 'near_earth_objects' in input data")

        if not isinstance(data['near_earth_objects'], dict):
            raise DataTransformationError("'near_earth_objects' must be a dictionary")

        logger.debug(f"Input validation passed: {len(data['near_earth_objects'])} date entries")

    @staticmethod
    def _explode_date_entries(df: DataFrame) -> DataFrame:
        """Explode the near_earth_objects map to get date and asteroids."""
        logger.debug("Exploding date entries from near_earth_objects map")

        date_df = df.select(explode(expr("map_entries(near_earth_objects)"))).toDF("date_entry")

        return date_df.select(
            col("date_entry.key").alias("observation_date"),
            explode(col("date_entry.value")).alias("asteroid")
        )

    @staticmethod
    def _extract_asteroid_properties(df: DataFrame) -> DataFrame:
        """Extract asteroid properties and diameter measurements."""
        logger.debug("Extracting asteroid properties")

        return df.select(
            col("observation_date"),
            col("asteroid.id").alias("asteroid_id"),
            col("asteroid.neo_reference_id"),
            col("asteroid.name"),
            col("asteroid.nasa_jpl_url"),
            col("asteroid.absolute_magnitude_h"),

            # Diameter measurements in different units
            col("asteroid.estimated_diameter.kilometers.estimated_diameter_min").alias("diameter_min_km"),
            col("asteroid.estimated_diameter.kilometers.estimated_diameter_max").alias("diameter_max_km"),
            col("asteroid.estimated_diameter.meters.estimated_diameter_min").alias("diameter_min_m"),
            col("asteroid.estimated_diameter.meters.estimated_diameter_max").alias("diameter_max_m"),
            col("asteroid.estimated_diameter.miles.estimated_diameter_min").alias("diameter_min_mi"),
            col("asteroid.estimated_diameter.miles.estimated_diameter_max").alias("diameter_max_mi"),
            col("asteroid.estimated_diameter.feet.estimated_diameter_min").alias("diameter_min_ft"),
            col("asteroid.estimated_diameter.feet.estimated_diameter_max").alias("diameter_max_ft"),

            # Risk indicators
            col("asteroid.is_potentially_hazardous_asteroid").alias("is_potentially_hazardous"),
            col("asteroid.is_sentry_object"),

            # Explode approach data
            explode(col("asteroid.close_approach_data")).alias("approach")
        )

    @staticmethod
    def _extract_approach_data(df: DataFrame) -> DataFrame:
        """Extract close approach data and measurements."""
        logger.debug("Extracting close approach data")

        return df.select(
            # Asteroid identification
            col("observation_date"),
            col("asteroid_id"),
            col("neo_reference_id"),
            col("name"),
            col("nasa_jpl_url"),
            col("absolute_magnitude_h"),

            # Size measurements
            col("diameter_min_km"),
            col("diameter_max_km"),
            col("diameter_min_m"),
            col("diameter_max_m"),
            col("diameter_min_mi"),
            col("diameter_max_mi"),
            col("diameter_min_ft"),
            col("diameter_max_ft"),

            # Risk indicators
            col("is_potentially_hazardous"),
            col("is_sentry_object"),

            # Approach timing
            col("approach.close_approach_date"),
            col("approach.close_approach_date_full"),
            col("approach.epoch_date_close_approach"),

            # Velocity measurements
            col("approach.relative_velocity.kilometers_per_second").alias("velocity_kps"),
            col("approach.relative_velocity.kilometers_per_hour").alias("velocity_kph"),
            col("approach.relative_velocity.miles_per_hour").alias("velocity_mph"),

            # Distance measurements
            col("approach.miss_distance.astronomical").alias("miss_distance_au"),
            col("approach.miss_distance.lunar").alias("miss_distance_ld"),
            col("approach.miss_distance.kilometers").alias("miss_distance_km"),
            col("approach.miss_distance.miles").alias("miss_distance_mi"),

            col("approach.orbiting_body")
        )

    @staticmethod
    def _convert_data_types(df: DataFrame) -> DataFrame:
        """Convert string columns to appropriate numeric types."""
        logger.debug("Converting data types")

        # Date conversions
        result_df = df.withColumn("observation_date", to_date(col("observation_date")))
        result_df = result_df.withColumn("close_approach_date", to_date(col("close_approach_date")))

        # Velocity conversions
        velocity_columns = ["velocity_kps", "velocity_kph", "velocity_mph"]
        for col_name in velocity_columns:
            result_df = result_df.withColumn(col_name, col(col_name).cast(DoubleType()))

        # Distance conversions
        distance_columns = ["miss_distance_au", "miss_distance_ld", "miss_distance_km", "miss_distance_mi"]
        for col_name in distance_columns:
            result_df = result_df.withColumn(col_name, col(col_name).cast(DoubleType()))

        return result_df

    @staticmethod
    def prepare_analytics_dataset(df: DataFrame) -> DataFrame:
        """
        Prepare dataset for analytics based on requirements.

        Args:
            df: Input DataFrame with NEO data

        Returns:
            DataFrame optimized for specified analytics use case
        """
        logger.info(f"Preparing analytics dataset")

        try:
            base_columns = [
                "observation_date",
                "asteroid_id",
                "name",
                "close_approach_date",
                "is_potentially_hazardous",
                "diameter_min_km",
                "diameter_max_km",
                "velocity_kph",
                "miss_distance_km",
                "diameter_min_m",
                "diameter_max_m",
                "diameter_min_mi",
                "diameter_max_mi",
                "diameter_min_ft",
                "diameter_max_ft",
                "velocity_kps",
                "velocity_mph",
                "miss_distance_au",
                "miss_distance_ld",
                "miss_distance_mi"
                ]
            return df.select(*base_columns)


        except Exception as e:
            logger.error(f"Analytics dataset preparation failed: {e}")
            raise DataTransformationError(f"Failed to prepare analytics dataset: {e}")