import logging
from typing import Optional
from pyspark.sql import SparkSession
from src.config import *
from src.custom_exceptions import *

logger = logging.getLogger(__name__)

def create_spark_session(bucket_name: str) -> SparkSession:
    """
    Create a Spark session configured for:
    - Apache Iceberg table format support
    - MinIO S3-compatible object storage access
    - Hadoop S3A filesystem integration

    Returns:
        SparkSession: Configured Spark session ready for data processing

    Raises:
        SparkSessionError: If session creation fails
    """

    # Build configuration
    config = _build_spark_config(bucket_name)

    # Create session with error handling
    return _create_session_with_retry(config)


def _build_spark_config(bucket_name: 'str') -> dict:
    """Build Spark configuration dictionary."""
    logger.debug(f"Building Spark configuration")

    config = {
        # Application
        "spark.app.name": "NASA_NEO_Spark_Session",

        # Iceberg configuration
        "spark.jars.packages": ",".join(["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
                                         "org.apache.hadoop:hadoop-aws:3.3.4",
                                         "org.apache.hadoop:hadoop-common:3.3.4",
                                         "com.amazonaws:aws-java-sdk-bundle:1.12.367",
                                         ]),

        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.type": "hadoop",
        "spark.sql.catalog.iceberg_catalog.warehouse": f"s3a://{bucket_name}/neo_db",
        "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "spark.sql.catalog.iceberg_catalog.s3.endpoint": f"http://{MINIO_ENDPOINT}",
        "spark.sql.defaultCatalog": "iceberg_catalog",

        # Hadoop S3a configuration
        "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
        "spark.hadoop.fs.s3a.access.key": MINIO_ROOT_USER,
        "spark.hadoop.fs.s3a.secret.key": MINIO_ROOT_PASSWORD,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        # Performance Configuration
        "spark.sql.adaptive.enabled": "false",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false"
    }

    logger.debug(f"Built configuration with {len(config)} settings")
    return config


def _create_session_with_retry(config: dict, max_retries: int = 3) -> Optional[SparkSession]:
    """Create Spark session with retry logic and proper error handling."""

    for attempt in range(max_retries):
        try:

            # Build new session
            builder = SparkSession.builder

            # Apply all configuration
            for key, value in config.items():
                builder = builder.config(key, value)

            # Create session
            spark = builder.getOrCreate()

            # Validate session
            _validate_session(spark)

            logger.info(f"Successfully created Spark session: {spark.sparkContext.appName}")

            return spark

        except Exception as e:
            attempt_num = attempt + 1
            logger.warning(f"Spark session creation attempt {attempt_num}/{max_retries} failed: {e}")

            if attempt_num == max_retries:
                logger.error(f"Failed to create Spark session after {max_retries} attempts")
                raise SparkSessionError(f"Could not create Spark session: {e}")

            # Brief pause before retry
            import time
            time.sleep(2 ** attempt)  # Exponential backoff
    return None


def _validate_session(spark: SparkSession) -> None:
    """Validate that the Spark session is working correctly."""
    try:
        # Test basic functionality
        spark.sql("SELECT 1").collect()
        logger.debug("Spark session basic functionality validated")

    except Exception as e:
        logger.error(f"Spark session validation failed: {e}")
        raise SparkSessionError(f"Session validation failed: {e}")


def close_spark_session(spark: SparkSession) -> None:
    """Properly close the Spark session and clean up resources."""
    if spark:
        try:
            logger.info("Closing Spark session...")
            spark.stop()
            logger.info("Spark session closed successfully")
        except Exception as e:
            logger.error(f"Error closing Spark session: {e}")
