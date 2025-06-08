import logging
from typing import  Optional
from minio import Minio
from minio.error import S3Error
from src.config import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from src.custom_exceptions import MinioConnectionError, MinioAuthenticationError


logger = logging.getLogger(__name__)

# Connect to Minio blob storage
def create_minio_client(endpoint=MINIO_ENDPOINT,
                        access_key=MINIO_ROOT_USER,
                        secret_key=MINIO_ROOT_PASSWORD,
                        secure=False) -> Optional[Minio]:
    """
    Create and validate a MinIO client connection.

    Initializes a MinIO API client and verifies connectivity by attempting
    to list buckets. This ensures the client is ready for storage operations.

    Args:
        endpoint (str): MinIO server endpoint (e.g., 'localhost:9000')
        access_key (str): Authentication access key/username
        secret_key (str): Authentication secret key/password
        secure (bool): Whether to use HTTPS (True) or HTTP (False)

    Returns:
        Minio: Configured and validated MinIO client instance

    Raises:
        MinioConnectionError: When client creation or connection validation fails
        MinioAuthenticationError: When authentication credentials are invalid
    """

    logger.info(f"Creating MinIO client for endpoint: {endpoint}")

    try:
        # Initialize client
        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        logger.debug("MinIO client instance created successfully")

        # Validate connection
        logger.info("Validating MinIO connection...")
        client.list_buckets()
        logger.info("MinIO connection validated successfully")

        return client

    except S3Error as e:
        logger.error(f"MinIO authentication failed: {e}")
        raise MinioAuthenticationError(f"Authentication failed for {endpoint}: {e}")
    except Exception as e:
        logger.error(f"Failed to connect to MinIO at {endpoint}: {e}")
        raise MinioConnectionError(f"Connection failed for {endpoint}: {e}")
