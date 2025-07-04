from calendar import Month
from enum import Enum

# Choose how many weeks of data you want to retrieve
DAY: int = 1
MONTH: int = 5
YEAR: int = 2025

# NASA Open API NEO config
NASA_NEO_URI = 'https://api.nasa.gov/neo/rest/v1/feed?'
NASA_NEO_API_KEY = '<Add NASA OPEN API KEY HERE>'

# MinIO environment variables
MINIO_ROOT_USER = 'minioadmin'
MINIO_ROOT_PASSWORD = 'minioadmin'
MINIO_VOLUMES = '/mnt/data'
MINIO_OPTS = '--console-address :9001'
MINIO_ENDPOINT = 'localhost:9000'
BUCKET_NAME = 'neo'

# Modes for NeoPipelineController
class Mode(Enum):
    BRONZE = 'bronze'
    SILVER = 'silver'
    GOLD = 'gold'

