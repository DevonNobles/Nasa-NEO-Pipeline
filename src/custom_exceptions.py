# Custom Exception classes
class SparkSessionError(Exception):
    """Raised when Spark session operations fail."""
    pass

class QueueOperationError(Exception):
    """Raised when queue operations fail."""
    pass

class DataExtractionError(Exception):
    """Raised when data extraction fails."""
    pass

class DataLoadError(Exception):
    """Raised when data load operations fail."""
    pass

class MinioConnectionError(Exception):
    """Raised when connection to MinIO fails."""
    pass

class MinioAuthenticationError(Exception):
    """Raised when MinIO authentication fails."""
    pass

class TableAlreadyExistsException(Exception):
    """Raised when table already exists in catalog"""
    pass