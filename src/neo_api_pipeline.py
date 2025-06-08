import requests
import json
from io import BytesIO
from typing import Self
import pyspark
from transformations import *
from queue_manager import queue
from spark_session import *

logger = logging.getLogger(__name__)


class NeoPipelineController:
    # Instantiate current task API client
    def __init__(self,
                 mode,
                 storage,
                 bucket_name,
                 api_key=None,
                 api_uri=None,
                 start_date=None,
                 end_date=None):
        """
        Initialize NASA NEO API client.

        Args:
            api_key: NASA API authentication key
            api_uri: Base URI for NASA NEO API
            start_date: Query start date (YYYY-MM-DD format)
            end_date: Query end date (YYYY-MM-DD format)
            storage: MinIO client instance for data storage
            bucket_name: Target storage bucket name
            mode: Data processing mode ('bronze', 'silver', 'gold')

        Raises:
            ValueError: If date range or mode parameters are invalid
        """

        logger.debug(f"Initializing NeoApiClient for date range {start_date} to {end_date}")

        # set attributes
        self.key = api_key
        self.api_uri = api_uri
        self.start_date = start_date
        self.end_date = end_date
        self.storage = storage
        self.bucket_name = bucket_name
        self.mode = mode

        # Initialize state
        self.data: Optional[dict | pyspark.sql.DataFrame] = None
        self.spark: Optional[SparkSession] = None

        logger.debug(f"NeoApiClient initialized for bucket '{bucket_name}' in {mode} mode")

    # ================================================================================================
    # QUEUE MANAGEMENT METHODS
    # ================================================================================================

    @staticmethod
    def _update_queue(mode: str, object_name: Optional[str] = None) -> Optional[str]:

        """
        Update a persistent queue stored in object storage.

        Args:
            mode: Operation mode - 'in' to add item, 'out' to remove item
            object_name: Item to add to queue (required for 'in' mode)

        Returns:
            str: Item removed from queue (for 'out' mode)
            None: For 'in' mode or when queue is empty

        Raises:
            ValueError: If mode is invalid or object_name missing for 'in' mode
            QueueOperationError: If storage operations fail
        """

        # Validate inputs
        if mode not in {'in', 'out'}:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'in' or 'out'")

        if mode == 'in' and object_name is None:
            raise ValueError("object_name is required for 'in' mode")

        logger.debug(f"Queue operation: mode={mode}, object_name={object_name}")

        try:
            # Select update mode
            # noinspection PyUnreachableCode
            match mode:
                case 'in':
                    queue.add_to_queue(item=object_name)
                    return None

                case 'out':
                    item = queue.get_from_queue()

                    return item

                case _:
                    return None

        except Exception as e:
            logger.error(f"Queue operation failed: mode={mode}, error={e}")
            raise QueueOperationError(f"Failed to {mode} queue: {e}")

    # ================================================================================================
    # UPSERT LOGIC
    # ================================================================================================

    @staticmethod
    def upsert_to_iceberg_table(df, table_name, key_columns, spark):
        """
        Performs upsert operation on Iceberg table using merge functionality.

        Args:
            df: Source DataFrame with new/updated data
            table_name: Target Iceberg table name
            key_columns: List of columns to match on (like primary keys)
            spark: a pyspark.SparkSession for data processing
        """

        if not spark.catalog.tableExists(table_name):
            df.writeTo(table_name).create()
            print(f"Created new table {table_name} with initial data")
            return

        else:
            # Create a temporary view of our source data for the merge operation
            temp_view_name = f"temp_source_{table_name.split('.')[-1]}"
            df.createOrReplaceTempView(temp_view_name)

            # Build the merge condition based on key columns
            key_conditions = " AND ".join([f"target.{col_name} = source.{col_name}" for col_name in key_columns])

            # Update all columns except the key columns
            all_columns = df.columns
            update_columns = [col_name for col_name in all_columns if col_name not in key_columns]

            # Build the UPDATE SET clause
            update_set_clause = ", ".join([f"target.{col_name} = source.{col_name}" for col_name in update_columns])

            # Build the INSERT VALUES clause
            all_columns = df.columns
            insert_columns = ", ".join(all_columns)
            insert_values = ", ".join([f"source.{col_name}" for col_name in all_columns])

            # Construct the complete MERGE statement
            merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING {temp_view_name} AS source
            ON {key_conditions}
            WHEN MATCHED THEN 
                UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns}) VALUES ({insert_values})
            """

            # Execute the merge operation
            spark.sql(merge_sql)
            print(f"Completed upsert operation on {table_name}")

            # Clean up the temporary view
            spark.catalog.dropTempView(temp_view_name)

    # ================================================================================================
    # DATA PROCESSING METHODS - EXTRACT
    # ================================================================================================

    def extract(self) -> Self:
        """
        Extract data based on processing mode.

        Bronze: Fetches from NASA NEO API
        Silver: Reads from bronze storage bucket
        Gold: Queries silver Iceberg tables

        Returns:
            Self for method chaining

        Raises:
            DataExtractionError: When extraction fails for any mode
            ValueError: When mode is invalid
        """
        logger.info(f"Starting data extraction for mode: {self.mode}")

        # Select API Client mode
        # noinspection PyUnreachableCode
        match self.mode:
            case 'bronze':  # Extract from source: NASA NEO API request
                self._extract_from_api()

            case 'silver':  # Extract from source: neo/bronze/
                self._extract_from_bronze_storage()

            case 'gold':  # Extract from source: neo/silver/
                self._extract_from_silver_tables()

            case _:
                raise ValueError(f"Invalid extraction mode: {self.mode}")

        logger.info(f"Data extraction completed successfully for mode: {self.mode}")
        return self


    def _extract_from_api(self) -> None:
        """Extract raw data from NASA NEO API (Bronze mode)."""
        # Generate API request uri
        full_uri = f'{self.api_uri}start_date={self.start_date}&end_date={self.end_date}&api_key={self.key}'

        try:
            logger.debug(f"Making API request to: {full_uri}")

            response = requests.get(full_uri, timeout=5)
            response.raise_for_status()  # Raises HTTPError for bad responses

            # Convert JSON to bytes
            json_bytes = json.dumps(response.json()).encode('utf-8')

            # Store data as BytesIO object
            self.data = BytesIO(json_bytes)
            logger.info(f"Successfully extracted {len(json_bytes)} bytes from NASA API")

        except requests.exceptions.Timeout:
            raise DataExtractionError(f"API request timed out")
        except requests.exceptions.ConnectionError:
            raise DataExtractionError("Failed to connect to NASA API - check network connection")
        except requests.exceptions.HTTPError as e:
            raise DataExtractionError(f"NASA API returned error: {e.response.status_code}")
        except requests.exceptions.RequestException as e:
            raise DataExtractionError(f"API request failed: {e}")
        except json.JSONDecodeError:
            raise DataExtractionError("NASA API returned invalid JSON")


    def _extract_from_bronze_storage(self) -> None:
        """Extract processed data from bronze storage (Silver mode)."""
        logger.debug("Extracting data from bronze storage")

        # Get next item from queue
        obj_name = self._update_queue('out')

        if not obj_name:
            logger.info("No objects available in queue")
            self.data = None
            return

        response = None
        try:
            logger.debug(f"Retrieving object: {obj_name}")
            response = self.storage.get_object(self.bucket_name, obj_name)

            # Read and parse JSON data
            raw_data = response.data.decode('utf-8')
            self.data = json.loads(raw_data)

            logger.info(f"Successfully extracted object '{obj_name}' from bronze storage")

        except json.JSONDecodeError:
            raise DataExtractionError(f"Invalid JSON in bronze object: {obj_name}")
        except Exception as e:
            # Put item back in queue if extraction failed
            if obj_name:
                self._update_queue('in', obj_name)
            raise DataExtractionError(f"Failed to extract from bronze storage: {e}")
        finally:
            # Always clean up HTTP connection
            if response:
                response.close()
                response.release_conn()


    def _extract_from_silver_tables(self) -> None:
        """Extract structured data from silver Iceberg tables (Gold mode)."""
        logger.debug("Extracting data from silver Iceberg tables")

        try:
            # Create Spark session for silver catalog
            self.spark = create_spark_session(self.bucket_name)

            # Execute query
            query = "SELECT * FROM iceberg_catalog.neo_db.silver_asteroids"
            logger.debug(f"Executing query: {query}")
            self.data = self.spark.sql(query)

            # Close SparkSession
            close_spark_session(self.spark)

            # Validate that we got data
            if self.data.count() == 0:
                logger.warning("No data found in silver tables")
            else:
                logger.info(f"Successfully extracted {self.data.count()} records from silver tables")

        except Exception as e:
            raise DataExtractionError(f"Failed to extract from silver tables: {e}")

    # ================================================================================================
    # DATA PROCESSING METHODS - TRANSFORM
    # ================================================================================================

    def transform(self) -> Self:
        """
        Transform data based on processing mode.

        Bronze: Store raw data (no transformation)
        Silver: Convert JSON to structured DataFrame
        Gold: Prepare analytics-ready dataset

        Returns:
            Self for method chaining

        Raises:
            DataTransformationError: When transformation fails
            ValueError: When mode is invalid
        """
        logger.info(f"Starting data transformation for mode: {self.mode}")

        try:
            # noinspection PyUnreachableCode
            match self.mode:
                case 'bronze':
                    # Bronze mode: no transformation, data already extracted
                    logger.info("Bronze mode: No transformation required")

                case 'silver':
                    # Silver mode: JSON to structured DataFrame
                    if not self.data:
                        raise DataTransformationError("No data available for silver transformation")

                    if not self.spark:
                        self.spark = create_spark_session(self.bucket_name)

                    self.data = NeoTransformations.json_to_structured_dataframe(self.data, self.spark)

                case 'gold':
                    # Gold mode: Analytics-ready dataset
                    if not self.data:
                        raise DataTransformationError("No data available for gold transformation")

                    self.data = NeoTransformations.prepare_analytics_dataset(self.data)

                case _:
                    raise ValueError(f"Invalid transformation mode: {self.mode}")

            logger.info(f"Data transformation completed successfully for mode: {self.mode}")
            return self

        except Exception as e:
            logger.error(f"Data transformation failed for mode {self.mode}: {e}")
            raise DataTransformationError(f"Transformation failed: {e}")

    # ================================================================================================
    # DATA PROCESSING METHODS - LOAD
    # ================================================================================================

    def load(self) -> Self:
        """Load data based on processing mode."""
        logger.info(f"Starting data load for mode: {self.mode}")

        try:
            # noinspection PyUnreachableCode
            match self.mode:
                case 'bronze':
                    if not self.data:
                        raise DataLoadError("No data available for bronze load")

                    obj_name = f'{self.mode}/{self.bucket_name}-{self.start_date}_{self.end_date}.json'

                    self.storage.put_object(
                        self.bucket_name,
                        obj_name,
                        self.data,
                        length=len(self.data.getvalue()),
                        content_type='application/json'
                    )

                    self._update_queue('in', obj_name)
                    logger.info(f"Successfully stored {obj_name} and added to queue")

                case 'silver':
                    if not self.data:
                        raise DataLoadError("No data available for silver load")

                    if not self.spark:
                        self.spark = create_spark_session(self.bucket_name)

                    self.spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.neo_db")

                    table = f'iceberg_catalog.neo_db.{self.mode}_asteroids'
                    keys = ['observation_date', 'asteroid_id']

                    self.upsert_to_iceberg_table(self.data, table, keys, self.spark)

                    logger.info(f"Successfully loaded data to silver table")

                    close_spark_session(self.spark)

                case 'gold':
                    if not self.data:
                        raise DataLoadError("No data available for gold load")

                    if not self.spark:
                        self.spark = create_spark_session(self.bucket_name)

                    table = f'iceberg_catalog.neo_db.{self.mode}_asteroids'
                    keys = ['observation_date', 'asteroid_id']

                    self.upsert_to_iceberg_table(self.data, table, keys, self.spark)

                    logger.info(f"Successfully loaded data to gold table")

                    close_spark_session(self.spark)

                case _:
                    raise ValueError(f"Invalid load mode: {self.mode}")

            logger.info(f"Data load completed successfully for mode: {self.mode}")
            return self

        except Exception as e:
            logger.error(f"Data load failed for mode {self.mode}: {e}")
            raise DataLoadError(f"Load failed: {e}")
