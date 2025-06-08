from datetime import *
from pathlib import Path
import pickle
import logging
import os
from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from src.config import NASA_NEO_API_KEY, NASA_NEO_URI, BUCKET_NAME, Mode
from src.minio_client import create_minio_client
from src.date_ranges import calculate_missing_dates
from src.neo_api_pipeline import NeoPipelineController

# Add parent directory to Python path
project_dir = Path(__file__).resolve().parent.parent.parent

# Define the pickle file path in a location Airflow can access
PICKLE_FILE_PATH = f"{project_dir}/airflow_temp/missing_dates.pkl"


def generate_missing_dates_pickle():
    """
    This function runs date_ranges.py to generate the missing_date_table
    and saves it as a pickle file for other tasks to use.
    """
    try:
        # Create the temp directory if it doesn't exist
        os.makedirs(os.path.dirname(PICKLE_FILE_PATH), exist_ok=True)

        minio_client = create_minio_client()
        missing_date_table = calculate_missing_dates(datetime.today().date(), minio_client)

        # Save the missing_date_table to a pickle file
        with open(PICKLE_FILE_PATH, 'wb') as f:
            pickle.dump(missing_date_table, f)

        logging.info(f"Successfully saved {len(missing_date_table)} date ranges to pickle file")
        logging.info(f"Date ranges: {missing_date_table}")

        # Return the count for downstream task decisions
        return len(missing_date_table)

    except Exception as e:
        logging.error(f"Error generating missing dates pickle: {e}")
        # Save an empty list as fallback
        with open(PICKLE_FILE_PATH, 'wb') as f:
            pickle.dump([], f)
        return 0


def load_missing_dates_and_process_bronze():
    """
    Load the pickled missing_date_table and process all bronze tasks.
    """
    try:
        # Load the missing_date_table from pickle file
        with open(PICKLE_FILE_PATH, 'rb') as f:
            missing_date_table = pickle.load(f)

        logging.info(f"Loaded {len(missing_date_table)} date ranges from pickle file")

        if len(missing_date_table) == 0:
            logging.info("No missing dates to process in bronze layer")
            return "no_data_processed"

        # Process each date range
        processed_ranges = []
        for i, (date_start, date_end) in enumerate(missing_date_table):
            logging.info(
                f"Processing bronze for date range {i + 1}/{len(missing_date_table)}: {date_start} to {date_end}")

            # Connect to Minio blob storage
            minio_client = create_minio_client()

            # Initialize NeoApiClient
            neo_client = NeoPipelineController(mode=Mode.BRONZE,
                                               storage=minio_client,
                                               bucket_name=BUCKET_NAME,
                                               api_key=NASA_NEO_API_KEY,
                                               api_uri=NASA_NEO_URI,
                                               start_date=date_start,
                                               end_date=date_end)

            # Execute ETL pipeline task based on mode
            neo_client.extract().transform().load()

            processed_ranges.append(f"{date_start}_{date_end}")

        logging.info(f"Completed bronze processing for {len(processed_ranges)} date ranges")
        return None

    except Exception as e:
        logging.error(f"Error in bronze processing: {e}")
        raise


def load_missing_dates_and_process_silver():
    """
    Load the pickled missing_date_table and process all silver tasks.
    """
    try:
        # Load the missing_date_table from pickle file
        with open(PICKLE_FILE_PATH, 'rb') as f:
            missing_date_table = pickle.load(f)

        logging.info(f"Loaded {len(missing_date_table)} date ranges from pickle file")

        if len(missing_date_table) == 0:
            logging.info("No missing dates to process in silver layer")
            return "no_data_processed"

        # Process each date range
        processed_ranges = []
        for i, (date_start, date_end) in enumerate(missing_date_table):
            logging.info(
                f"Processing silver for date range {i + 1}/{len(missing_date_table)}: {date_start} to {date_end}")

            # Connect to Minio blob storage
            minio_client = create_minio_client()

            # Initialize NeoApiClient
            neo_client = NeoPipelineController(mode=Mode.SILVER,
                                               storage=minio_client,
                                               bucket_name=BUCKET_NAME,
                                               start_date=date_start,
                                               end_date=date_end)

            # Execute ETL pipeline task based on mode
            neo_client.extract().transform().load()

            processed_ranges.append(f"{date_start}_{date_end}")

        logging.info(f"Completed silver processing for {len(processed_ranges)} date ranges")
        return None

    except Exception as e:
        logging.error(f"Error in silver processing: {e}")
        raise


def process_gold_layer():
    """
    Process the gold layer to create the final analytical dataset.
    """
    try:
        # Connect to Minio blob storage
        minio_client = create_minio_client()

        # Initialize NeoApiClient
        neo_client = NeoPipelineController(mode=Mode.GOLD,
                                           storage=minio_client,
                                           bucket_name=BUCKET_NAME)

        # Execute ETL pipeline task based on mode
        neo_client.extract().transform().load()

        logging.info("Completed gold layer processing")
        return "gold_processed"

    except Exception as e:
        logging.error(f"Error in gold processing: {e}")
        raise


def cleanup_pickle_file():
    """
    Clean up the temporary pickle file after processing is complete.

    This keeps the airflow directory clean and prevents stale data issues.
    """
    try:
        if os.path.exists(PICKLE_FILE_PATH):
            os.remove(PICKLE_FILE_PATH)
            logging.info(f"Successfully cleaned up pickle file: {PICKLE_FILE_PATH}")
        else:
            logging.info("Pickle file already cleaned up or never existed")
    except Exception as e:
        logging.error(f"Error cleaning up pickle file: {e}")
        # Don't raise here as this is cleanup - we don't want to fail the whole pipeline


# NASA NEO API Pipeline DAG
with DAG(
        dag_id="NeoAPIPipeline2",
        description="NASA NEO ETL Pipeline with runtime dynamic data loading",
        start_date=datetime(2025, 1, 1),
        schedule=None,  # "@daily",
        catchup=False,
        tags={'nasa', 'neo', 'data-pipeline'},
        doc_md="""
    ## NASA NEO ETL Pipeline

    This DAG processes Near Earth Object data from NASA's API :

    1. **Generate Missing Dates**: Runs date_ranges.py logic and saves results to pickle file
    2. **Bronze Processing**: Loads date ranges and processes all bronze tasks sequentially  
    3. **Silver Processing**: Loads date ranges and processes all silver tasks sequentially
    4. **Gold Processing**: Creates final analytical dataset
    5. **Cleanup**: Removes temporary pickle file
    """
) as dag:
    # Task 1: Generate the missing dates pickle file
    generate_dates_task = PythonOperator(
        task_id='generate_missing_dates_pickle',
        python_callable=generate_missing_dates_pickle,
        doc_md="""
        Executes the date_ranges.py logic and saves the missing_date_table to a pickle file.
        This moves the dynamic data loading from DAG parse-time to runtime.
        """
    )

    # Task 2: Process all bronze tasks in sequence
    bronze_processing_task = PythonOperator(
        task_id='process_all_bronze_data',
        python_callable=load_missing_dates_and_process_bronze,
        doc_md="""
        Loads the pickled missing_date_table and processes all bronze data extraction
        tasks sequentially. This consolidates what used to be multiple dynamic tasks.
        """
    )

    # Task 3: Process all silver tasks in sequence
    silver_processing_task = PythonOperator(
        task_id='process_all_silver_data',
        python_callable=load_missing_dates_and_process_silver,
        doc_md="""
        Loads the pickled missing_date_table and processes all silver data transformation
        tasks sequentially. This consolidates what used to be multiple dynamic tasks.
        """
    )

    # Task 4: Process gold layer
    gold_processing_task = PythonOperator(
        task_id='process_gold_data',
        python_callable=process_gold_layer,
        doc_md="""
        Creates the final analytical dataset from all processed silver data.
        """
    )

    # Task 5: Cleanup the pickle file
    cleanup_task = PythonOperator(
        task_id='cleanup_pickle_file',
        python_callable=cleanup_pickle_file,
        trigger_rule='all_done',  # Run even if upstream tasks fail
        doc_md="""
        Cleans up the temporary pickle file regardless of whether the pipeline
        succeeded or failed. Uses 'all_done' trigger rule for reliable cleanup.
        """
    )

    # Define the task dependencies
    generate_dates_task >> bronze_processing_task >> silver_processing_task >> gold_processing_task >> cleanup_task