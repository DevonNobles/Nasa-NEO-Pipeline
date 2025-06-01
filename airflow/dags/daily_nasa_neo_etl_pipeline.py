from datetime import *
from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from src.config import NASA_NEO_API_KEY, NASA_NEO_URI
from pathlib import Path
from airflow.providers.standard.operators.empty import EmptyOperator
import logging

# Add parent directory to Python path
project_dir = Path(__file__).resolve().parent.parent.parent

# Create template directories paths
base_bronze_dir = Path(f"{project_dir}/notebooks/tmp/bronze/")
base_silver_dir = Path(f"{project_dir}/notebooks/tmp/silver/")
base_gold_dir = Path(f"{project_dir}/notebooks/tmp/gold/")

# Create template directories
base_bronze_dir.mkdir(parents=True, exist_ok=True)
base_silver_dir.mkdir(parents=True, exist_ok=True)
base_gold_dir.mkdir(parents=True, exist_ok=True)

def create_timestamped_directory(directory_path):
    Path(directory_path).mkdir(parents=True, exist_ok=True)

def get_missing_dates():
    try:
        from src.date_ranges import missing_date_table
        logging.info(f"Successfully loaded missing_date_table with {len(missing_date_table)} entries")
        return missing_date_table
    except Exception as e:
        logging.error(f"Unexpected error loading missing_date_table: {e}")
        return []  # Return empty list as fallback

missing_date_table = get_missing_dates()
logging.info(f"missing_date_table contains {len(missing_date_table)} date ranges")
if missing_date_table:
    logging.info(f"Date ranges: {missing_date_table}")

# NASA NEO API Pipeline DAG
with DAG(
    dag_id="NeoAPIPipeline",
    description="For executing the NeoAPIClient in bronze mode: extract from NASA NEO API",
    start_date=datetime(2025, 1, 1),
    schedule=None, # "@daily",
    catchup=False,
    tags={'nasa', 'neo', 'data-pipeline'}
) as dag:

    if len(missing_date_table) == 0:
        logging.info("No missing dates found - creating empty pipeline")
        bronze_task = EmptyOperator(task_id='EmptyBronze')
        silver_task = EmptyOperator(task_id='EmptySilver')
        gold_task = EmptyOperator(task_id='EmptyGold')

        # Set dependencies for empty case
        bronze_task >> silver_task >> gold_task

    else:
        logging.info(f"Processing {len(missing_date_table)} missing date ranges")
        bronze_tasks = []
        silver_tasks = []

        # Directory creation task for gold
        create_gold_dir_task = PythonOperator(
            task_id="create_gold_directory",
            python_callable=create_timestamped_directory,
            op_args=[f"{project_dir}/notebooks/tmp/gold/{{ ts }}"]
        )

        # Create a task for each date range
        for i, (date_start, date_end) in enumerate(missing_date_table):
            task_suffix = f"{i:03d}_{date_start}_{date_end}"

            # Directory creation task for bronze
            create_bronze_dir_task = PythonOperator(
                task_id=f"create_bronze_dir-{task_suffix}",
                python_callable=create_timestamped_directory,
                op_args=[f"{project_dir}/notebooks/tmp/bronze/{{{{ ts }}}}"]
            )

            bronze_task = PapermillOperator(
                task_id=f"Ingest_NASA_NEO_API_data.date_range-{task_suffix}",
                input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
                output_nb= f"{project_dir}/notebooks/tmp/bronze/{{{{ ts }}}}/{task_suffix}-NeoApiClient.ipynb",
                parameters={
                'api_key_param': NASA_NEO_API_KEY,
                'api_uri_param': NASA_NEO_URI,
                'start_date_param': date_start,
                'end_date_param': date_end,
                'bucket_name_param': 'neo',
                'mode': 'bronze'
                }
            )
            bronze_tasks.append(bronze_task)

            # Directory creation task for silver
            create_silver_dir_task = PythonOperator(
                task_id=f"create_silver_dir-{task_suffix}",
                python_callable=create_timestamped_directory,
                op_args=[f"{project_dir}/notebooks/tmp/silver/{{{{ ts }}}}"]
            )

            silver_task = PapermillOperator(
                task_id=f"Convert_raw_JSON_to_DataFrame-{task_suffix}",
                input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
                output_nb=f"{project_dir}/notebooks/tmp/silver/{{{{ ts }}}}/{task_suffix}-NeoApiClient.ipynb",
                parameters={
                    'bucket_name_param': 'neo',
                    'mode': 'silver'
                }
            )
            silver_tasks.append(silver_task)

            # Set individual bronze_task and silver_task dependency
            create_bronze_dir_task >> bronze_task
            create_silver_dir_task >> silver_task
            bronze_task >> silver_task

        gold_task = PapermillOperator(
            task_id="Curate_analytical_dataset",
            input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
            output_nb= f"{project_dir}/notebooks/tmp/gold/{{{{ ts }}}}-NeoApiClient.ipynb",
            parameters={
                'bucket_name_param': 'neo',
                'mode': 'gold'
            }
        )

        create_gold_dir_task >> gold_task
        silver_tasks >> gold_task