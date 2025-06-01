from datetime import *
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from src.config import NASA_NEO_API_KEY, NASA_NEO_URI
from src.date_ranges import missing_date_table
from pathlib import Path
from airflow.providers.standard.operators.empty import EmptyOperator


# Add parent directory to Python path
project_dir = Path(__file__).resolve().parent.parent.parent

# NASA NEO API Pipeline DAG
with DAG(
    dag_id="NeoAPIPipeline",
    description="For executing the NeoAPIClient in bronze mode: extract from NASA NEO API",
    start_date=datetime(2025, 1, 1),
    schedule=None, # "@daily",
    catchup=False,
    tags={'nasa', 'neo', 'data-pipeline'}
) as dag:
    dag_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if len(missing_date_table) == 0:
        bronze_tasks = EmptyOperator()
        silver_tasks = EmptyOperator()
        gold_task = EmptyOperator()
    else:
        bronze_tasks = []

        # Create a task for each date range
        for date_start, date_end in missing_date_table:

            # create bronze output notebook directory
            bronze_output_nb_path = Path(f"{project_dir}/notebooks/tmp/bronze/{dag_timestamp}/")
            bronze_output_nb_path.mkdir(parents=True, exist_ok=True)

            bronze_task = PapermillOperator(
                task_id=f"Ingest_NASA_NEO_API_data.date_range-{date_start}_{date_end}",
                input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
                output_nb= f"{project_dir}/notebooks/tmp/bronze/{dag_timestamp}/{date_start}_{date_end}-OUT_NeoApiClient.ipynb",
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

        silver_tasks = []

        # Create a task for each date range
        for date_start, date_end in missing_date_table:
            # create silver output notebook directory
            silver_output_nb_path = Path(f"{project_dir}/notebooks/tmp/silver/{dag_timestamp}/{date_start}_{date_end}-OUT_NeoApiClient.ipynb")
            silver_output_nb_path.mkdir(parents=True, exist_ok=True)

            silver_task = PapermillOperator(
                task_id="Convert_raw_JSON_to_DataFrame",
                input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
                output_nb=f"{project_dir}/notebooks/tmp/silver/{dag_timestamp}-OUT_NeoApiClient.ipynb",
                parameters={
                    'bucket_name_param': 'neo',
                    'mode': 'silver'
                }
            )
            silver_tasks.append(silver_task)

        # create gold output notebook directory
        output_nb_path = Path(f"{project_dir}/notebooks/tmp/gold/")
        output_nb_path.mkdir(parents=True, exist_ok=True)

        gold_task = PapermillOperator(
            task_id="Curate_analytical_dataset",
            input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
            output_nb= f"{project_dir}/notebooks/tmp/gold/{dag_timestamp}-OUT_NeoApiClient.ipynb",
            parameters={
                'bucket_name_param': 'neo',
                'mode': 'gold'
            }
        )


    bronze_tasks >> silver_tasks >> gold_task