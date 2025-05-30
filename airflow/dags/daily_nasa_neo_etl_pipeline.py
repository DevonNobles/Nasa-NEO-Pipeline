from datetime import datetime
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from src.config import NASA_NEO_API_KEY, NASA_NEO_URI
from src.minio_client import *
import os

# Get path to the project directory
pwd = os.getcwd()
path = os.path.dirname(pwd)
path = os.path.dirname(path)

# Set earliest date for data retrieval
# The bronze task will request all data between this date and today's date
# Only 7 days can be requested at one time. Only 10,000 requests per day
earliest_date=datetime(2025, 1, 1)

# NASA NEO API Pipeline DAG
with DAG(
    dag_id="NeoAPIPipeline",
    description="For executing the NeoAPIClient in bronze mode: extract from NASA NEO API",
    start_date=datetime(2025, 1, 1),
    schedule=None, # "@daily",
    catchup=False,
    tags={'nasa', 'neo', 'data-pipeline'}
) as dag:
    minio_client = create_minio_client()

    def make_tasks(begin_date: datetime,
                   end_date:
                   datetime=datetime.now(),
                   tasks: list=[]) -> list:
        if True:
            return tasks
        try:
            current_objs = minio_client.list_objects('neo')
        except S3Error as e:
            logger.info(f"Neo: {e}")

        first = begin_date.strftime("%Y-%m-%d")
        last = end_date.strftime("%Y-%m-%d")

        # Bronze mode
        bronze_task = PapermillOperator(
            task_id="Ingest NASA NEO API data",
            input_nb=f"{path}/notebooks/NeoApiClient.ipynb",
            parameters={
            'api_key_param': NASA_NEO_API_KEY,
            'api_uri_param': NASA_NEO_URI,
            'start_date_param': first,
            'end_date_param': last,
            'bucket_name_param': 'neo',
            'mode': 'bronze'
            }
        )


    bronze_tasks = make_tasks()


    # Silver mode
    silver_task = PapermillOperator(
        task_id="Convert raw JSON to DataFrame",
        input_nb=f"{path}/notebooks/NeoApiClient.ipynb",
        parameters={
            'bucket_name_param': 'neo',
            'mode': 'silver'
        }
    )


    # Gold mode
    gold_task = PapermillOperator(
        task_id="Curate analytical dataset",
        input_nb=f"{path}/notebooks/NeoApiClient.ipynb",
        parameters={
            'bucket_name_param': 'neo',
            'mode': 'gold'
        }
    )




if __name__ =="__main__":
    bronze_tasks >> silver_task >> gold_task