from datetime import datetime
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from src.config import NASA_NEO_API_KEY, NASA_NEO_URI
from src.minio_client import *
import os
import logging

logging.basicConfig(filename='NeoPipeline.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)

pwd = os.getcwd()
path = os.path.dirname(pwd)
path = os.path.dirname(path)


# NASA NEO API Pipeline DAG
with DAG(
    dag_id="NeoAPIPipeline",
    description="For executing the NeoAPIClient in bronze mode: extract from NASA NEO API",
    start_date=datetime(2025, 1, 1),
    schedule=None, # "@daily",
    catchup=False,
    tags={'nasa', 'neo', 'data-pipeline'}
) as dag:

    def make_tasks() -> list:
        # Bronze mode
        bronze_task = PapermillOperator(
            task_id="Ingest NASA NEO API data",
            input_nb=f"{path}/notebooks/NeoApiClient.ipynb",
            parameters={
            'api_key_param': NASA_NEO_API_KEY,
            'api_uri_param': NASA_NEO_URI,
            'start_date_param': '2025-05-02',
            'end_date_param': '2025-05-09',
            'bucket_name_param': 'neo',
            'mode': 'bronze'
            }
        )

        return [bronze_task]

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
    minio_client = create_minio_client()
    bronze_tasks >> silver_task >> gold_task