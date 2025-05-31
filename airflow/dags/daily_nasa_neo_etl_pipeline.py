from datetime import *
from typing import Optional
import minio
import pandas as pd
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from src.config import NASA_NEO_API_KEY, NASA_NEO_URI
from src.minio_client import create_minio_client
# import sys
from pathlib import Path

# Add parent directory to Python path
project_dir = Path(__file__).resolve().parent.parent.parent
#sys.path.insert(0, str(project_dir))

# Set earliest date for data retrieval
# The bronze task will request all data between this date and today's date
# Only 7 days can be requested at one time. Only 10,000 requests per day
earliest_date=date(2025, 1, 1)

# Connect to MinIO blob storage
minio_client = create_minio_client()

def create_missing_date_list(df: pd.DataFrame, filter_by: pd.DataFrame=None)-> list[tuple[str, str]]:
    # Add a column to group every 7 rows together
    df['group'] = (df.index // 7) + 1

    # Filter out existing dates
    if filter_by is not None:
        df = df[~df['all_dates'].isin(filter_by['all_dates'])]

    # Get the max and min datetime values within each group
    df = df.groupby(df['group']).aggregate({'all_dates': ['max', 'min']})
    min_group_dates = df['all_dates']['min'].apply(lambda x: date.strftime(x, '%Y-%m-%d'))
    max_group_dates = df['all_dates']['max'].apply(lambda x: date.strftime(x, '%Y-%m-%d'))

    # Return list of (min date, max date) tuples
    ranges = list(zip(min_group_dates, max_group_dates))
    return ranges


def get_current_bronze_file_datetimes(client: minio.Minio) -> list[tuple[date, date]]:
    # Retrieve list of names of objects in 'neo/bronze/'
    obj_list = client.list_objects('neo', recursive=True, prefix='bronze/')
    obj_list = [obj.object_name for obj in obj_list]

    # Parse object names to extract start_date and end_date
    parsed_dates = parse_bronze_file_names(obj_list)
    return parsed_dates


def parse_bronze_file_names(object_names: list) -> list[tuple[date, date]]:
    # Create regex expressions for date parsing
    import regex as re
    start_date_expr = r'\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])_'
    end_date_expr = r'\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\.'

    # Parse date strings
    parsed_start_dates = [re.search(start_date_expr, object_name) for object_name in object_names]
    parsed_end_dates = [re.search(end_date_expr, object_name) for object_name in object_names]

    # Convert strings to datetime objects
    start_datetimes = [datetime.strptime(date_string.group()[:-1], '%Y-%m-%d').date() for date_string in parsed_start_dates]
    end_datetimes = [datetime.strptime(date_string.group()[:-1], '%Y-%m-%d').date() for date_string in parsed_end_dates]

    # Combine datetime objects in a list of tuples
    grouped_datetimes = [(start, end) for start, end in zip(start_datetimes, end_datetimes)]
    return grouped_datetimes



def date_table_df(first_date: date, last_date: date) -> pd.DataFrame:
    # Find the delta between today and the earliest date
    delta = last_date - first_date

    # Create a dictionary containing
    # key: 'all_dates'
    # values: a list of  datetime objects, one for each day between today and the earliest date
    data: dict[str, list[date]] = {'all_dates': [(last_date - timedelta(n)) for n in range(delta.days + 1)]}

    # Return sorted pd.DataFrame
    df = pd.DataFrame(data)
    df = df.sort_values('all_dates').reset_index(drop=True)
    return df



# DataFrame of all dates between today and the earliest_date
full_date_range_df = date_table_df(earliest_date, datetime.today().date())

# List of all date ranges already stored in 'neo/bronze/'
obj_datetimes_list = get_current_bronze_file_datetimes(minio_client)

# Create Dataframe of dates stored in 'neo/bronze/'
existing_date_range_df = pd.DataFrame({'all_dates': []})
for first, last in obj_datetimes_list:
    partial_date_range_df = date_table_df(first, last)
    if len(existing_date_range_df['all_dates']) == 0:
        existing_date_range_df = partial_date_range_df
    else:
        existing_date_range_df = pd.concat([existing_date_range_df, partial_date_range_df])

missing_date_table = create_missing_date_list(full_date_range_df, existing_date_range_df)


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

    def make_tasks(date_start: str,
                   date_end: str,
                   date_ranges: list,
                   tasks: Optional[list]=None) -> Optional[list[PapermillOperator]]:
        # Initialize tasks list
        if tasks is None:
            tasks = []

        # Return takes if tasks created for all date ranges
        if len(date_ranges) <= 0:
            return tasks

        # Create a task for each date range
        while True:
            # create output notebook directory
            output_nb_path = Path(f"{project_dir}/notebooks/tmp/bronze/{dag_timestamp}/")
            output_nb_path.mkdir(parents=True, exist_ok=True)

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
            tasks.append(bronze_task)
            start_date, end_date = date_ranges.pop()

            return make_tasks(start_date, end_date, date_ranges, tasks)


    init_start, init_end = missing_date_table.pop()
    bronze_tasks = make_tasks(init_start, init_end, missing_date_table)

    # create output notebook directories
    for mode in ['silver', 'gold']:
        output_nb_path = Path(f"{project_dir}/notebooks/tmp/{mode}/")
        output_nb_path.mkdir(parents=True, exist_ok=True)

    silver_task = PapermillOperator(
        task_id="Convert_raw_JSON_to_DataFrame",
        input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
        output_nb=f"{project_dir}/notebooks/tmp/silver/{dag_timestamp}-OUT_NeoApiClient.ipynb",
        parameters={
            'bucket_name_param': 'neo',
            'mode': 'silver'
        }
    )


    gold_task = PapermillOperator(
        task_id="Curate_analytical_dataset",
        input_nb=f"{project_dir}/notebooks/NeoApiClient.ipynb",
        output_nb= f"{project_dir}/notebooks/tmp/gold/{dag_timestamp}-OUT_NeoApiClient.ipynb",
        parameters={
            'bucket_name_param': 'neo',
            'mode': 'gold'
        }
    )


    bronze_tasks >> silver_task >> gold_task