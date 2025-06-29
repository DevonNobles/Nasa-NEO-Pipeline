from datetime import *
from typing import Optional
import minio
import pandas as pd
from src.config import BUCKET_NAME, DAY, MONTH, YEAR

# Set earliest date for data retrieval
# The bronze task will request all data between this date and today's date
# Only 7 days can be requested at one time. Only 10,000 requests per day
earliest_date=date(YEAR, MONTH, DAY)


def create_missing_date_list(df: pd.DataFrame, filter_by: Optional[pd.DataFrame]=None) -> list[tuple[str, str]]:
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
    obj_list = client.list_objects(BUCKET_NAME, recursive=True, prefix='bronze/')
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

def calculate_missing_dates(execution_date: date, storage_client) -> list[tuple[str, str]]:
    # DataFrame of all dates between today and the earliest_date
    full_date_range_df = date_table_df(earliest_date, execution_date)

    obj_datetimes_list: list[tuple[date, date]] = get_current_bronze_file_datetimes(storage_client)

    # Create Dataframe of dates stored in 'neo/bronze/'
    existing_date_range_df = pd.DataFrame({'all_dates': []})
    for first, last in obj_datetimes_list:
        partial_date_range_df = date_table_df(first, last)
        if len(existing_date_range_df['all_dates']) == 0:
            existing_date_range_df = partial_date_range_df
        else:
            existing_date_range_df = pd.concat([existing_date_range_df, partial_date_range_df])

    missing_dates = create_missing_date_list(full_date_range_df, existing_date_range_df)
    return missing_dates
