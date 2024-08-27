from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from dex_ingestor.connectors.pipedrive.api import extract_pipedrive, create_pipedrive_df
from dex_ingestor.connectors.pipedrive.endpoint_schemas import *
from dex_ingestor.athena.reader import read_data_from_athena
from dex_ingestor.athena.writer import write_pandas_to_table
from dex_ingestor.utils import task_fail_alert

LANDING_PATH = "s3://lakehouse-cannect/landing/pipedrive/v1"

default_args = {
    "owner": "data",
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    "on_failure_callback": task_fail_alert,
}

accounts = [
    "cannect"
]

date_start = "{{ macros.ds_add(ds, 0) }} 00:00:00"
date_end = "{{ macros.ds_add(ds, 1) }} 00:00:00"

aws_hook = AwsBaseHook(aws_conn_id='aws_cannect')
aws_session = aws_hook.get_session()

def get_deals_ids(aws_session, date_start, date_end):
            
    query = f"""
    select distinct
        id
    from
        landing.pipedrive_deals
    where
        date(update_time) >= date('{date_start[:10]}')
        and date(update_time) < date('{date_end[:10]}')
    """

    print(query)

    ids = read_data_from_athena(query=query, aws_session=aws_session, s3_output="s3://cannect-athena-results-68203827/ingestion/")

    ids = [r["id"] for r in ids]

    return ids

extractions = {
    "activities": {
        "request_url": "https://api.pipedrive.com/v1/activities/collection",
        "params": {"limit": 500, "since": date_start, "until": date_end},
        "df_schema": activities_schema
    },
    "deals": {
        "request_url": "https://api.pipedrive.com/v1/deals/collection",
        "params": {"limit": 500, "since": date_start, "until": date_end},
        "df_schema": deals_schema
    },
    "deals_products": {
        "request_url": "https://api.pipedrive.com/api/v2/deals/products",
        "batch_iteration": {"param_name": "deal_ids", "content_function": get_deals_ids, "content_args": [aws_session, date_start, date_end], "batch_size": 100},
        "params": {"limit": 500},
        "df_schema": deals_products_schema
    },
    "deal_fields": {
        "request_url": "https://api.pipedrive.com/v1/dealFields",
        "df_schema": deal_fields_schema
    },
    "persons": {
        "request_url": "https://api.pipedrive.com/v1/persons/collection",
        "params": {"limit": 500, "since": date_start, "until": date_end},
        "df_schema": persons_schema
    },
    "products": {
        "request_url": "https://api.pipedrive.com/api/v2/products",
        "params": {"limit": 500, "sort_by": "update_time", "sort_direction": "desc"},
        "extraction_control": {
            "record_date_field": "update_time",
            "min_record_date": date_start,
            "max_record_date": date_end,
        },
        "df_schema": products_schema
    },
    "leads": {
        "request_url": "https://api.pipedrive.com/v1/leads",
        "params": {"limit": 500, "sort": "update_time DESC"},
        "extraction_control": {
            "record_date_field": "update_time",
            "min_record_date": date_start,
            "max_record_date": date_end,
        },
        "df_schema": leads_schema
    },
    "leadLabels": {"request_url": "https://api.pipedrive.com/v1/leadLabels", "df_schema": lead_labels_schema},
    "organizations": {
        "request_url": "https://api.pipedrive.com/v1/organizations",
        "df_schema": organizations_schema
    },
    "organization_fields": {
        "request_url": "https://api.pipedrive.com/v1/organizationFields",
        "df_schema": organization_fields_schema
    },
    "stages": {
        "request_url": "https://api.pipedrive.com/api/v2/stages",
        "params": {"limit": 500},
        "df_schema": stages_schema
    },
}

task_dict = {}

@dag(
    dag_id="data_ingestion_pipedrive",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=days_ago(1),
    tags=["ingestion"],
    catchup=False,
    concurrency=1,
)
def data_ingestion_pipedrive():

    for account in accounts:
        
        api_token = Variable.get(f"pipedrive_{account}_api_token")

        for extraction, extraction_info in extractions.items():

            @task(task_id=f"extract_data_{extraction}_{account}")
            def extract_data(extraction, extraction_info, api_token):

                if extraction_info.get("batch_iteration"):
                    extraction_info["content"] = extraction_info["content_function"](*extraction_info["content_args"])

                data = extract_pipedrive(extraction, extraction_info, api_token)
        
                return data

            @task(task_id=f"create_df_{extraction}_{account}")
            def create_df(data, **kwargs):
        
                df = create_pipedrive_df(data, kwargs["partition_date"], kwargs["account"], kwargs["df_schema"])
        
                return df

            @task(task_id=f"write_data_{extraction}_{account}")
            def write_data(df, s3_path, database, table, df_athena_schema, aws_session, mode, partition_cols):
                write_pandas_to_table(
                    df=df,
                    s3_path=s3_path,
                    database=database,
                    table=table,
                    df_athena_schema=df_athena_schema,
                    aws_session=aws_session,
                    mode=mode,
                    partition_cols=partition_cols
                )
        
            data = extract_data(extraction=extraction, extraction_info=extraction_info, api_token=api_token)
        
            df = create_df(
                data=data,
                partition_date='{{ ds }}',
                account=account,
                df_schema=extraction_info["df_schema"]
            )
        
            write_task = write_data(
                df=df,
                s3_path=f"{LANDING_PATH}/{extraction}",
                database="landing",
                table=f"pipedrive_{extraction}",
                df_athena_schema=extraction_info["df_schema"],
                aws_session=aws_session,
                mode="overwrite_partitions",
                partition_cols=["partition_date", "account"]
            )

            task_dict[f"write_data_{extraction}_{account}"] = write_task
            task_dict[f"extract_data_{extraction}_{account}"] = data

        task_dict[f"write_data_deals_{account}"] >> task_dict[f"extract_data_deals_products_{account}"]


data_ingestion_pipedrive = data_ingestion_pipedrive()
