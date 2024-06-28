import json
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from datetime import datetime, timezone
from dateutil.parser import parse as date_parse

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

LANDING_PATH = "s3://lakehouse-cannect/landing/pagarme/v1"

default_args = {
    "owner": "data",
}

accounts = ["cannect"]

aws_connection = "aws_cannect"

endpoints = {
    "transactions": {
        "date_column": "date_updated",  # date_created
        "params_str": "{date_column}=>={date_start_int}&{date_column}=<{date_end_int}",
    },
    "refunds": {
        "date_column": "date_updated",  # date_created
        "params_str": "{date_column}=>={date_start_int}&{date_column}=<{date_end_int}",
    },
    "payables": {
        "date_column": "updated_at",  # created_at
        "params_str": "{date_column}=>={date_start_int}&{date_column}=<{date_end_int}",
        "partition_column": "_ingestion_date_"
    },
    "customers": {
        "date_column": "date_created",  # date_updated is NOT available
        "params_str": "{date_column}=>={date_start}&{date_column}=<{date_end}"
    },
    "chargebacks": {
        "date_column": "updated_at",  # created_at
        "params_str": "{date_column}=>={date_start}&{date_column}=<{date_end}",
    },
}

data_schemas = {
    "transactions": {
        "object": str,
        "status": str,
        "refuse_reason": str,
        "status_reason": str,
        "acquirer_response_code": str,
        "acquirer_response_message": str,
        "acquirer_name": str,
        "acquirer_id": str,
        "authorization_code": str,
        "soft_descriptor": str,
        "tid": str,
        "nsu": str,
        "date_created": "date",
        "date_updated": "date",
        "amount": int,
        "authorized_amount": int,
        "paid_amount": int,
        "refunded_amount": int,
        "installments": int,
        "id": str,
        "cost": int,
        "card_holder_name": str,
        "card_last_digits": str,
        "card_first_digits": str,
        "card_brand": str,
        "card_pin_mode": str,
        "card_magstripe_fallback": str,
        "card_funding_source": str,
        "cvm_pin": str,
        "postback_url": str,
        "payment_method": str,
        "capture_method": str,
        "antifraud_score": str,
        "boleto_url": str,
        "boleto_barcode": str,
        "boleto_expiration_date": "date",
        "boleto": str,
        "referer": str,
        "ip": str,
        "subscription_id": str,
        "phone": str,
        "address": str,
        "customer": str,
        "billing": str,
        "shipping": str,
        "items": str,
        "card": str,
        "split_rules": str,
        "metadata": str,
        "antifraud_metadata": str,
        "reference_key": str,
        "device": str,
        "local_transaction_id": str,
        "local_time": str,
        "fraud_covered": str,
        "fraud_reimbursed": str,
        "order_id": str,
        "risk_level": str,
        "receipt_url": str,
        "payment": str,
        "addition": str,
        "discount": str,
        "private_label": str,
        "pix_data": str,
        "pix_qr_code": str,
        "pix_expiration_date": "date",
        "service_referer_name": str,
        "partition_date": str,
        "account": str
    },
    "refunds": {
        "object": str,
        "id": str,
        "amount": int,
        "fee": int,
        "fraud_coverage_fee": int,
        "type": str,
        "status": str,
        "charge_fee_recipient_id": str,
        "bank_account_id": str,
        "transaction_id": str,
        "local_transaction_id": str,
        "date_created": "date",
        "date_updated": "date",
        "metadata": str,
        "partition_date": str,
        "account": str
    },
    "payables": {
        "object": str,
        "id": str,
        "status": str,
        "amount": int,
        "fee": int,
        "anticipation_fee": int,
        "fraud_coverage_fee": int,
        "installment": int,
        "transaction_id": str,
        "split_rule_id": str,
        "bulk_anticipation_id": str,
        "anticipation_id": str,
        "recipient_id": str,
        "originator_model": str,
        "originator_model_id": str,
        "payment_date": "date",
        "original_payment_date": "date",
        "type": str,
        "payment_method": str,
        "accrual_date": "date",
        "date_created": "date",
        "liquidation_arrangement_id": str,
        "partition_date": str,
        "account": str
    },
    "customers": {
        "object": str,
        "id": str,
        "external_id": str,
        "type": str,
        "country": str,
        "document_number": str,
        "document_type": str,
        "name": str,
        "email": str,
        "phone_numbers": str,
        "born_at": str,
        "birthday": "date",
        "gender": str,
        "date_created": "date",
        "documents": str,
        "addresses": str,
        "phones": str,
        "client_since": str,
        "risk_indicator": str,
        "partition_date": str,
        "account": str
    },
    "chargebacks": {
        "object": str,
        "id": str,
        "installment": int,
        "transaction_id": str,
        "amount": int,
        "reason_code": str,
        "card_brand": str,
        "updated_at": "date",
        "created_at": "date",
        "date_updated": "date",
        "date_created": "date",
        "accrual_date": "date",
        "status": str,
        "cycle": int,
        "partition_date": str,
        "account": str
    },
}


def treat_nulls(col_value):
    if isinstance(col_value, (dict, list)):
        return col_value
    if pd.isnull(col_value):
        return None
    return col_value
    

def treat_str_columns(col_value):

    if isinstance(col_value, (dict, list)):
        return json.dumps(col_value, ensure_ascii=False)

    if pd.isnull(col_value):
        return None

    return str(col_value)


def apply_data_types(df, df_schema):

    for column, dtype in df_schema.items():

        if column not in df.columns:
            df[column] = None
        else:
            df[column] = df[column].apply(lambda x: treat_nulls(x))

        original_dtype = df[column].dtype

        if dtype == str:
            df[column] = df[column].apply(treat_str_columns)
        elif dtype == int:
            df[column] = df[column].astype("Int64")
        elif dtype == "date":
            df[column] = df[column].apply(
                lambda x: x if pd.isnull(x) else date_parse(x)
            )
        else:
            df[column] = df[column].astype(dtype)
            if dtype == str and original_dtype == "float64":
                df[column] = df[column].apply(
                    lambda x: None if pd.isnull(x) else x.replace(".0", "")
                )
    return df


@dag(
    dag_id="data_ingestion_pagarme",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    tags=["ingestion"],
    catchup=False,
    concurrency=1,
)
def data_ingestion_pagarme():

    for account in accounts:

        for endpoint, extraction_info in endpoints.items():
            df_schema = data_schemas[endpoint]
            date_column = extraction_info["date_column"]
            partition_column = extraction_info.get("partition_column", date_column)

            @task(task_id=f"extract_data_{account}_{endpoint}")
            def extract_data(
                account,
                auth_token,
                endpoint,
                extraction_info,
                date_column,
                date_start_str,
                date_end_str,
                timeout_seconds=90,
                number_of_retries=2,
                retry_time_seconds=120,
                exponential_backoff=True,
                max_wait_time_seconds=1800,
            ):
                _endpoint = extraction_info.get("endpoint_path", endpoint)
                count = extraction_info.get("count", 1000)

                req_headers = {"accept": "application/json"}
                params = {"api_key": auth_token, "count": count}

                date_start = str(date_parse(date_start_str).date())
                date_end = str(date_parse(date_end_str).date())

                date_start_int = int(date_parse(date_start_str).timestamp() * 1000)
                date_end_int = int(date_parse(date_end_str).timestamp() * 1000)

                request_url = f"https://api.pagar.me/1/{_endpoint}"
                page = 1
                request_counter = 0
                data = []

                while True:
                    _request_url = request_url

                    req_params = params.copy()

                    req_params["page"] = page

                    params_str = extraction_info.get("params_str", "").format(
                        date_column=date_column,
                        date_start=date_start,
                        date_end=date_end,
                        date_start_int=date_start_int,
                        date_end_int=date_end_int,
                    )

                    print(f"Requesting with params: {params_str}")

                    if params_str != "":
                        params_str += "&" + "&".join(
                            [f"{k}={v}" for k, v in req_params.items()]
                        )
                        req_params = {}
                        _request_url += f"?{params_str}"

                    for attempt in range(number_of_retries + 1):
                        request_counter += 1

                        print(f"Requesting page {page}")

                        try:
                            response = requests.get(
                                _request_url,
                                params=req_params,
                                headers=req_headers,
                                timeout=timeout_seconds,
                            )
                            response.raise_for_status()

                            break
                        except Exception as e:
                            status = response.status_code
                            print(f"Failed with status {status}: exception {e}")
                            if attempt >= number_of_retries:
                                raise e
                            delay = min(
                                max_wait_time_seconds,
                                (
                                    retry_time_seconds * (2**attempt)
                                    if exponential_backoff
                                    else retry_time_seconds
                                ),
                            )
                            print(f"Sleeping for {delay} seconds before retry")
                            time.sleep(delay)

                    if not response.json():
                        break

                    data += response.json()
                    page += 1

                return data

            @task(task_id=f"treat_data_{account}_{endpoint}")
            def treat_data(data, **kwargs):

                if len(data) == 0:
                    return pd.DataFrame()
                    
                df = pd.DataFrame(data)

                df = apply_data_types(df, kwargs["df_schema"])

                partition_column = kwargs.get("partition_date_column")
                
                if partition_column == "_ingestion_date_":
                    df["partition_date"] = kwargs["ingestion_date"]
                else:
                    def get_date_str(col_value):
                        if isinstance(col_value, str):
                            col_value = date_parse(col_value)
                        return str(col_value.date())
                        
                    df["partition_date"] = df[partition_column].apply(get_date_str)

                df["account"] = kwargs["account"]

                return df

            @task(task_id=f"write_data_{account}_{endpoint}")
            def write_data(endpoint, df, df_schema, partition_cols):
                print(endpoint)

                from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

                aws_hook = AwsBaseHook(aws_connection, client_type="sts")
                sts_client = aws_hook.get_conn()
                assumed_role = sts_client.assume_role(
                    RoleArn="arn:aws:iam::094506792271:role/deXAutomationRole",
                    RoleSessionName='cannect_session'
                )
                
                credentials = assumed_role['Credentials']
                access_key = credentials['AccessKeyId']
                secret_key = credentials['SecretAccessKey']
                session_token = credentials['SessionToken']

                s3_path = f"{LANDING_PATH}/{endpoint}/"

                print(f"Writing data to {s3_path}")

                if df.empty:
                    print("Empty dataframe. Skipping!")
                    return

                s3_filesystem = pafs.S3FileSystem(
                    region="us-east-1", access_key=access_key, secret_key=secret_key, session_token=session_token
                )

                type_map = {
                    str: pa.string(),
                    float: pa.float64(),
                    int: pa.int32(),
                    bool: pa.bool_(),
                    "date": pa.timestamp("us"),
                }

                pa_schema = pa.schema(
                    [(key, type_map[value]) for key, value in df_schema.items()]
                )

                pa_table = pa.Table.from_pandas(
                    df, preserve_index=False, schema=pa_schema
                )

                pq.write_to_dataset(
                    table=pa_table,
                    root_path=s3_path.replace("s3://", ""),
                    filesystem=s3_filesystem,
                    partition_cols=partition_cols,
                    existing_data_behavior="delete_matching",
                    version="2.6",
                    compression="snappy",
                )

            data = extract_data(
                account=account,
                auth_token=Variable.get(f"pagarme_{account}_token"),
                endpoint=endpoint,
                extraction_info=extraction_info,
                date_column=date_column,
                date_start_str="{{ (logical_date - macros.timedelta(days=0)).strftime('%Y-%m-%dT%H:%M:%S.%fZ') }}",
                date_end_str="{{ (logical_date + macros.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ') }}",
                timeout_seconds=90,
                number_of_retries=2,
                retry_time_seconds=120,
                exponential_backoff=True,
                max_wait_time_seconds=1800,
            )

            df = treat_data(
                data=data,
                df_schema=df_schema,
                account=account,
                partition_date_column=partition_column,
                ingestion_date="{{ (logical_date + macros.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ') }}"
            )

            write_data(
                endpoint=endpoint,
                df=df,
                df_schema=df_schema,
                partition_cols=["partition_date", "account"],
            )


data_ingestion_pagarme = data_ingestion_pagarme()