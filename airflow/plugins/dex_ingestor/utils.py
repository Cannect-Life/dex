import json
import pytz
import pandas as pd
from dateutil.parser import parse as parse_date_str
from airflow.models import Variable


def date_parse(date, default_timezone="UTC"):
    if date is None:
        return None

    if isinstance(date, str):
        date = parse_date_str(date)

    if date.tzinfo is None:
        date = date.astimezone(pytz.timezone(default_timezone))

    return date


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

    type_mapping = {
        "boolean": bool,
        "tinyint": int,
        "smallint": int,
        "int": int,
        "integer": int,
        "bigint": int,
        "float": float,
        "double": float,
        "decimal(38,2)": str,
        "decimal(38,4)": str,
        "decimal(38,6)": str,
        "decimal(38,8)": str,
        "string": str,
        "char": str,
        "varchar": str,
        "binary": bytes,
        "date": "date",
        "timestamp": "timestamp",
    }

    for column, _dtype in df_schema.items():

        dtype = type_mapping.get(_dtype, str)

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
                lambda x: x if pd.isnull(x) else date_parse(x).date()
            )
        elif dtype == "timestamp":
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


def parse_datetime_str(d):
    return parse_date_str(d).replace(tzinfo=None) if isinstance(d, str) else d


def task_fail_alert(context):
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

    airflow_url = Variable.get(airflow_url, "")

    log_url = context.get("task_instance").log_url.split("?")[-1]

    log_url = f"{airflow_url}/log?{log_url}"

    msg = """
            :red_circle: Task Failed.
            *Project*: {project_name}
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {exec_date}
            *Log Link*: <{log_url}|Click here>
            """.format(
        project_name="Cannect Lakehouse",
        dag=context.get("task_instance").dag_id,
        task=context.get("task_instance").task_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=log_url,
    )

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": msg,
            },
        }
    ]

    failed_alert = SlackWebhookOperator(
        task_id="slack_alert",
        slack_webhook_conn_id="slack_default",
        blocks=blocks,
        username="deX",
    )

    return failed_alert.execute(context=context)
