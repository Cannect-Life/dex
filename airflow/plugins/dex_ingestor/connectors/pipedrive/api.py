import time
import requests
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_random, wait_exponential

from dex_ingestor.utils import date_parse, apply_data_types


def paginate_cursor(response):

    next_cursor = response["additional_data"]["next_cursor"]

    return next_cursor is None, {"cursor": next_cursor}


def paginate_limit(response):

    finished = response["additional_data"]["pagination"]["more_items_in_collection"]

    next_start = (
        response["additional_data"]["pagination"]["start"]
        + response["additional_data"]["pagination"]["limit"]
    )

    return finished, {"start": next_start}


def extract_pipedrive(extraction, extraction_info, api_token):

    extraction_config = {
        "activities": {"pagination_function": paginate_cursor},
        "deals": {"pagination_function": paginate_cursor},
        "persons": {"pagination_function": paginate_cursor},
        "products": {"pagination_function": paginate_cursor},
        "leads": {"pagination_function": paginate_limit},
        "leadLabels": {},
    }

    pagination_function = extraction_config[extraction].get("pagination_function")

    request_url = extraction_info["request_url"]
    params = extraction_info.get("params", {})
    min_record_date = None
    max_record_date = None

    extraction_control = extraction_info.get("extraction_control")

    if extraction_control:
        record_date_field = extraction_control.get("record_date_field")
        min_record_date = date_parse(extraction_control.get("min_record_date"))
        max_record_date = date_parse(extraction_control.get("max_record_date"))

    data = []
    finished = False
    page_counter = 1

    while not finished:

        print(f"Requesting page: {page_counter}")

        response = get_pipedrive_data(request_url=request_url, api_token=api_token, params=params)

        page_data = response["data"]

        if not page_data:
            return data

        if pagination_function:
            finished, params_update = pagination_function(response)
            params.update(params_update)
        else:
            print("Unpaginated extraction")
            finished = True

        if max_record_date:
            page_data = [
                r
                for r in page_data
                if date_parse(r[record_date_field]) <= max_record_date
            ]

        if min_record_date:
            last_record = page_data[-1]
            if date_parse(last_record[record_date_field]) < min_record_date:
                finished = True

            page_data = [
                r
                for r in page_data
                if date_parse(r[record_date_field]) >= min_record_date
            ]

        data.extend(page_data)

        print(f"Fetched {len(page_data)} deals, total: {len(data)}")

        page_counter += 1

    return data


def log_retry(retry_state):
    if retry_state.attempt_number > 1:
        print("Retry ", retry_state.attempt_number - 1)


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=60, min=60, max=15 * 60) + wait_random(0, 30),
    reraise=True,
    before=log_retry,
)
def get_pipedrive_data(request_url, api_token, params=None, timeout=120):
    if params is None:
        params = {}
    params["api_token"] = api_token

    print(request_url)
    response = requests.get(request_url, params=params, timeout=timeout)

    if "X-RateLimit-Remaining" in response.headers:
        remaining_requests = int(response.headers["X-RateLimit-Remaining"])
        if remaining_requests < 5:
            reset_time = int(response.headers["X-RateLimit-Reset"])
            print(f"Rate limit reached. Sleeping for {reset_time} seconds.")
            time.sleep(reset_time)
            return get_pipedrive_data(request_url, api_token, params, timeout)

    response.raise_for_status()

    return response.json()


def create_pipedrive_df(data, partition_date, account, df_schema):
    df = pd.DataFrame(data)

    df["partition_date"] = partition_date
    df["account"] = account

    df = apply_data_types(df, df_schema)

    return df

if __name__ == '__main__':

    import pandas as pd
    from dex_ingestor.connectors.pipedrive.endpoint_schemas import *

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 100)

    api_token = "<FILL ME>"

    date_start = None
    date_end = None

    date_start = "2024-07-01 00:00:00"
    date_end = "2024-07-24 00:00:00"

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
    }

    for extraction, extraction_info in extractions.items():
        print(extraction)
        data = extract_pipedrive(extraction, extraction_info, api_token)
        print(len(data))
        df = create_pipedrive_df(data=data, partition_date="test", account="test", df_schema=extraction_info["df_schema"])
        print(df.head(2))
        print("----------------------------------------------------------")