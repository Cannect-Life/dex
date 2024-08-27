import time
import requests
import pandas as pd

from itertools import islice
from tenacity import retry, stop_after_attempt, wait_random, wait_exponential

from dex_ingestor.utils import date_parse, apply_data_types


def batch_iterable(iterable, batch_size):
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch

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
        "deals_products": {"pagination_function": paginate_cursor},
        "deal_fields": {"pagination_function": paginate_limit},
        "persons": {"pagination_function": paginate_cursor},
        "products": {"pagination_function": paginate_cursor},
        "leads": {"pagination_function": paginate_limit},
        "leadLabels": {},
        "organizations": {"pagination_function": paginate_limit},
        "organization_fields": {"pagination_function": paginate_limit},
        "stages": {"pagination_function": paginate_cursor},
    }

    pagination_function = extraction_config[extraction].get("pagination_function")

    request_url = extraction_info["request_url"]
    min_record_date = None
    max_record_date = None

    extraction_control = extraction_info.get("extraction_control")

    if extraction_control:
        record_date_field = extraction_control.get("record_date_field")
        min_record_date = date_parse(extraction_control.get("min_record_date"))
        max_record_date = date_parse(extraction_control.get("max_record_date"))
    
    batch_iteration = extraction_info.get("batch_iteration", {})
    iterable_items = ["dummy"]
    batch_size = 100
    batch_param = None
    if batch_iteration:
        iterable_items = batch_iteration["content"]
        print(f"Iterating over {len(iterable_items)} items")
        batch_size = batch_iteration["batch_size"]
        batch_param = batch_iteration["param_name"]

    data = []
    batch_counter = 1

    for items in batch_iterable(iterable_items, batch_size=batch_size):

        print(f"Requesting batch: {batch_counter}")

        page_counter = 1
        finished = False
        params = extraction_info.get("params", {})

        if batch_param:
            params[batch_param] = ",".join(items)

        while not finished:

            print(f"Requesting page: {page_counter}")

            response = get_pipedrive_data(request_url=request_url, api_token=api_token, params=params)

            page_data = response["data"]

            if not page_data:
                break

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

            if min_record_date and page_data:
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
        
        batch_counter += 1

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
