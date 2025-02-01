import csv
import requests
from datetime import datetime
import os
import json
import logging

logger = logging.getLogger(__name__)
foundation_id = os.getenv("PROD_FOUNDATION_ID")
update_grant_metadata = os.getenv("UPDATE_GRANT_METADATA")
bearer_token = os.getenv("PROD_BEARER_TOKEN")
get_grants_endpoint = os.getenv("GET_GRANTS_ENDPOINT").format(foundation_id)

pipeline_data_json = os.getenv("PIPELINES_DATA")
pipelines_data = json.loads(pipeline_data_json)

pipeline_json = os.getenv("PIPELINES")
pipelines = json.loads(pipeline_json)

HEADERS = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json",
}


def parse_date(date_str, input_format="%m/%d/%Y", output_format="%Y-%m-%d"):
    """Parses a date string into a desired format."""
    try:
        return datetime.strptime(date_str, input_format).strftime(output_format)
    except (ValueError, TypeError):
        logger.info(f"Error parsing date: {date_str}")
        return None


def send_request(url, headers, json_payload=None, method="POST"):
    """Sends an HTTP request and returns the JSON response or None."""
    response = requests.request(method, url, headers=headers, json=json_payload)
    if response.status_code == 200:
        return response.json()
    else:
        logger.info(
            f"Failed request to {url}. Status: {response.status_code}, Response: {response.text}"
        )
        return None


def get_grants_name():
    """Fetches the grant names from the API."""
    payload = {"pageSize": 10000}

    data = send_request(
        get_grants_endpoint, HEADERS, json_payload=payload, method="POST"
    )

    if not data:
        return []

    grant_names = []
    for item in data.get("responses", []):
        grant_data = {
            "grant_name": item.get("name"),
            "grant_id": item.get("id"),
            "stage_name": item.get("stage"),
            "non_profit_id": item.get("nonprofitId"),
            "stageId": item.get("stageId"),
        }
        grant_names.append(grant_data)
    return grant_names


def read_csv(file_path):
    """Reads a CSV file and returns its content as a list of dictionaries."""
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        return [row for row in reader]


successful_requests = []
failed_requests = []


def update_custom_fields(update_data):
    """Update the custom fields of a nonprofit."""
    response = requests.post(update_grant_metadata, headers=HEADERS, json=update_data)
    if response.status_code == 200:
        logger.info(f"Successfully updated custom fields: {update_data}")
        successful_data = {"grant_name": update_data.get("name")}
        successful_requests.append(successful_data)
    else:
        logger.info(f"{update_data}")
        logger.info(
            f"Failed to update custom fields. Status: {response.status_code}, Response: {response.text}"
        )
        failed_data = {"grant_name": update_data.get("name"), "message": response.text}
        failed_requests.append(failed_data)

    return successful_requests, failed_requests


# Function to get the stage ID by pipeline name and stage name
def get_stage_id_by_pipeline_and_name(pipeline_name, stage_name):
    pipeline = pipelines_data.get(pipeline_name)
    if not pipeline:
        return "Pipeline not found."

    if isinstance(pipeline["stages"], dict):
        return pipeline["stages"].get(stage_name, "Stage not found.")
    elif isinstance(pipeline["stages"], list):
        for stage in pipeline["stages"]:
            if stage["name"].lower() == stage_name.lower():
                return stage["id"]
        return "Stage not found."


def process_csv(file_path):

    csv_data = read_csv(file_path)
    api_data = get_grants_name()

    for api_response in api_data:
        grant_name = api_response.get("grant_name")
        grant_id = api_response.get("grant_id")
        non_profit_id = api_response.get("non_profit_id")

        for row in csv_data:
            row_grant_name = row.get("Name")
            pipeline_id_csv = row.get("Pipeline")
            stage_name_csv = row.get("Stage")
            stage_id = get_stage_id_by_pipeline_and_name(
                pipeline_id_csv, stage_name_csv
            )
            temelio_pipeline_id = (
                None
                if pipelines.get(pipeline_id_csv)
                == "00000000-0000-0000-0000-000000000000"
                else pipelines.get(pipeline_id_csv)
            )

            if row_grant_name == grant_name:
                # formatted_date = parse_date(row.get("Close Date (decision made)"))
                update_data = {
                    "id": grant_id,
                    "name": grant_name,
                    "nonprofitId": non_profit_id,
                    # "awardedDate": formatted_date,
                    # "awardedAmount": row.get("Amount") if stage_name in approved_stages else 0,
                    "pipelineId": temelio_pipeline_id,
                    "StageId": stage_id,
                    "stage": stage_name_csv,
                }
                logger.info(f"Updating with data:")
                successful_requests, failed_requests = update_custom_fields(update_data)
    logger.info(
        f"successfully updated {len(successful_requests)} for these requests {successful_requests}"
    )
    logger.info("***************************")
    logger.info(
        f"Failed requests {len(failed_requests)} for these requests {failed_requests}"
    )


if __name__ == "__main__":
    process_csv(
        "/Users/joycendichu/Downloads/!Production data migration - Grant_opportunities_unsaved_view__export_Jan-30-2025.csv"
    )
