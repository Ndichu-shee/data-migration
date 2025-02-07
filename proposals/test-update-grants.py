import csv
import requests
from datetime import datetime
import os
import json
import logging
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class GrantUpdater:
    def __init__(self, file_path, dry_run):
        self.file_path = file_path
        self.successful_requests = []
        self.failed_requests = []
        self.dry_run = dry_run
        self.pipelines_data = json.loads(os.getenv("PIPELINES_DATA"))
        self.pipelines = json.loads(os.getenv("PIPELINES"))
        self.logger = logging.getLogger(__name__)
        self._load_env_variables()

    def _load_env_variables(self):
        self.foundation_id = os.getenv("PROD_FOUNDATION_ID")
        self.update_grant_metadata = os.getenv("UPDATE_GRANT_METADATA")
        self._bearer_token = os.getenv("PROD_BEARER_TOKEN")
        self.get_grants_endpoint = os.getenv("GET_GRANTS_ENDPOINT").format(
            self.foundation_id
        )
        self.headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Content-Type": "application/json",
        }

    def _read_csv(self):
        """Reads a CSV file and returns its content as a list of dictionaries."""
        if self.dry_run:
            self.logger.info("Dry run step: read csv")
        with open(self.file_path, mode="r") as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

    def _parse_date(self, date_str, input_format="%m/%d/%Y", output_format="%Y-%m-%d"):
        """Parses a date string into a desired format."""
        try:
            return datetime.strptime(date_str, input_format).strftime(output_format)
        except (ValueError, TypeError):
            self.logger.info(f"Error parsing date: {date_str}")
            return None

    def _get_grants_name(self):
        """Fetches the grant names from the API."""
        if self.dry_run:
            self.logger.info("Dry step: Get grants from temelio")
            return []
        payload = {"pageSize": 10000}

        data = self._send_request(
            self.get_grants_endpoint, self.headers, json_payload=payload, method="POST"
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

    def _update_custom_fields(self, update_data):
        """Update the custom fields of a nonprofit."""
        if self.dry_run:
            self.logger.info("Dry run step: update grants custom field")
        response = self._send_request_with_retry(
            self.update_grant_metadata, self.headers, update_data
        )
        if response:
            self.logger.info(f"Successfully updated custom fields: {update_data}")
            successful_data = {"grant_name": update_data.get("name")}
            self.successful_requests.append(successful_data)
        else:
            self.logger.info(f"{update_data}")
            self.logger.info(
                f"Failed to update custom fields. Status: {response.status_code}, Response: {response.text}"
            )
            failed_data = {
                "grant_name": update_data.get("name"),
                "message": response.text,
            }
            self.failed_requests.append(failed_data)

        return self.successful_requests, self.failed_requests

    # Function to get the stage ID by pipeline name and stage name
    def _get_stage_id_by_pipeline_and_name(self, pipeline_name, stage_name):
        pipeline = self.pipelines_data.get(pipeline_name)
        if not pipeline:
            return "Pipeline not found."

        if isinstance(pipeline["stages"], dict):
            return pipeline["stages"].get(stage_name, "Stage not found.")
        elif isinstance(pipeline["stages"], list):
            for stage in pipeline["stages"]:
                if stage["name"].lower() == stage_name.lower():
                    return stage["id"]
            return "Stage not found."

    def _send_request_with_retry(self, url, headers, data, retries=3, delay=5):
        for attempt in range(retries):
            try:
                response = requests.post(url, headers=headers, json=data, timeout=30)
                if response.status_code == 200:
                    return response
                else:
                    raise requests.exceptions.RequestException(
                        f"Request failed with status code {response.status_code}"
                    )
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException,
            ) as e:
                self.logger.info(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise
        return None

    def process_csv(self):

        csv_data = self._read_csv(self.file_path)
        api_data = self._get_grants_name()

        for api_response in api_data:
            grant_name = api_response.get("grant_name")
            grant_id = api_response.get("grant_id")
            non_profit_id = api_response.get("non_profit_id")

            for row in csv_data:
                row_grant_name = row.get("Name")
                pipeline_id_csv = row.get("Pipeline")
                stage_name_csv = row.get("Stage")
                stage_id = self._get_stage_id_by_pipeline_and_name(
                    pipeline_id_csv, stage_name_csv
                )
                temelio_pipeline_id = (
                    None
                    if self.pipelines.get(pipeline_id_csv)
                    == "00000000-0000-0000-0000-000000000000"
                    else self.pipelines.get(pipeline_id_csv)
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
                    self.logger.info(f"Updating with data:")
                    successful_requests, failed_requests = self._update_custom_fields(
                        update_data
                    )
        self.logger.info(
            f"successfully updated {len(successful_requests)} for these requests {successful_requests}"
        )
        self.logger.info("***************************")
        self.logger.info(
            f"Failed requests {len(failed_requests)} for these requests {failed_requests}"
        )


if __name__ == "__main__":
    file_path = "/Users/joycendichu/Downloads/!Production data migration - Grant_opportunities_unsaved_view__export_Jan-30-2025.csv"
    grant_updator = GrantUpdater(file_path, dry_run=True)
    grant_updator.process_csv()
