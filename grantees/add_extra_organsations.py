import csv
import requests
import logging
import math
import os
import time

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ExtraOrganisation:
    def __init__(self, file_path, dry_run=False):
        self.file_path = file_path
        self._load_env_variables()
        self.success_responses = []
        self.failed_responses = []
        self.update_success_responses = []
        self.update_failed_responses = []
        self.all_failed = []
        self.all_success = []
        self.all_update_failed = []
        self.all_update_success = []
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

    def _load_env_variables(self):
        self.foundation_id = os.getenv("PROD_FOUNDATION_ID")
        self.get_grantees_endpoint = os.getenv("GET_CONTACTS_ENDPOINT").format(
            self.foundation_id
        )
        self._bearer_token = os.getenv("PROD_BEARER_TOKEN")
        self.create_nonprofit_endpoint = os.getenv(
            "CREATE_GRANTEE_PROD_ENDPOINT"
        ).format(self.foundation_id)
        self.update_nonprofit_endpoint = os.getenv("UPDATE_GRANTEE_PROD_ENDPOINT")
        self.headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Content-Type": "application/json",
        }

    def _read_csv(self):
        if self.dry_run:
            self.logger.info(f"Dry run step: The First step is to read the csv")
        """Reads CSV data from the specified file path."""
        with open(self.file_path, "r") as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

    def _get_nonprofit_org_ids(self):
        """Fetches existing nonprofit IDs from the API."""
        if self.dry_run:
            self.logger.info(
                f"Dry run step: get the list of the existing non profits- then get the affinity id"
            )
            return
        response = requests.post(
            self.get_grantees_endpoint, headers=self.headers, json={"pageSize": 10000}
        )
        if response.status_code != 200:
            logging.error(f"Failed to fetch nonprofit org IDs: {response.text}")
            return []

        nonprofits = []
        response_data = response.json()
        for item in response_data.get("searchResponse", {}).get("responses", []):
            org_id = item.get("customFields", {}).get("Affinity ID-4jS8olxc")
            nonprofits.append(org_id)
        return nonprofits

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
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise
        return None

    def _create_nonprofit(self, row):
        """Creates a nonprofit organization."""
        if self.dry_run:
            self.logger.info(f"Dry run would create the non profit")
            return
        payload = {"legalName": row.get("Name")}

        response = self._send_request_with_retry(
            self.create_nonprofit_endpoint, self.headers, payload
        )
        if response:
            return response.json().get("id")
        else:
            logging.error(
                f"Failed to create nonprofit for {row.get('Name')}: {response.text}"
            )
            return None

    def _update_nonprofit(self, nonprofit_id, row, tag):
        """Updates metadata for a nonprofit organization."""
        if self.dry_run:
            self.logger.info(
                f"Dry run would update the custom fields after a suceesful creation"
            )
            return
        payload = {
            "website": row.get("Website"),
            "customFields": {"Affinity ID-4jS8olxc": row.get("Organization Id")},
            "interactionAdditionalInfo": {
                "qbVendorDetails": None,
                "organizationTags": [tag],
            },
        }
        url = self.update_nonprofit_endpoint.format(self.foundation_id, nonprofit_id)
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code == 204:
            return True
        else:
            logging.error(f"Failed to update nonprofit {nonprofit_id}: {response.text}")
            return False

    def process_batch(self, batch, tamelio_organisations):
        """Processes a batch of rows."""

        for row in batch:
            org_id = row.get("Organization Id")
            tag = row.get("Tags")

            if org_id not in tamelio_organisations:
                nonprofit_id = self._create_nonprofit(row)
                if nonprofit_id:
                    if self._update_nonprofit(nonprofit_id, row, tag):
                        self.update_success_responses.append(org_id)
                    else:
                        self.update_failed_responses.append(org_id)
                else:
                    self.failed_responses.append(row.get("Name"))
            else:
                logging.info(
                    f"Organization {org_id} already exists. Skipping creation."
                )

        return (
            self.failed_responses,
            self.success_responses,
            self.update_failed_responses,
            self.update_success_responses,
        )

    def process_csv(self, batch_size=100):
        """Processes the CSV file to create and update nonprofits in batches."""
        csv_data = self._read_csv(file_path)
        tamelio_organisations = self._get_nonprofit_org_ids()

        total_batches = math.ceil(len(csv_data) / batch_size)
        self.logger.info(
            f"Processing {len(csv_data)} rows in {total_batches} batches of {batch_size} rows each."
        )

        for i in range(0, len(csv_data), batch_size):
            batch = csv_data[i : i + batch_size]
            self.logger.info(
                f"Processing batch {i // batch_size + 1} of {total_batches}..."
            )
            failed, success, update_failed, update_success = self._process_batch(
                batch, tamelio_organisations
            )
            self.all_failed.extend(failed)
            self.all_success.extend(success)
            self.all_update_failed.extend(update_failed)
            self.all_update_success.extend(update_success)

        return (
            self.all_failed,
            self.all_success,
            self.all_update_failed,
            self.all_update_success,
        )


if __name__ == "__main__":
    file_path = "/Users/joycendichu/Downloads/!Production data migration - All orgs - non grantees.csv"
    extra_orgs = ExtraOrganisation(file_path)
    failed, success, update_failed, update_success = extra_orgs.process_csv(
        file_path, batch_size=100
    )
    extra_orgs.logger.info(f"Failed Creation Responses: {failed}")
    extra_orgs.logger.info(f"Successful Creation Responses: {success}")
    extra_orgs.logger.info(f"Failed Update Responses: {update_failed}")
    extra_orgs.logger.info(f"Successful Update Responses: {update_success}")
