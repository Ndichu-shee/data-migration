import csv
import requests
import logging
import os
import time
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Grantee:
    def __init__(self, file_path, dry_run=False):
        self.file_path = file_path
        self.successful_responses = []
        self.failed_responses = []
        self._load_env_variables()
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

    def _load_env_variables(self):
        self.foundation_id = os.getenv("PROD_FOUNDATION_ID")
        self.create_nonprofit_endpoint = os.getenv(
            "CREATE_GRANTEE_PROD_ENDPOINT"
        ).format(self.foundation_id)
        self._bearer_token = os.getenv("PROD_BEARER_TOKEN")
        self._headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Content-Type": "application/json",
        }
        self.get_grantees_endpoint = os.getenv("GET_CONTACTS_ENDPOINT").format(
            self.foundation_id
        )

    def _read_csv(self):
        """
        Read the grantees CSV and return a list of dictionaries for each row.
        """
        if self.dry_run:
            self.logger.info("Dry run: Reading CSV file.")
        with open(self.file_path, mode="r") as file:
            reader = csv.DictReader(file)
            return [row for row in reader], reader.fieldnames

    def _write_csv(self, data, fieldnames):
        """
        Add the nonprofit ID value back to the CSV. This helps when updating nonprofit data.
        """
        with open(self.file_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

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
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise
        return None

    def _get_nonprofits(self):
        """Fetches existing nonprofit IDs from the API."""
        if self.dry_run:
            self.logger.info(
                f"Dry run step: get the list of the existing non profits- then get the affinity id"
            )
            return []
        response = self._send_request_with_retry(
            self.get_grantees_endpoint, self._headers, json={"pageSize": 10000}
        )

        if response:
            nonprofits = []
            response_data = response.json()
            for item in response_data.get("searchResponse", {}).get("responses", []):
                org_id = item.get("customFields", {}).get("Affinity ID-4jS8olxc")
                nonprofits.append(org_id)
            return nonprofits

    def _create_nonprofit(self, nonprofit_data):
        """
        Create a nonprofit record. If successful, append the organization name to the successful responses list.
        Otherwise, log the failure.
        """
        if self.dry_run:
            self.logger.info(f"Dry run: Would create nonprofit")
            return
        try:
            response = self._send_request_with_retry(
                self.create_nonprofit_endpoint, self._headers, nonprofit_data
            )
            if response:
                self.logger.info(f"Nonprofit created successfully: {response.json()}")
                grantee_id = response.json().get("id")
                self.successful_responses.append(
                    {"org_name": nonprofit_data.get("legalName")}
                )
                return grantee_id
            else:
                self.logger.error(
                    f"Failed to create nonprofit: {response.status_code}, {response.text}"
                )
                self.failed_responses.append(
                    {
                        "message": response.text,
                        "org_name": nonprofit_data.get("legalName"),
                    }
                )
                return None
        except Exception as e:
            self.logger.error(f"Error occurred during API call: {e}")
            self.failed_responses.append(
                {"message": str(e), "org_name": nonprofit_data.get("legalName")}
            )
        return None

    def process_csv(self):
        data, fieldnames = self._read_csv()
        existing_temelio_grants = self._get_nonprofits()
        if "id" not in fieldnames:
            fieldnames.append("id")
        for row in data:
            required_fields = ["Name", "LIF Primary Lead Name"]
            missing_fields = [field for field in required_fields if not row.get(field)]
            if missing_fields:
                self.logger.warning(f"Missing fields {missing_fields} for row {row}")
                self.failed_responses.append(
                    {
                        "message": f"Missing fields: {', '.join(missing_fields)}",
                        "row": row,
                    }
                )
                row["id"] = "Failed"
                continue
            """
            Map the CSV data to the expected Temelio fields.
            """
            if row.get("Name") not in existing_temelio_grants:
                self.logger.info(
                    f"Non profit already exists in temelio..not creating one"
                )
                continue
            nonprofit_data = {
                "legalName": row.get("Name"),
                "primaryContactName": row.get("LIF Primary Lead Name"),
            }
            # Call Temelio create nonprofit endpoint to create the nonprofit record.
            nonprofit_id = self._create_nonprofit(nonprofit_data)
            row["id"] = nonprofit_id if nonprofit_id else "Failed"
        self._write_csv(data, fieldnames)
        if self.dry_run:
            self.logger.info(
                f"Dry run complete: {len(data)} total rows, {len(self.successful_responses)} successful, {len(self.failed_responses)} failed."
            )
            return
        self.logger.info(
            f"Processing complete: {len(self.successful_responses)} success, {len(self.failed_responses)} failed."
        )


if __name__ == "__main__":
    file_path = "/Users/joycendichu/Downloads/!Production data migration - All orgs - grantees.csv"
    grantee = Grantee(file_path, dry_run=True)
    grantee.process_csv()
