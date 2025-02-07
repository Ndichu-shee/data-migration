import csv
import requests
import logging
import json
import os
import time

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class GranteeUpdater:
    def __init__(self, file_path, dry_run=False):
        self.file_path = file_path
        self._load_env_variables()
        self.successful_responses = []
        self.failed_responses = []
        self.logger = logging.getLogger(__name__)
        self.dry_run = dry_run

    def _load_env_variables(self):
        self.foundation_id = os.getenv("PROD_FOUNDATION_ID")
        self._bearer_token = os.getenv("PROD_BEARER_TOKEN")
        self.update_grantee_endpoint = os.getenv("UPDATE_GRANTEE_PROD_ENDPOINT")
        self.headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Content-Type": "application/json",
        }
        self.users_json = os.getenv("USERS")
        self.users = json.loads

    def _read_csv(self):
        if self.dry_run:
            self.logger.info("Dry run step: read the non profits csv")

        """Reads data from a CSV file."""
        with open(self.file_path, mode="r") as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

    def _send_request_with_retry(self, url, headers, data, retries=3, delay=5):
        for attempt in range(retries):
            try:
                response = requests.post(url, headers=headers, json=data, timeout=30)
                if response.status_code == 204:
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

    def _update_custom_fields(self, non_profit_id, custom_fields):
        if self.dry_run:
            self.logger.info(
                "Dry run would update the custom fields for the non profits"
            )
            return
        """Update the custom fields of a nonprofit."""
        update_grantee_record_endpoint = self.update_grantee_endpoint.format(
            self.foundation_id, non_profit_id
        )
        # normalize the text
        lif_contact_person = custom_fields.get("lif_contact_person").strip()
        if lif_contact_person == "Amolo Ng'weno":
            lif_contact_person = lif_contact_person.replace(
                "Amolo Ngweno", "Amolo Ng'weno"
            )
        elif lif_contact_person == "Seth Aaron Gross Andrew":
            lif_contact_person = lif_contact_person.replace(
                "Seth Aaron Gross Andrew", "Seth Andrews"
            )

        foundation_poc = None
        if lif_contact_person and lif_contact_person != "N/A":
            foundation_poc = self.users.get(lif_contact_person)

        payload = {
            "website": custom_fields.get("website"),
            "description": custom_fields.get("description"),
            "customFields": custom_fields,
            "foundationPOC": {"id": foundation_poc},
            "interactionAdditionalInfo": {
                "qbVendorDetails": None,
                "organizationTags": ["Grantee (all time)"],
            },
        }
        response = self._send_request_with_retry(
            update_grantee_record_endpoint, self.headers, payload
        )

        if response:
            self.logger.info(
                f"Successfully updated custom fields for nonprofit {non_profit_id}"
            )
            self.successful_responses.append({"non_profit_id": non_profit_id})
        else:
            self.logger.error(
                f"Failed to update custom fields for nonprofit {non_profit_id}: {response.status_code}, {response.text}"
            )
            self.failed_responses.append(
                {"message": response.text, "non_profit_id": non_profit_id}
            )

    def process_csv(self, file_path):
        """Process the CSV and update custom fields."""
        successful_responses = []
        failed_responses = []
        data = self._read_csv(file_path)
        for row in data:
            # Extract nonprofit ID and custom fields from CSV
            nonprofit_id = row["id"]
            custom_fields = {
                "Area of intervention-1Dl5ES7a": row.get("Area of intervention"),
                "Org type-ngM_Rj--": row.get("Org type"),
                "Operate in-h0GCmal-": row.get("Operate in"),
                "description": row.get("Mission"),
                "Level of Engagement-tuTYKb5E": row.get("Level of engagement"),
                "Affinity ID-4jS8olxc": row.get("Organization Id"),
                "website": row.get("Website"),
                "Portfolio-WgSIOWIz": row.get("Portfolio"),
                "Region-ObIfV84Z": row.get("Region"),
                "Status (manual)-Jj5hsNIX": row.get("Grantee Status"),
                "lif_contact_person": row.get("LIF Primary Lead Name"),
                "End of Accounting Year-XWqSCPwH": row.get("End of Accounting Year"),
            }

            # Update custom fieldsRegionLevel of engagementNext Decision Point
            self._update_custom_fields(
                nonprofit_id, custom_fields, successful_responses, failed_responses
            )
        if self.dry_run:
            self.logger("Dry run would return the successful and failed responses")
            return
        self.logger.info(
            f"Processing complete: {len(successful_responses)} success {successful_responses}, {len(failed_responses)} failed {failed_responses}."
        )


if __name__ == "__main__":
    csv_file_path = "/Users/joycendichu/Downloads/!Production data migration - All orgs - grantees.csv"

    grantee_processor = GranteeUpdater(csv_file_path, dry_run=True)
    grantee_processor.process_csv()
