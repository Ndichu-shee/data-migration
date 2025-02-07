import os
import csv
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
load_dotenv()


class GrantPayment:
    def __init__(self, file_path, dry_run):
        self.file_path = file_path
        self.dry_run = dry_run
        self.support_type_to_id = json.loads(os.getenv("SUPPORT_TYPES"))
        self.program_areas_to_id = json.loads(os.getenv("PROGRAM_AREAS"))
        self.users = json.loads(os.getenv("USERS"))
        self.pipelines = json.loads(os.getenv("PIPELINES"))
        self.successful_requests = []
        self.failed_requests = []
        self.logger = logging.getLogger(__name__)
        self._load_env_variables()

    def _load_env_variables(self):
        self.foundation_id = os.getenv("PROD_FOUNDATION_ID")
        self._bearer_token = os.getenv("PROD_BEARER_TOKEN")
        self.get_contacts_endpoint = os.getenv("GET_CONTACTS_ENDPOINT").format(
            self.foundation_id
        )
        self.get_grants_endpoint = os.getenv("GET_GRANTS_ENDPOINT").format(
            self.foundation_id
        )
        self.create_grant_endpoint = os.getenv("CREATE_GRANT_ENDPOINT").format(
            self.foundation_id
        )
        self.headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Content-Type": "application/json",
        }

    def read_csv(self):
        """Reads data from a CSV file."""
        if self.dry_run:
            self.logger.info("Dry run first step: read csv")
            return []

        with open(self.file_path, mode="r") as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

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

    def _parse_date(self, date_str):
        """Parses a date string into 'YYYY-MM-DD' format, handling both 2-digit and 4-digit years."""
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        self.logger.info(f"Error parsing date: {date_str}")
        return None

    def _clean_amount(self, amount_str):
        """Converts a comma-separated string to a float or None if invalid."""
        try:
            return float(amount_str.replace(",", ""))
        except ValueError:
            self.logger.info(f"Invalid amount format: {amount_str}")
            return None

    def get_nonprofit_data(self):
        """Fetches nonprofit data from the API."""
        if self.dry_run:
            self.logger.info("Dry run step: get non profit data from temelio")
            return [{"name": "Joyce Nonprofit", "id": 2025}]
        response_data = (self.get_contacts_endpoint, self.headers, {"pageSize": 10000})
        if response_data:
            return response_data.get("searchResponse", {}).get("responses", [])
        return None

    def map_nonprofit_to_csv(self, csv_data, nonprofit_data):
        """Maps nonprofit data to the CSV data based on 'affinity organisation id-MmHY-pXM'."""
        if self.dry_run:
            self.logger.info("Dry run step:Map the csv data to temelio's schema")
            return []
        for record in csv_data:
            org_id = record.get("Organization Id")
            matching_nonprofit = next(
                (
                    np
                    for np in nonprofit_data
                    if np.get("customFields", {}).get("Affinity ID-4jS8olxc")
                    == str(org_id)
                ),
                None,
            )
            record["nonprofitId"] = (
                matching_nonprofit["nonprofitId"] if matching_nonprofit else None
            )
        return csv_data

    def _get_grants_name(self):
        """Fetches the grant names from the API."""
        if self.dry_run:
            self.logger.info("Dry run step:Get existing grantees from temelio")
            return []
        data = requests.get(
            self.get_grants_endpoint,
            self.headers,
            json_payload={"pageSize": 10000},
            method="POST",
        )

        if not data:
            return []

        grant_names = []
        for item in data.get("responses", []):
            grant_name = item.get("name")

            if grant_name:
                grant_names.append(grant_name)
        return grant_names

    def generate_json_request(self, mapped_data):
        """Generates and posts JSON requests based on mapped data."""
        if self.dry_run:
            self.logger.info(
                "Dry run step: Generate json request, this is the request data sent to temelio"
            )
            return {"name": "Joyce Nonprofit", "id": 2025}

        approved_stages = ["Active grant", "Engagement completed"]

        for data in mapped_data:
            name = data.get("Name")
            if name in self._get_grants_name():
                self.logger.info("Grant already exists")
                continue

            self.logger.info("Grant does not exist...creating one")
            support_type = data.get("Support type").replace("S.A.F.E", "SAFE")
            if support_type:
                formatted_date = self._parse_date(
                    data.get("Close Date (decision made)")
                )
                raw_due_date = data.get("Disbursement date") or data.get(
                    "Estimated disbursement date"
                )
                due_date_iso = self._parse_date(raw_due_date)
                year = data.get("LIF Calendar Year")
                status = "SENT" if data.get("Disbursement date") else "NOT_STARTED"
                program_area = data.get("Portfolio [Organization]")
                custom_grant_type_id = self.support_type_to_id.get(support_type)
                custom_program_area = self.program_areas_to_id.get(program_area)
                stage_name = data.get("Stage")
                pipeline = data.get("Pipeline")
                temelio_pipeline = self.pipelines.get(pipeline)
                contact_person_id = None
                lif_contact_person = data.get("LIF Primary Lead").strip()
                if lif_contact_person != "N/A":
                    temelio_assignee = self.users.get(lif_contact_person)
                    contact_person_id = (
                        temelio_assignee
                        if temelio_assignee
                        else "77f5fdb5-3d94-4ac5-b548-b6c021ffc321"
                    )
                else:
                    contact_person_id = "77f5fdb5-3d94-4ac5-b548-b6c021ffc321"

                grant_payments = []
                if stage_name in approved_stages:
                    grant_payments = [
                        {
                            "active": True,
                            "additionalInfo": {
                                "additionalFields": {
                                    "Disbursement Entity-pTa3wGtW": data.get(
                                        "Disbursement Entity"
                                    )
                                },
                                "budgetCategory": data.get("Portfolio [Organization]"),
                            },
                            "amount": self._clean_amount(data.get("Amount")),
                            "assignee": None,
                            "assigneeId": contact_person_id,
                            "comments": None,
                            "dueDate": due_date_iso,
                            "nonprofitId": data.get("nonprofitId"),
                            "status": status,
                            "type": "ACH",
                            "contingencies": None,
                            "created": None,
                            "createdBy": None,
                            "foundation": None,
                            "hasScenario": None,
                            "id": None,
                            "linkedEntities": None,
                            "scenarios": None,
                            "sentDate": None,
                            "sourceId": None,
                            "submission": None,
                            "updated": None,
                            "updatedBy": None,
                        }
                    ]
                json_request = {
                    "formProposalId": None,
                    "foundationId": data.get("foundationId", self.foundation_id),
                    "grantPayments": grant_payments,
                    "grantProposalSubmission": {
                        "additionalInfo": {
                            "entities": [],
                            "grantRefereeInfo": {"grantRefereeRequestDetails": []},
                            "customGrantFields": None,
                            "commentsDisabled": None,
                            "customGrantTypeId": custom_grant_type_id,
                        },
                        "archived": None,
                        "assigneeToTaskTemplates": None,
                        "assigneesToTask": None,
                        "awardedAmount": (
                            self._clean_amount(data.get("Amount"))
                            if stage_name in approved_stages
                            else 0
                        ),
                        "awardedDate": formatted_date,
                        "coloredTags": [],
                        "customEmailTemplate": None,
                        "customGrantType": None,
                        "customProgramAreas": [],
                        "description": "",
                        "disableStageChange": True,
                        "duration": {"start": f"{year}-01-01", "end": f"{year}-12-31"},
                        "entityType": None,
                        "externalAssigneesToTask": None,
                        "firstFormDetails": {
                            "formTitle": "Historical Grants",
                            "internal": False,
                        },
                        "formProposal": None,
                        "foundation": {
                            "id": "0b758d8b-586b-467c-b415-ce853e23379b",
                            "displayName": "Livelihood Impact Fund",
                            "ein": "1",
                            "subdomain": "livelihood",
                            "created": "2025-01-29T21:08:46.560999Z",
                            "vitallyId": "",
                            "accountType": "CLIENT",
                            "logoFile": None,
                            "granteeMFAEnabled": False,
                            "foundationMFAEnabled": False,
                            "currency": {"locale": "en-US", "code": "USD"},
                        },
                        "foundationId": self.foundation_id,
                        "foundationTaskAssignees": None,
                        "foundationWatchers": None,
                        "grantAmount": {
                            "minAmount": self._clean_amount(data.get("Amount"))
                        },
                        "grantFormProposal": "c60a7106-9371-4f27-927d-ce464e3f2ff6",
                        "hasPendingPayments": None,
                        "hasPendingReports": None,
                        "id": None,
                        "multiForm": None,
                        "name": data.get("Name"),
                        "nonprofit": None,
                        "nonprofitId": data.get("nonprofitId"),
                        "nonprofitStage": None,
                        "nonprofitTaskAssignees": None,
                        "organizationName": None,
                        "parentGrant": None,
                        "parentGrantId": None,
                        "parentNonprofit": None,
                        "parentNonprofitId": None,
                        "paymentSummary": None,
                        "pipelineId": temelio_pipeline,
                        "pipelineInfo": None,
                        "programAreas": [custom_program_area],
                        "purpose": "",
                        "readyForNextStage": None,
                        "recipientEmail": None,
                        "responses": [],
                        "scenarios": None,
                        "sendProposalCreatedEmail": False,
                        "stage": stage_name,
                        "submissionBindings": None,
                        "submissionIndividual": None,
                        "submittable": None,
                        "submitted": None,
                        "tags": [],
                        "taskAssignees": None,
                        "taskDeadline": None,
                        "taskIds": None,
                        "taskTemplateResponses": None,
                        "updatedByFoundationUser": None,
                        "updatedByNonprofitUser": None,
                        "watchers": [],
                        "status": "PUBLISHED",
                        "eligibilityEnabled": False,
                        "eligibility": None,
                        "scoringCriteria": None,
                        "visibility": "PRIVATE",
                        "form": {
                            "elements": [],
                            "title": "",
                            "submitButtonText": "Submit",
                        },
                        "draftComponent": None,
                        "published": None,
                    },
                }
                failed_requests, successful_requests = self.post_to_api(
                    json_request, name
                )

        self.logger.info(f"Successfuly created {successful_requests}***")
        self.logger.info(f"Failed to create {failed_requests}***")
        self.logger.info(
            f"Successfuly created {len(successful_requests)}****but had these failed requests {len(failed_requests)}"
        )

    def post_to_api(self, json_request, name):
        """Posts JSON request to the API."""
        if self.dry_run:
            self.logger.info(
                "Dry run step:Call temelio non profit endpoint to create the grant and payment and return list of successful and failed requests"
            )
            return [{"name": "Joyce Nonprofit", "id": 2025}]

        response = self._send_request_with_retry(
            self.create_grant_endpoint, self.headers, json_request
        )

        self.logger.info(f"making requests for {name}####")
        if response:
            response_data = response.json()
            self.logger.info("Successfully posted the data.")
            success_response_body = {
                "name": response_data.get("name"),
            }
            self.successful_requests.append(success_response_body)
        else:
            self.logger.info(
                f"Failed to post the data. Status: {response.status_code}, Response: {response.text}"
            )
            grant_proposal_submission = json_request.get("grantProposalSubmission", {})
            name = grant_proposal_submission.get("name")

            failed_response_body = {"name": name, "message": response.text}
            self.failed_requests.append(failed_response_body)
        return self.failed_requests, self.successful_requests


if __name__ == "__main__":
    csv_file = "/Users/joycendichu/Downloads/!Production data migration - Grant_opportunities_unsaved_view__export_Jan-30-2025.csv"
    grant_payment = GrantPayment(csv_file, dry_run=True)

    csv_data = grant_payment.read_csv()
    nonprofit_data = grant_payment.get_nonprofit_data()

    if nonprofit_data:
        mapped_data = grant_payment.map_nonprofit_to_csv(csv_data, nonprofit_data)

        grant_payment.generate_json_request(mapped_data)
    else:
        grant_payment.logger.info("Failed to retrieve nonprofit data from API.")
