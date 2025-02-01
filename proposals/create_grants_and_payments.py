import os
import csv
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
load_dotenv()

foundation_id = os.getenv("PROD_FOUNDATION_ID")
bearer_token = os.getenv("PROD_BEARER_TOKEN")
get_contacts_endpoint = os.getenv("GET_CONTACTS_ENDPOINT").format(foundation_id)
get_grants_endpoint = os.getenv("GET_GRANTS_ENDPOINT").format(foundation_id)
create_grant_endpoint = os.getenv("CREATE_GRANT_ENDPOINT").format(foundation_id)
HEADERS = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json",
}
support_type_json = os.getenv("SUPPORT_TYPES")
support_type_to_id = json.loads(support_type_json)

program_areas_json = os.getenv("PROGRAM_AREAS")
program_areas_to_id = json.loads(program_areas_json)

users_json = os.getenv("USERS")
users = json.loads(users_json)

pipeline_json = os.getenv("PIPELINES")
pipelines = json.loads(pipeline_json)


def read_csv(file_path):
    """Reads data from a CSV file."""
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        return [row for row in reader]


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


def parse_date(date_str):
    """Parses a date string into 'YYYY-MM-DD' format, handling both 2-digit and 4-digit years."""
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.info(f"Error parsing date: {date_str}")
    return None


def clean_amount(amount_str):
    """Converts a comma-separated string to a float or None if invalid."""
    try:
        return float(amount_str.replace(",", ""))
    except ValueError:
        logger.info(f"Invalid amount format: {amount_str}")
        return None


def get_nonprofit_data(foundation_id):
    """Fetches nonprofit data from the API."""

    json_request = {"pageSize": 10000}
    response_data = send_request(get_contacts_endpoint, HEADERS, json_request)
    if response_data:
        return response_data.get("searchResponse", {}).get("responses", [])
    return None


def map_nonprofit_to_csv(csv_data, nonprofit_data):
    """Maps nonprofit data to the CSV data based on 'affinity organisation id-MmHY-pXM'."""
    for record in csv_data:
        org_id = record.get("Organization Id")
        matching_nonprofit = next(
            (
                np
                for np in nonprofit_data
                if np.get("customFields", {}).get("Affinity ID-4jS8olxc") == str(org_id)
            ),
            None,
        )
        record["nonprofitId"] = (
            matching_nonprofit["nonprofitId"] if matching_nonprofit else None
        )
    return csv_data


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
        grant_name = item.get("name")

        if grant_name:
            grant_names.append(grant_name)
    return grant_names


def generate_json_request(mapped_data):
    """Generates and posts JSON requests based on mapped data."""

    approved_stages = ["Active grant", "Engagement completed"]

    for data in mapped_data:
        name = data.get("Name")
        if name in get_grants_name():
            logger.info("Grant already exists")
            continue

        logger.info("Grant does not exist...creating one")
        support_type = data.get("Support type").replace("S.A.F.E", "SAFE")
        if support_type:
            formatted_date = parse_date(data.get("Close Date (decision made)"))
            raw_due_date = data.get("Disbursement date") or data.get(
                "Estimated disbursement date"
            )
            due_date_iso = parse_date(raw_due_date)
            year = data.get("LIF Calendar Year")
            status = "SENT" if data.get("Disbursement date") else "NOT_STARTED"
            program_area = data.get("Portfolio [Organization]")
            custom_grant_type_id = support_type_to_id.get(support_type)
            custom_program_area = program_areas_to_id.get(program_area)
            stage_name = data.get("Stage")
            pipeline = data.get("Pipeline")
            temelio_pipeline = pipelines.get(pipeline)
            contact_person_id = None
            lif_contact_person = data.get("LIF Primary Lead").strip()
            if lif_contact_person != "N/A":
                temelio_assignee = users.get(lif_contact_person)
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
                        "amount": clean_amount(data.get("Amount")),
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
            non = data.get("nonprofitId")
            json_request = {
                "formProposalId": None,
                "foundationId": data.get("foundationId", foundation_id),
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
                        clean_amount(data.get("Amount"))
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
                    "foundationId": foundation_id,
                    "foundationTaskAssignees": None,
                    "foundationWatchers": None,
                    "grantAmount": {"minAmount": clean_amount(data.get("Amount"))},
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
                    "form": {"elements": [], "title": "", "submitButtonText": "Submit"},
                    "draftComponent": None,
                    "published": None,
                },
            }
            failed_requests, successful_requests = post_to_api(json_request, name)

    logger.info(f"Successfuly created {successful_requests}***")
    logger.info(f"Failed to create {failed_requests}***")
    logger.info(
        f"Successfuly created {len(successful_requests)}****but had these failed requests {len(failed_requests)}"
    )


successful_requests = []
failed_requests = []


def post_to_api(json_request, name):
    """Posts JSON request to the API."""
    response = requests.post(create_grant_endpoint, json=json_request, headers=HEADERS)

    logger.info(f"making requests for {name}####")
    if response.status_code == 200:
        response_data = response.json()
        logger.info("Successfully posted the data.")
        success_response_body = {
            "name": response_data.get("name"),
        }
        successful_requests.append(success_response_body)
    else:
        logger.info(
            f"Failed to post the data. Status: {response.status_code}, Response: {response.text}"
        )
        grant_proposal_submission = json_request.get("grantProposalSubmission", {})
        name = grant_proposal_submission.get("name")

        failed_response_body = {"name": name, "message": response.text}
        failed_requests.append(failed_response_body)
    return failed_requests, successful_requests


if __name__ == "__main__":
    csv_file = "/Users/joycendichu/Downloads/!Production data migration - Grant_opportunities_unsaved_view__export_Jan-30-2025.csv"
    foundation_id = foundation_id

    csv_data = read_csv(csv_file)
    nonprofit_data = get_nonprofit_data(foundation_id)

    if nonprofit_data:
        mapped_data = map_nonprofit_to_csv(csv_data, nonprofit_data)

        generate_json_request(mapped_data)
    else:
        logger.info("Failed to retrieve nonprofit data from API.")
