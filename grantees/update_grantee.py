import csv
import requests
import logging
import json
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
import os
load_dotenv()

foundation_id = os.getenv("PROD_FOUNDATION_ID")
bearer_token = os.getenv("PROD_BEARER_TOKEN")


HEADERS = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json",
}
users_json = os.getenv("USERS")
users = json.loads(users_json)

def read_csv(file_path):
    """Reads data from a CSV file."""
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]
    
def update_custom_fields(
    nonprofit_id, custom_fields, successful_responses, failed_responses
):
    """Update the custom fields of a nonprofit."""
    update_grantee_endpoint = os.getenv("UPDATE_GRANTEE_PROD_ENDPOINT").format(
        foundation_id, nonprofit_id
    )
    lif_contact_person= custom_fields.get("lif_contact_person").strip()
    if lif_contact_person == "Amolo Ng'weno":
       lif_contact_person = lif_contact_person.replace("Amolo Ngweno", "Amolo Ng'weno")
    elif lif_contact_person == "Seth Aaron Gross Andrew":
        lif_contact_person = lif_contact_person.replace("Seth Aaron Gross Andrew", "Seth Andrews")


    foundation_poc = None
    if lif_contact_person and lif_contact_person != "N/A":
        foundation_poc = users.get(lif_contact_person)
    
    payload = {
        "website": custom_fields.get("website"),
        "description": custom_fields.get("description"),
        "customFields": custom_fields,
        "foundationPOC": {
              "id": foundation_poc},
        "interactionAdditionalInfo": {
            "qbVendorDetails": None,
            "organizationTags": ["Grantee (all time)"],
        },
    }
    response = requests.post(update_grantee_endpoint, headers=HEADERS, json=payload)
    if response.status_code == 204:
        print(f"Successfully updated custom fields for nonprofit {nonprofit_id}")
        successful_responses.append({"non_profit_id": nonprofit_id})
    else:
        print(
            f"Failed to update custom fields for nonprofit {nonprofit_id}: {response.status_code}, {response.text}"
        )
        failed_responses.append(
            {"message": response.text, "non_profit_id": nonprofit_id}
        )


def process_csv(file_path):
    """Process the CSV and update custom fields."""
    successful_responses = []
    failed_responses = []
    data = read_csv(file_path)
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
            "lif_contact_person":row.get("LIF Primary Lead Name"), 
            "End of Accounting Year-XWqSCPwH": row.get("End of Accounting Year")
        }

        # Update custom fieldsRegionLevel of engagementNext Decision Point
        update_custom_fields(
            nonprofit_id, custom_fields, successful_responses, failed_responses
        )
    print(
        f"Processing complete: {len(successful_responses)} success {successful_responses}, {len(failed_responses)} failed {failed_responses}."
    )


if __name__ == "__main__":
    csv_file_path = "/Users/joycendichu/Downloads/!Production data migration - All orgs - grantees.csv"
    process_csv(csv_file_path)
