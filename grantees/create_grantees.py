import csv
import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()


logger = logging.getLogger(__name__)
foundation_id = os.getenv("PROD_FOUNDATION_ID")
create_nonprofit_endpoint = os.getenv("CREATE_GRANTEE_PROD_ENDPOINT").format(
    foundation_id
)
bearer_token = os.getenv("PROD_BEARER_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json",
}


def read_csv(file_path):
    """
    read the grantees csv and return the dict of each row
    """
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        return [row for row in reader], reader.fieldnames


def write_csv(file_path, data, fieldnames):
    """
    add the non profit id value back to the csv, this is help when update non profit data
    """
    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def create_nonprofit(nonprofit_data, successful_responses, failed_responses):
    """
    create each record, if the response is 200, append the org name to the sucessful response
    else add the response text to the failed responses
    """
    try:
        response = requests.post(
            create_nonprofit_endpoint, headers=HEADERS, json=nonprofit_data
        )
        if response.status_code == 200:
            logger.info(f"Nonprofit created successfully: {response.json()}")
            grantee_id = response.json().get("id")
            successful_responses.append({"org_name": nonprofit_data.get("legalName")})
            return grantee_id
        else:
            logger.error(
                f"Failed to create nonprofit: {response.status_code}, {response.text}"
            )
            failed_responses.append(
                {"message": response.text, "org_name": nonprofit_data.get("legalName")}
            )
            return None
    except Exception as e:
        logger.error(f"Error occurred during API call: {e}")
        failed_responses.append(
            {"message": str(e), "org_name": nonprofit_data.get("legalName")}
        )
        return None


def process_csv(file_path):
    data, fieldnames = read_csv(file_path)

    if "id" not in fieldnames:
        fieldnames.append("id")

    successful_responses = []
    failed_responses = []

    for row in data:
        required_fields = [ "Name",  "LIF Primary Lead Name"]
    
        missing_fields = [field for field in required_fields if not row.get(field)]
        
        if missing_fields:
            failed_responses.append(f"Missing fields: {', '.join(missing_fields)} for row: {row}")
            row["id"] = "Failed"
            continue
        """
        map the csv data with the expected temelio fields
        """
        nonprofit_data = {
            "legalName": row.get("Name"),
            "primaryContactName": row.get("LIF Primary Lead Name"),
        }
        # call temelio create non profit endpoint to create the non profit record
        nonprofit_id = create_nonprofit(
            nonprofit_data, successful_responses, failed_responses
        )
        row["id"] = nonprofit_id if nonprofit_id else "Failed"

    write_csv(file_path, data, fieldnames)

    logger.info(
        f"Processing complete: {len(successful_responses)} success {successful_responses}, {len(failed_responses)} failed {failed_responses}."
    )


if __name__ == "__main__":
    csv_file_path = "/Users/joycendichu/Downloads/!Production data migration - All orgs - grantees.csv"
    process_csv(csv_file_path)
