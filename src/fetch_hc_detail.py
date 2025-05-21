import os
import requests
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Get Grist credentials from environment variables
GRIST_API_KEY = os.getenv('GRIST_API_KEY')
GRIST_DOC_ID = os.getenv('GRIST_DOC_ID')
GRIST_BASE_URL = os.getenv('GRIST_BASE_URL', 'https://docs.getgrist.com')
HOURCLOCK_TABLE_NAME = os.getenv('GRIST_HOURCLOCK_TABLE_NAME', 'HC_Detail')

# Construct the base URL for the Grist document
base_url = f"{GRIST_BASE_URL}/api/docs/{GRIST_DOC_ID}"

# Headers for API requests
headers = {
    "Authorization": f"Bearer {GRIST_API_KEY}",
    "Content-Type": "application/json"
}

def fetch_all_records(table_name):
    """
    Fetches all records from the specified Grist table.

    :param table_name: The name of the table to fetch records from.
    :return: A list of records, or None if an error occurred.
    """
    url = f"{base_url}/tables/{table_name}/records"
    print(f"Fetching all records from: {url}")

    try:
        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if request was successful
        response.raise_for_status()

        # Extract records
        records_data = response.json().get('records', [])

        print(f"Successfully fetched {len(records_data)} records from {table_name}")
        return records_data

    except requests.RequestException as e:
        print(f"Error fetching records from {table_name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None

if __name__ == "__main__":
    print(f"Attempting to fetch records from table: {HOURCLOCK_TABLE_NAME}")
    all_records = fetch_all_records(HOURCLOCK_TABLE_NAME)

    if all_records is not None:
        print("\n--- HC_Detail Records ---")
        for record in all_records:
            # Print the entire record object as a JSON string
            print(json.dumps(record, indent=2))
        print("------------------------")
    else:
        print("Failed to fetch records.")
