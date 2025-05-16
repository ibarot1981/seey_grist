import os
import pandas as pd
from dotenv import load_dotenv
from grist_updater import GristUpdater # Assuming GristUpdater is in the same directory or accessible

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Grist credentials and document details will be loaded from environment variables
# using the GristUpdater class.
OUTPUT_FILE = 'grist_extracted_data.txt'
REQUIRED_FIELDS = ['SFNo', 'DOJ', 'Created_at', 'Last_updated_at']

# --- Main Extraction Logic ---
def extract_data_to_text_file():
    """
    Extracts specified fields from a Grist document and saves them to a text file.
    """
    print("Initializing GristUpdater...")
    try:
        # GristUpdater will load API key, doc ID, and table name from environment variables
        grist_updater = GristUpdater()
        print("GristUpdater initialized.")
    except Exception as e:
        print(f"Error initializing GristUpdater: {e}")
        print("Please ensure GRIST_API_KEY, GRIST_DOC_ID, and GRIST_TABLE_NAME are set in your .env file.")
        return

    print(f"Fetching records from Grist document ID: {grist_updater.doc_id}, Table: {grist_updater.main_table_name}...")
    records_df = grist_updater.get_existing_records()

    if records_df.empty:
        print("No records fetched from Grist. Output file will be empty.")
        # Create an empty output file to indicate the process ran but found no data
        with open(OUTPUT_FILE, 'w') as f:
            f.write("No records found.\n")
        return

    print(f"Successfully fetched {len(records_df)} records.")

    # Check if all required fields exist in the DataFrame columns
    missing_fields = [field for field in REQUIRED_FIELDS if field not in records_df.columns]
    if missing_fields:
        print(f"Error: Missing required fields in Grist data: {', '.join(missing_fields)}")
        print("Please ensure the Grist table contains these columns.")
        # Optionally, save a file indicating the error
        with open(OUTPUT_FILE, 'w') as f:
            f.write(f"Error: Missing required fields in Grist data: {', '.join(missing_fields)}\n")
        return

    print(f"Extracting fields: {', '.join(REQUIRED_FIELDS)}")

    # Select only the required columns
    extracted_data_df = records_df[REQUIRED_FIELDS]

    # Format date fields if they are datetime objects (pandas might read them as such)
    # Ensure date fields are formatted as strings
    for field in ['DOJ', 'Created_at', 'Last_updated_at']:
        if field in extracted_data_df.columns:
            # Add debugging print to see raw date values
            print(f"DEBUG: Raw values for field '{field}':")
            print(extracted_data_df[field].head()) # Print first few values

            # Convert to datetime objects from Unix timestamps, then format, using .loc
            # Use unit='s' to interpret the float values as seconds since the epoch
            extracted_data_df.loc[:, field] = pd.to_datetime(extracted_data_df[field], unit='s', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            # Handle cases where conversion failed (NaT) using .loc
            extracted_data_df.loc[:, field] = extracted_data_df[field].fillna('')


    print(f"Writing extracted data to {OUTPUT_FILE}...")
    try:
        # Write to text file, using a simple format like tab-separated values
        # Include a header row
        extracted_data_df.to_csv(OUTPUT_FILE, sep='\t', index=False)
        print(f"Data successfully written to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error writing data to {OUTPUT_FILE}: {e}")

if __name__ == "__main__":
    extract_data_to_text_file()
