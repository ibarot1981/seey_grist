import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging
import json # Import the json library

# Load environment variables
load_dotenv()

# Configure logging
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO').upper()
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

class HourClockGristUpdater:
    def __init__(self,
                 api_key=None,
                 doc_id=None,
                 hourclock_table_name=None,
                 base_url=None,
                 month_year=None):
        """
        Initialize HourClockGristUpdater

        :param api_key: Grist API key
        :param doc_id: Grist document ID
        :param hourclock_table_name: Name of the HourClock detail table to update
        :param base_url: Optional base URL for custom Grist installations
        :param month_year: Month and year in MMM-YY format from the Excel file
        """
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.doc_id = doc_id or os.getenv('GRIST_DOC_ID')
        self.hourclock_table_name = hourclock_table_name or os.getenv('GRIST_HOURCLOCK_TABLE_NAME', 'HC_Detail')

        # Support for custom Grist installations
        grist_url = base_url or os.getenv('GRIST_BASE_URL', 'https://docs.getgrist.com')
        self.base_url = f"{grist_url}/api/docs/{self.doc_id}"

        self.month_year = month_year

        logging.info(f"Using Grist API at: {self.base_url}")
        logging.info(f"Targeting HourClock table: {self.hourclock_table_name}")

        # Column mappings from Excel HourClock sheet to Grist HC_Detail table
        self.excel_to_grist_mapping = {
            'Sr.No': 'SrNo',
            'EmpNo': 'SFNo',
            # 'Name' is not directly mapped as per user requirement for HC_Detail
            # P and OT columns are handled dynamically
        }

        # Initialize counters for summary
        self._new_records_count = 0
        self._updated_records_count = 0

        # Headers for API requests
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def get_existing_records(self):
        """
        Fetch existing records from Grist HourClock table for the specific month/year

        :return: DataFrame of existing records for the month/year
        """
        try:
            # Construct the API endpoint for fetching records
            # Filter by Month_Year
            # Serialize the filter dictionary to a JSON string
            filter_value_json = json.dumps({"Month_Year": [self.month_year]})

            filter_params = {
                "filter": filter_value_json
            }
            url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"

            logging.info(f"Fetching existing records from: {url} with filter Month_Year = {self.month_year}")

            # Make the GET request with filter
            # requests should URL-encode the filter_value_json string
            response = requests.get(url, headers=self.headers, params=filter_params)

            # Check if request was successful
            response.raise_for_status()

            # Extract records
            records_data = response.json().get('records', [])

            logging.info(f"Fetched {len(records_data)} existing records for {self.month_year} from {self.hourclock_table_name}")

            # If no records, return empty DataFrame but try to get columns
            if not records_data:
                 # Try to get table columns by fetching table schema
                try:
                    schema_url = f"{self.base_url}/tables/{self.hourclock_table_name}"
                    schema_response = requests.get(schema_url, headers=self.headers)
                    schema_response.raise_for_status()
                    fields = schema_response.json().get('fields', {})
                    columns = list(fields.keys()) + ['id']  # Add id column
                    return pd.DataFrame(columns=columns)
                except Exception as e:
                    logging.warning(f"Could not fetch HourClock table schema: {e}")
                    return pd.DataFrame()


            # Convert to DataFrame
            records_df = pd.DataFrame([
                {**record['fields'], 'id': record['id']}
                for record in records_data
            ])

            # Ensure 'SFNo' is treated as string for comparison
            if 'SFNo' in records_df.columns:
                records_df['SFNo'] = records_df['SFNo'].astype(str)

            return records_df

        except requests.RequestException as e:
            logging.error(f"Error fetching existing records from {self.hourclock_table_name}: {e}")
            if hasattr(e.response, 'text'):
                logging.error(f"Response: {e.response.text}")
            return pd.DataFrame()

    def compare_and_update(self, excel_data):
        """
        Compare Excel HourClock data with existing Grist records and update/add

        :param excel_data: DataFrame with Excel HourClock data
        """
        if self.month_year is None:
            logging.error("Month-year is not set. Cannot process HourClock data.")
            return

        logging.info("Excel data columns received by compare_and_update:")
        logging.info(excel_data.columns)
        logging.info("First 5 rows of excel_data received by compare_and_update:")
        logging.info(excel_data.head().to_string()) # Use to_string() to ensure full output

        try:
            # Fetch existing HourClock records for the specific month/year
            existing_records = self.get_existing_records()

            if existing_records.empty and not excel_data.empty:
                logging.info(f"No existing records found in Grist table {self.hourclock_table_name} for {self.month_year}. All records will be added as new.")

            # Make a copy of the data to avoid modifying the original
            excel_data = excel_data.copy()

            # Remove rows with NaN or null in the 'SFNo' column (using the new column name)
            if 'SFNo' in excel_data.columns:
                null_emp_nos = excel_data['SFNo'].isna()
                if null_emp_nos.any():
                    logging.warning(f"Warning: Found {null_emp_nos.sum()} rows with empty employee numbers in HourClock sheet. These will be skipped.")
                    excel_data = excel_data.dropna(subset=['SFNo'])

                # Also remove rows where 'SFNo' is 'nan' as a string
                nan_emp_nos = excel_data['SFNo'] == 'nan'
                if nan_emp_nos.any():
                    logging.warning(f"Warning: Found {nan_emp_nos.sum()} rows with 'nan' as employee number in HourClock sheet. These will be skipped.")
                    excel_data = excel_data[~nan_emp_nos]

            # Ensure 'SFNo' is treated as string and strip whitespace
            if 'SFNo' in excel_data.columns:
                excel_data['SFNo'] = excel_data['SFNo'].astype(str).str.strip()

            # If SFNo exists in existing_records, make sure it's a string for comparison
            if not existing_records.empty and 'SFNo' in existing_records.columns:
                existing_records['SFNo'] = existing_records['SFNo'].astype(str)

            # Check for duplicate SFNo in Excel data (using the new column name)
            if 'SFNo' in excel_data.columns:
                duplicates = excel_data['SFNo'].duplicated()
                if duplicates.any():
                    duplicate_emp_nos = excel_data.loc[duplicates, 'SFNo'].tolist()
                    logging.warning(f"Warning: Duplicate employee numbers found in HourClock Excel sheet: {duplicate_emp_nos}")
                    logging.warning("Only the last occurrence of each duplicate will be processed.")
                    # Keep only the last occurrence of each duplicate
                    excel_data = excel_data.drop_duplicates(subset=['SFNo'], keep='last')

            # Prepare lists for operations
            records_to_add = []
            updates_to_perform = []

            logging.info(f"Processing {len(excel_data)} valid rows from HourClock Excel sheet")

            # Process each row from Excel
            for _, excel_row in excel_data.iterrows():
                emp_no = str(excel_row['SFNo']) # Use SFNo here
                sr_no = excel_row.get('Sr.No') # Use .get for safety, and 'Sr.No' as per Excel column name

                # Prepare Grist fields for the HourClock table
                grist_hourclock_fields = {
                    'Month_Year': self.month_year,
                    'SFNo': emp_no,
                    'SrNo': sr_no if pd.notna(sr_no) else None # Map Sr.No to SrNo
                }

                # Dynamically add P and OT columns for each day
                for day in range(1, 32):
                    p_col_excel = f'P{day}'
                    ot_col_excel = f'OT{day}'
                    p_col_grist = f'P{day}'
                    ot_col_grist = f'OT{day}'

                    # Get values, handling potential missing columns or NaN
                    p_value = excel_row.get(p_col_excel)
                    ot_value = excel_row.get(ot_col_excel)

                    # Convert P value to integer (0 or 1), handle NaN/errors
                    if pd.notna(p_value):
                        try:
                            grist_hourclock_fields[p_col_grist] = int(p_value)
                        except (ValueError, TypeError):
                            logging.warning(f"Warning: Could not convert P value '{p_value}' to integer for EmpNo {emp_no}, Day {day}. Setting to None.")
                            grist_hourclock_fields[p_col_grist] = None
                    else:
                        grist_hourclock_fields[p_col_grist] = None # Set to None if NaN

                    # Convert OT value to float, handle NaN/errors
                    if pd.notna(ot_value):
                        try:
                            grist_hourclock_fields[ot_col_grist] = float(ot_value)
                        except (ValueError, TypeError):
                            logging.warning(f"Warning: Could not convert OT value '{ot_value}' to float for EmpNo {emp_no}, Day {day}. Setting to None.")
                            grist_hourclock_fields[ot_col_grist] = None
                    else:
                        grist_hourclock_fields[ot_col_grist] = None # Set to None if NaN


                # Find if record for this employee and month/year exists in Grist
                matched_records = pd.DataFrame()
                if not existing_records.empty and 'SFNo' in existing_records.columns and 'Month_Year' in existing_records.columns:
                    matched_records = existing_records[
                        (existing_records['SFNo'] == emp_no) &
                        (existing_records['Month_Year'] == self.month_year)
                    ]

                if matched_records.empty:
                    # Scenario: New record for this employee and month/year
                    logging.info(f"Attempting to add new HourClock record for employee {emp_no} ({self.month_year}).")
                    records_to_add.append({'fields': grist_hourclock_fields})

                else:
                    # Scenario: Existing record for this employee and month/year
                    record_id = matched_records['id'].iloc[0]
                    current_grist_record = matched_records.iloc[0]

                    # Compare fields to see if an update is needed
                    needs_update = False
                    update_payload_fields = {}

                    # Compare mapped fields (No, SFNo, Month_Year - though Month_Year/SFNo are keys)
                    for excel_col, grist_col in self.excel_to_grist_mapping.items():
                         if excel_col in excel_row.index and grist_col in current_grist_record:
                             excel_value = excel_row[excel_col]
                             grist_value = current_grist_record[grist_col]

                             # Handle None/NaN comparison
                             if not pd.isna(excel_value) or not pd.isna(grist_value):
                                 excel_str = str(excel_value) if pd.notna(excel_value) else 'None'
                                 grist_str = str(grist_value) if pd.notna(grist_value) else 'None'

                                 if excel_str != grist_str:
                                     needs_update = True
                                     update_payload_fields[grist_col] = grist_hourclock_fields[grist_col] # Use the prepared value
                                     logging.debug(f"DEBUG: Update needed for {emp_no} ({self.month_year}): {grist_col} differs (Excel: '{excel_str}', Grist: '{grist_str}')")


                    # Compare P and OT columns
                    for day in range(1, 32):
                        p_col_grist = f'P-{day}'
                        ot_col_grist = f'OT-{day}'

                        if p_col_grist in current_grist_record:
                            current_p_value = current_grist_record[p_col_grist]
                            new_p_value = grist_hourclock_fields.get(p_col_grist) # Get the prepared new value

                            # Compare P values (handle None/NaN and integer comparison)
                            if (pd.isna(current_p_value) and pd.notna(new_p_value)) or \
                               (pd.notna(current_p_value) and pd.isna(new_p_value)) or \
                               (pd.notna(current_p_value) and pd.notna(new_p_value) and int(current_p_value) != new_p_value): # Ensure comparison as int
                                needs_update = True
                                update_payload_fields[p_col_grist] = new_p_value
                                logging.debug(f"DEBUG: Update needed for {emp_no} ({self.month_year}): {p_col_grist} differs (Excel: {new_p_value}, Grist: {current_p_value})")


                        if ot_col_grist in current_grist_record:
                            current_ot_value = current_grist_record[ot_col_grist]
                            new_ot_value = grist_hourclock_fields.get(ot_col_grist) # Get the prepared new value

                            # Compare OT values (handle None/NaN and float comparison)
                            if (pd.isna(current_ot_value) and pd.notna(new_ot_value)) or \
                               (pd.notna(current_ot_value) and pd.isna(new_ot_value)) or \
                               (pd.notna(current_ot_value) and pd.notna(new_ot_value) and float(current_ot_value) != new_ot_value): # Ensure comparison as float
                                needs_update = True
                                update_payload_fields[ot_col_grist] = new_ot_value
                                logging.debug(f"DEBUG: Update needed for {emp_no} ({self.month_year}): {ot_col_grist} differs (Excel: {new_ot_value}, Grist: {current_ot_value})")


                    if needs_update:
                        updates_to_perform.append({
                            'id': int(record_id),
                            'fields': update_payload_fields
                        })
                        logging.info(f"HourClock record for employee {emp_no} ({self.month_year}) queued for update.")
                    else:
                        logging.info(f"HourClock record for employee {emp_no} ({self.month_year}): No update needed.")


            # Perform bulk add operations
            if records_to_add:
                add_url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"
                logging.info(f"Adding {len(records_to_add)} new HourClock records to {self.hourclock_table_name}.")
                if records_to_add: # Debug sample
                    logging.debug(f"Sample add record for HourClock table: {records_to_add[0]}")

                try:
                    add_response = requests.post(
                        add_url,
                        headers=self.headers,
                        json={'records': records_to_add}
                    )
                    add_response.raise_for_status()
                    logging.info(f"Successfully added {len(records_to_add)} new HourClock records.")
                    self._new_records_count += len(records_to_add)
                except requests.RequestException as e:
                    logging.error(f"Error adding new HourClock records: {e}")
                    if hasattr(e.response, 'text'):
                        logging.error(f"Response: {e.response.text}")

            # Perform bulk update operations
            if updates_to_perform:
                update_url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"
                logging.info(f"Updating {len(updates_to_perform)} existing HourClock records in {self.hourclock_table_name}.")
                if updates_to_perform: # Debug sample
                    logging.debug(f"Sample update record for HourClock table: {updates_to_perform[0]}")

                try:
                    update_response = requests.patch(
                        update_url,
                        headers=self.headers,
                        json={'records': updates_to_perform}
                    )
                    update_response.raise_for_status()
                    logging.info(f"Successfully updated {len(updates_to_perform)} existing HourClock records.")
                    self._updated_records_count += len(updates_to_perform)
                except requests.RequestException as e:
                    logging.error(f"Error updating existing HourClock records: {e}")
                    if hasattr(e.response, 'text'):
                        logging.error(f"Response: {e.response.text}")

        except requests.RequestException as e:
            logging.error(f"A Grist API request failed during the HourClock process: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response: {e.response.text}")
        except Exception as e:
            import traceback
            logging.error(f"Unexpected error during HourClock update: {e}")
            logging.error(traceback.format_exc())

        # Print summary of actions
        logging.info("\n--- HourClock Update Summary ---")
        logging.info(f"New HourClock records added: {self._new_records_count}")
        logging.info(f"Existing HourClock records updated: {self._updated_records_count}")
        logging.info("------------------------------\n")

# Example usage (for testing purposes)
if __name__ == "__main__":
    # This requires a Grist document with a table named 'HC_Detail'
    # and appropriate environment variables set (.env file)
    # GRIST_API_KEY, GRIST_DOC_ID, GRIST_HOURCLOCK_TABLE_NAME=HC_Detail

    # Create a dummy DataFrame for HourClock sheet data
    data = {'Sr.No': [1, 2],
            'EmpNo': ['E001', 'E002'],
            'Name': ['User One', 'User Two']} # Name is not used in updater, but might be in Excel
    for day in range(1, 32):
        data[f'P{day}'] = [1, 0]
        data[f'OT{day}'] = [2.5, 0] # Example float OT

    dummy_hourclock_df = pd.DataFrame(data)

    # Example month/year
    dummy_month_year = 'May-25'

    # Initialize the updater
    updater = HourClockGristUpdater(month_year=dummy_month_year)

    # Run the update process
    updater.compare_and_update(dummy_hourclock_df)
