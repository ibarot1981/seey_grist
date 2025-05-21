import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging
import json

# Get logger for this module
logger = logging.getLogger(__name__)

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

        logger.info(f"Using Grist API at: {self.base_url}")
        logger.info(f"Targeting HourClock table: {self.hourclock_table_name}")

        # Initialize counters for summary
        self._new_records_count = 0
        self._updated_records_count = 0

        # Headers for API requests
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Store table schema to validate field names
        self.table_columns = []
        self._fetch_table_schema()

    def _fetch_table_schema(self):
        """
        Fetch the table schema to know which columns actually exist in Grist
        """
        try:
            # Prioritize fetching column names from the /columns endpoint as suggested
            columns_url = f"{self.base_url}/tables/{self.hourclock_table_name}/columns"
            columns_response = requests.get(columns_url, headers=self.headers)
            columns_response.raise_for_status()
            columns_data = columns_response.json()

            # Access the list of columns from the 'columns' key
            column_list = columns_data.get('columns', [])

            if isinstance(column_list, list):
                self.table_columns = [col.get('id') for col in column_list if isinstance(col, dict) and 'id' in col]
                logger.info(f"Fetched table columns from /columns endpoint: {len(self.table_columns)} columns")
                logger.info(f"Available columns: {', '.join(sorted(self.table_columns))}")

                # Check for P_* and OT_* columns specifically
                p_columns = [col for col in self.table_columns if col.startswith('P_')]
                ot_columns = [col for col in self.table_columns if col.startswith('OT_')]
                logger.info(f"Found {len(p_columns)} P_* columns: {', '.join(sorted(p_columns))}")
                logger.info(f"Found {len(ot_columns)} OT_* columns: {', '.join(sorted(ot_columns))}")

                # Look for case sensitivity issues and common naming variations
                lowercase_columns = [col.lower() for col in self.table_columns]
                if 'ot_1' in lowercase_columns and 'OT_1' not in self.table_columns:
                    logger.warning("Case sensitivity issue detected: 'ot_1' exists but 'OT_1' does not")

                variations = {
                    'OT_': ['OT_', 'ot_', 'OT-', 'ot-'],
                    'P_': ['P_', 'p_', 'P-', 'p-']
                }
                for base_prefix, prefixes in variations.items():
                    for day in range(1, 32):  # Days 1-31
                        variations_found = []
                        for prefix in prefixes:
                            col_name = f"{prefix}{day}"
                            if col_name in self.table_columns:
                                variations_found.append(col_name)

                        if variations_found and len(variations_found) > 1:
                            logger.warning(f"Multiple variations found for day {day}: {variations_found}")
                        elif not any(f"{prefix}{day}" in self.table_columns for prefix in prefixes):
                            if day <= 28:  # Only warn for expected days
                                logger.warning(f"No column variation found for {base_prefix}{day}")

            else:
                logger.warning("Unexpected response format from /columns endpoint.")
                logger.warning(f"Raw response content: {columns_response.text}") # Log raw response
                self.table_columns = [] # Ensure it's an empty list on unexpected format

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching table columns from /columns endpoint: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            self.table_columns = [] # Ensure it's an empty list on error

        except Exception as e:
            import traceback
            logger.error(f"Unexpected error during table schema fetch: {e}")
            logger.error(traceback.format_exc())
            self.table_columns = [] # Ensure it's an empty list on unexpected error

    def get_existing_records(self):
        """
        Fetch existing records from Grist HourClock table for the specific month/year

        :return: DataFrame of existing records for the month/year
        """
        try:
            # Construct the API endpoint for fetching records
            # Filter by Month_Year
            filter_value_json = json.dumps({"Month_Year": [self.month_year]})

            filter_params = {
                "filter": filter_value_json
            }
            url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"

            logger.info(f"Fetching existing records from: {url} with filter Month_Year = {self.month_year}")

            # Make the GET request with filter
            response = requests.get(url, headers=self.headers, params=filter_params)

            # Check if request was successful
            response.raise_for_status()

            # Extract records
            records_data = response.json().get('records', [])

            logger.info(f"Fetched {len(records_data)} existing records for {self.month_year} from {self.hourclock_table_name}")

            # If no records, return empty DataFrame.
            # We rely on the schema fetched in __init__ for column validation.
            if not records_data:
                # If table_columns is empty, it means the initial schema fetch failed.
                # In this case, we cannot determine the columns, so return empty DataFrame.
                if not self.table_columns:
                     logger.warning("Initial table schema fetch failed, and no existing records found. Cannot determine columns.")
                     return pd.DataFrame()

                # If table_columns is not empty, use those columns for the empty DataFrame
                columns = self.table_columns + ['id'] # Add id column if not present
                if 'id' not in self.table_columns:
                     columns = self.table_columns + ['id']
                else:
                     columns = self.table_columns

                return pd.DataFrame(columns=columns)

            # Convert to DataFrame
            records_df = pd.DataFrame([
                {**record['fields'], 'id': record['id']}
                for record in records_data
            ])

            # Ensure 'SFNo' is treated as string for comparison
            if 'SFno' in records_df.columns:
                records_df['SFno'] = records_df['sfno'].astype(str)

            return records_df

        except requests.RequestException as e:
            logger.error(f"Error fetching existing records from {self.hourclock_table_name}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return pd.DataFrame()

    def compare_and_update(self, excel_data):
        """
        Compares the provided Excel hour clock data with existing records in the Grist
        HourClock detail table for the specified month and year. It identifies new
        records to be added and existing records that need updating based on changes
        in the Excel data. Finally, it performs bulk add and update operations
        to synchronize the Grist table with the Excel data.

        :param excel_data: A pandas DataFrame containing the hour clock data read
                           from the Excel file. Expected columns include 'SFNo',
                           'No', and columns for each day of the month prefixed
                           with 'P-' or 'P_' (for presence) and 'OT-' or 'OT_'
                           (for overtime hours).
        """
        if self.month_year is None:
            logger.error("Month-year is not set. Cannot process HourClock data.")
            return

        if not self.table_columns:
            logger.error("Grist table schema not available. Cannot proceed with comparing and updating records.")
            return

        logger.info("Excel data columns received by compare_and_update:")
        logger.info(excel_data.columns)
        logger.info("First 5 rows of excel_data received by compare_and_update:")
        logger.info(excel_data.head().to_string())

        # Print P and OT column patterns from Excel data to help diagnose issues
        p_cols = [col for col in excel_data.columns if 'P-' in col or 'P_' in col]
        ot_cols = [col for col in excel_data.columns if 'OT-' in col or 'OT_' in col]
        logger.info(f"P columns in Excel: {', '.join(sorted(p_cols))}")
        logger.info(f"OT columns in Excel: {', '.join(sorted(ot_cols))}")

        try:
            # Fetch existing HourClock records for the specific month/year
            existing_records = self.get_existing_records()

            if existing_records.empty and not excel_data.empty:
                logger.info(f"No existing records found in Grist table {self.hourclock_table_name} for {self.month_year}. All records will be added as new.")

            # Make a copy of the data to avoid modifying the original
            excel_data = excel_data.copy()

            # Remove rows with NaN or null in the 'SFNo' column
            if 'SFNo' in excel_data.columns:
                null_emp_nos = excel_data['SFNo'].isna()
                if null_emp_nos.any():
                    logger.warning(f"Warning: Found {null_emp_nos.sum()} rows with empty employee numbers in HourClock sheet. These will be skipped.")
                    excel_data = excel_data.dropna(subset=['SFNo'])

                # Also remove rows where 'SFNo' is 'nan' as a string
                nan_emp_nos = excel_data['SFNo'] == 'nan'
                if nan_emp_nos.any():
                    logger.warning(f"Warning: Found {nan_emp_nos.sum()} rows with 'nan' as employee number in HourClock sheet. These will be skipped.")
                    excel_data = excel_data[~nan_emp_nos]

            # Ensure 'SFNo' is treated as string and strip whitespace
            if 'SFNo' in excel_data.columns:
                excel_data['SFNo'] = excel_data['SFNo'].astype(str).str.strip()

            # If SFNo exists in existing_records, make sure it's a string for comparison
            if not existing_records.empty and 'SFNo' in existing_records.columns:
                existing_records['SFno'] = existing_records['SFno'].astype(str)

            # Check for duplicate SFNo in Excel data
            if 'SFNo' in excel_data.columns:
                duplicates = excel_data['SFNo'].duplicated()
                if duplicates.any():
                    duplicate_emp_nos = excel_data.loc[duplicates, 'SFNo'].tolist()
                    logger.warning(f"Warning: Duplicate employee numbers found in HourClock Excel sheet: {duplicate_emp_nos}")
                    logger.warning("Only the last occurrence of each duplicate will be processed.")
                    # Keep only the last occurrence of each duplicate
                    excel_data = excel_data.drop_duplicates(subset=['SFNo'], keep='last')

            # Prepare lists for operations
            records_to_add = []
            updates_to_perform = []

            logger.info(f"Processing {len(excel_data)} valid rows from HourClock Excel sheet")

            # Process each row from Excel
            for _, excel_row in excel_data.iterrows():
                emp_no = str(excel_row['SFNo'])
                sr_no = excel_row.get('No')

                # Prepare Grist fields for the HourClock table
                grist_hourclock_fields = {
                    'Month_Year': self.month_year,
                    'SFNo': emp_no,
                }

                # Add Sr_No if the column exists in Grist
                if 'Sr_No' in self.table_columns:
                    grist_hourclock_fields['Sr_No'] = sr_no if pd.notna(sr_no) else None

                # Find all P and OT columns in the Excel data with both hyphen and underscore formats
                p_cols = [col for col in excel_row.index if col.startswith('P-') or col.startswith('P_')]
                ot_cols = [col for col in excel_row.index if col.startswith('OT-') or col.startswith('OT_')]

                # Map P columns considering both formats (hyphen and underscore)
                for p_col in p_cols:
                    # Extract day number handling both P-1 and P_1 formats
                    if '-' in p_col:
                        day = p_col.split('-')[1]
                    else:
                        day = p_col.split('_')[1]

                    # Try both formats for Grist columns
                    p_col_grist_underscore = f'P_{day}'
                    p_col_grist_hyphen = f'P-{day}'

                    # Check which format exists in Grist, prioritize underscore format
                    if p_col_grist_underscore in self.table_columns:
                        p_col_grist = p_col_grist_underscore
                    elif p_col_grist_hyphen in self.table_columns:
                        p_col_grist = p_col_grist_hyphen
                    else:
                        # Neither format exists, log and skip
                        logger.debug(f"Neither P_{day} nor P-{day} found in Grist table, skipping")
                        continue

                    p_value = excel_row[p_col]

                    # Convert P value to integer (0 or 1), handle NaN/errors
                    if pd.notna(p_value):
                        try:
                            grist_hourclock_fields[p_col_grist] = int(p_value)
                        except (ValueError, TypeError):
                            logger.warning(f"Warning: Could not convert P value '{p_value}' to integer for EmpNo {emp_no}, Day {day}. Setting to None.")
                            grist_hourclock_fields[p_col_grist] = None
                    else:
                        grist_hourclock_fields[p_col_grist] = None

                # Map OT columns considering both formats (hyphen and underscore)
                for ot_col in ot_cols:
                    # Extract day number handling both OT-1 and OT_1 formats
                    if '-' in ot_col:
                        day = ot_col.split('-')[1]
                    else:
                        day = ot_col.split('_')[1]

                    # Try both formats for Grist columns
                    ot_col_grist_underscore = f'OT_{day}'
                    ot_col_grist_hyphen = f'OT-{day}'

                    # Check which format exists in Grist, prioritize underscore format
                    if ot_col_grist_underscore in self.table_columns:
                        ot_col_grist = ot_col_grist_underscore
                    elif ot_col_grist_hyphen in self.table_columns:
                        ot_col_grist = ot_col_grist_hyphen
                    else:
                        # Check for case-insensitive match as fallback
                        lowercase_columns = [col.lower() for col in self.table_columns]
                        if ot_col_grist_underscore.lower() in lowercase_columns:
                            # Find the correct case
                            correct_case = next(col for col in self.table_columns if col.lower() == ot_col_grist_underscore.lower())
                            ot_col_grist = correct_case
                            logger.warning(f"Case mismatch: Using '{correct_case}' instead of '{ot_col_grist_underscore}'")
                        else:
                            # Neither format exists, log and skip
                            logger.debug(f"Neither OT_{day} nor OT-{day} found in Grist table columns: {self.table_columns}, skipping")
                            continue

                    ot_value = excel_row[ot_col]

                    # Convert OT value to float, handle NaN/errors
                    if pd.notna(ot_value):
                        try:
                            grist_hourclock_fields[ot_col_grist] = float(ot_value)
                        except (ValueError, TypeError):
                            logger.warning(f"Warning: Could not convert OT value '{ot_value}' to float for EmpNo {emp_no}, Day {day}. Setting to None.")
                            grist_hourclock_fields[ot_col_grist] = None
                    else:
                        grist_hourclock_fields[ot_col_grist] = None

                # Find if record for this employee and month/year exists in Grist
                matched_records = pd.DataFrame()
                if not existing_records.empty and 'SFNo' in existing_records.columns and 'Month_Year' in existing_records.columns:
                    matched_records = existing_records[
                        (existing_records['SFNo'] == emp_no) &
                        (existing_records['Month_Year'] == self.month_year)
                    ]

                if matched_records.empty:
                    # Scenario: New record for this employee and month/year
                    logger.info(f"Attempting to add new HourClock record for employee {emp_no} ({self.month_year}).")
                    records_to_add.append({'fields': grist_hourclock_fields})

                else:
                    # Scenario: Existing record for this employee and month/year
                    record_id = matched_records['id'].iloc[0]
                    current_grist_record = matched_records.iloc[0]

                    # Compare fields to see if an update is needed
                    needs_update = False
                    update_payload_fields = {}

                    # Compare SrNo field if it exists in both places
                    if 'SrNo' in current_grist_record and 'SrNo' in grist_hourclock_fields:
                        grist_value = current_grist_record['SrNo']
                        excel_value = grist_hourclock_fields['SrNo']

                        # Handle None/NaN comparison
                        if not pd.isna(excel_value) or not pd.isna(grist_value):
                            excel_str = str(excel_value) if pd.notna(excel_value) else 'None'
                            grist_str = str(grist_value) if pd.notna(grist_value) else 'None'

                            if excel_str != grist_str:
                                needs_update = True
                                update_payload_fields['SrNo'] = excel_value
                                logger.debug(f"Update needed for {emp_no} ({self.month_year}): SrNo differs (Excel: '{excel_str}', Grist: '{grist_str}')")

                    # Compare all fields in grist_hourclock_fields (P and OT with correct format)
                    for field_name, new_value in grist_hourclock_fields.items():
                        # Skip Month_Year and SFno since they're our match criteria and Sr_No which was already checked
                        if field_name in ['Month_Year', 'SFno', 'Sr_No']:
                            continue

                        if field_name in current_grist_record:
                            current_value = current_grist_record[field_name]

                            # Compare values (handle None/NaN and type comparison)
                            if (pd.isna(current_value) and pd.notna(new_value)) or \
                               (pd.notna(current_value) and pd.isna(new_value)) or \
                               (pd.notna(current_value) and pd.notna(new_value) and current_value != new_value):
                                needs_update = True
                                update_payload_fields[field_name] = new_value
                                logger.debug(f"Update needed for {emp_no} ({self.month_year}): {field_name} differs (Excel: {new_value}, Grist: {current_value})")

                    if needs_update:
                        updates_to_perform.append({
                            'id': int(record_id),
                            'fields': update_payload_fields
                        })
                        logger.info(f"HourClock record for employee {emp_no} ({self.month_year}) queued for update.")
                    else:
                        logger.info(f"HourClock record for employee {emp_no} ({self.month_year}): No update needed.")

            # Perform bulk add operations
            if records_to_add:
                add_url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"
                logger.info(f"Adding {len(records_to_add)} new HourClock records to {self.hourclock_table_name}.")

                # Print the month_year value before adding records
                # print(f"Month_Year value being inserted: {self.month_year}")

                # Print the records to be added before sending the request
                # print("\n--- Records to be added to HC_Detail ---")
                # print(json.dumps(records_to_add, indent=2))
                # print("---------------------------------------")

                if records_to_add:
                    logger.debug(f"Sample add record for HourClock table: {records_to_add[0]}")

                try:
                    add_response = requests.post(
                        add_url,
                        headers=self.headers,
                        json={'records': records_to_add}
                    )
                    add_response.raise_for_status()
                    logger.info(f"Successfully added {len(records_to_add)} new HourClock records.")
                    self._new_records_count += len(records_to_add)
                except requests.RequestException as e:
                    logger.error(f"Error adding new HourClock records: {e}")
                    if hasattr(e, 'response') and e.response is not None:
                        logger.error(f"Response: {e.response.text}")
                        # Get more details about which columns might be invalid
                        response_text = e.response.text
                        try:
                            error_data = json.loads(response_text)
                            error_message = error_data.get('error', '')
                            if "Invalid column" in error_message:
                                invalid_col = error_message.split('"')[1]
                                logger.error(f"The column '{invalid_col}' doesn't exist in the Grist table.")
                                logger.error(f"Available columns in Grist: {', '.join(self.table_columns)}")

                                # For debugging: print a sample record to see what we're trying to send
                                if records_to_add:
                                    sample_record = records_to_add[0]['fields']
                                    logger.error(f"Sample record fields: {list(sample_record.keys())}")
                        except:
                            pass

            # Perform bulk update operations
            if updates_to_perform:
                update_url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"
                logger.info(f"Updating {len(updates_to_perform)} existing HourClock records in {self.hourclock_table_name}.")
                if updates_to_perform:
                    logger.debug(f"Sample update record for HourClock table: {updates_to_perform[0]}")

                try:
                    update_response = requests.patch(
                        update_url,
                        headers=self.headers,
                        json={'records': updates_to_perform}
                    )
                    update_response.raise_for_status()
                    logger.info(f"Successfully updated {len(updates_to_perform)} existing HourClock records.")
                    self._updated_records_count += len(updates_to_perform)
                except requests.RequestException as e:
                    logger.error(f"Error updating existing HourClock records: {e}")
                    if hasattr(e, 'response') and e.response is not None:
                        logger.error(f"Response: {e.response.text}")

        except requests.RequestException as e:
            logger.error(f"A Grist API request failed during the HourClock process: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
        except Exception as e:
            import traceback
            logger.error(f"Unexpected error during HourClock update: {e}")
            logger.error(traceback.format_exc())

        # Print summary of actions
        logger.info("\n--- HourClock Update Summary ---")
        logger.info(f"New HourClock records added: {self._new_records_count}")
        logger.info(f"Existing HourClock records updated: {self._updated_records_count}")
        logger.info("------------------------------\n")
