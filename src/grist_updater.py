import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

# Configure logging
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO').upper()
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

class GristUpdater:
    def __init__(self,
                 api_key=None,
                 doc_id=None,
                 main_table_name=None,
                 rate_log_table_name=None,
                 base_url=None):
        """
        Initialize Grist Updater

        :param api_key: Grist API key
        :param doc_id: Grist document ID
        :param main_table_name: Name of the main employee table to update
        :param rate_log_table_name: Name of the table for logging salary rate changes
        :param base_url: Optional base URL for custom Grist installations
        """
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.doc_id = doc_id or os.getenv('GRIST_DOC_ID')
        self.main_table_name = main_table_name or os.getenv('GRIST_TABLE_NAME')
        self.rate_log_table_name = rate_log_table_name or os.getenv('GRIST_RATE_LOG_TABLE', 'Emp_RateLog')

        # Support for custom Grist installations
        grist_url = base_url or os.getenv('GRIST_BASE_URL', 'https://docs.getgrist.com')
        self.base_url = f"{grist_url}/api/docs/{self.doc_id}"

        logging.info(f"Using Grist API at: {self.base_url}")

        # Column mappings from Excel to Grist
        self.excel_to_grist_mapping = {
            'Emp No.': 'SFNo',
            # 'Emp Name' is handled separately for splitting
            'Designation': 'Designation', # Assuming Excel 'Designation' maps to Grist 'Designation'
            'Emp Type : Temp / Perm': 'Perm_Temp',
            'Salary Calculation on Fixed / Hourly': 'Fixed_Hourly',
            'Date of Joining': 'DOJ'
        }

        # Initialize counters for summary
        self._new_emp_count = 0
        self._updated_emp_count = 0
        self._rate_log_count = 0

        # Removed 'Salary Rate (Per Day)': 'Salary_PerDay' from mapping as it's a formula field
        # 'Emp Name' from Excel is processed into FirstName, MiddleName, LastName in Grist.

        # Headers for API requests
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _split_name(self, full_name_str):
        """
        Splits a full name string into FirstName, MiddleName, and LastName.
        Handles cases like "Md ghulam Abdul sattar Mustafa"
        where FirstName = "Md Ghulam", MiddleName = "Abdul Sattar", LastName = "Mustafa".
        Also handles names with fewer parts.
        """
        if not full_name_str or pd.isna(full_name_str):
            return None, None, None

        parts = str(full_name_str).strip().split()

        if not parts:
            return None, None, None

        if len(parts) == 1:
            # Only one part, assume it's FirstName
            return parts[0], None, None
        elif len(parts) == 2:
            # Two parts, assume FirstName and LastName
            return parts[0], None, parts[1]
        elif len(parts) == 3:
            # Three parts, standard FirstName, MiddleName, LastName
            return parts[0], parts[1], parts[2]
        else: # More than 3 parts, apply the specific logic
            # Example: "Md ghulam Abdul sattar Mustafa" (5 parts)
            # LastName is the last part
            last_name = parts[-1]

            # FirstName is the first two parts if "Md" or "Mohd" is the first part
            # and there are at least 4 parts to allow for a middle name and last name.
            if (parts[0].lower() in ['md', 'mohd', 'md.', 'mohd.']) and len(parts) >= 4:
                first_name = " ".join(parts[0:2])
                # MiddleName is everything between FirstName and LastName
                middle_name_parts = parts[2:-1]
                middle_name = " ".join(middle_name_parts) if middle_name_parts else None
            else:
                # Default: first part is FirstName
                first_name = parts[0]
                # MiddleName is everything between FirstName and LastName
                middle_name_parts = parts[1:-1]
                middle_name = " ".join(middle_name_parts) if middle_name_parts else None

            return first_name, middle_name, last_name

    def get_existing_records(self, table_name=None):
        """
        Fetch existing records from Grist table

        :param table_name: Optional table name override
        :return: DataFrame of existing records
        """
        try:
            # Use provided table name or default to main table
            table = table_name or self.main_table_name

            # Construct the API endpoint for fetching records
            url = f"{self.base_url}/tables/{table}/records"

            logging.info(f"Fetching records from: {url}")

            # Make the GET request
            response = requests.get(url, headers=self.headers)

            # Check if request was successful
            response.raise_for_status()

            # Extract records
            records_data = response.json().get('records', [])

            logging.info(f"Fetched {len(records_data)} records from {table}")

            # If no records, return empty DataFrame but try to get columns
            if not records_data:
                # Try to get table columns by fetching table schema
                try:
                    schema_url = f"{self.base_url}/tables/{table}"
                    schema_response = requests.get(schema_url, headers=self.headers)
                    schema_response.raise_for_status()
                    fields = schema_response.json().get('fields', {})
                    columns = list(fields.keys()) + ['id']  # Add id column
                    return pd.DataFrame(columns=columns)
                except:
                    return pd.DataFrame()

            # Convert to DataFrame
            records_df = pd.DataFrame([
                {**record['fields'], 'id': record['id']}
                for record in records_data
            ])

            # --- Convert Unix timestamps to datetime strings for known date fields ---
            date_fields = ['DOJ', 'Created_at', 'Last_updated_at']
            for field in date_fields:
                if field in records_df.columns:
                    # Convert Unix timestamps (float) to datetime objects, coercing errors
                    # Then format to string, handling NaT by filling with empty string
                    records_df.loc[:, field] = pd.to_datetime(records_df[field], unit='s', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
            # --- End of date conversion ---

            return records_df

        except requests.RequestException as e:
            logging.error(f"Error fetching existing records from {table}: {e}")
            if hasattr(e.response, 'text'):
                logging.error(f"Response: {e.response.text}")
            return pd.DataFrame()

    def add_rate_log_entry(self, emp_no, new_rate, is_initial=False):
        """
        Add entry to the rate log table when salary rate changes or for new employees

        :param emp_no: Employee number
        :param new_rate: New salary rate per day
        :param is_initial: Whether this is the initial rate entry for a new employee
        """
        try:
            # Get the structure of the rate log table first to understand its columns
            try:
                rate_log_records = self.get_existing_records(self.rate_log_table_name)
                logging.debug(f"Rate log table columns: {rate_log_records.columns.tolist() if not rate_log_records.empty else 'No records found'}")
            except Exception as e:
                logging.warning(f"Warning: Could not fetch rate log table structure: {e}")

            # Skip if rate is NaN
            if pd.isna(new_rate):
                logging.warning(f"Skipping rate log entry for employee {emp_no} due to missing/invalid rate")
                return

            # Basic record with required fields
            fields = {
                'SFNo': str(emp_no),  # Ensure string type
                'NewPerDayRate': float(new_rate),
                'Remarks': 'Initial Rate' if is_initial else 'Rate Change - AutoCode'
            }

            # Only add the LogDate field if we know it's needed/supported
            # Uncomment this if the table has a LogDate column
            # fields['LogDate'] = datetime.now().strftime('%Y-%m-%d')

            add_record = {'fields': fields}

            # Add to rate log table
            add_url = f"{self.base_url}/tables/{self.rate_log_table_name}/records"

            # Print the payload for debugging
            logging.debug(f"Rate log payload: {add_record}")

            add_response = requests.post(
                add_url,
                headers=self.headers,
                json={'records': [add_record]}
            )

            # If request fails, print more detailed error info
            if not add_response.ok:
                logging.error(f"Rate log add failed with status {add_response.status_code}")
                logging.error(f"Response: {add_response.text}")

            add_response.raise_for_status()
            logging.info(f"Added rate log entry for employee {emp_no} {'(initial rate)' if is_initial else '(rate change)'}")
            self._rate_log_count += 1 # Increment rate log counter here

        except requests.RequestException as e:
            logging.error(f"Error adding rate log entry: {e}")
            logging.error("Please check that:")
            logging.error("1. The Emp_RateLog table exists in your Grist document")
            logging.error("2. It has the columns: SFNo, NewPerDayRate, and Remarks")
            logging.error("3. The API key has write permissions to this table")
        except ValueError as e:
            logging.error(f"Error processing rate value for employee {emp_no}: {e}")
            logging.warning("Skipping rate log entry for this employee")

    def compare_and_update(self, excel_data):
        """
        Compare Excel data with existing Grist records and update according to business rules

        :param excel_data: DataFrame with Excel data
        """
        try:
            # Fetch existing employee records
            existing_records = self.get_existing_records()

            if existing_records.empty and not excel_data.empty:
                logging.info("No existing records found in Grist table. All records will be added as new.")

            # Make a copy of the data to avoid modifying the original
            excel_data = excel_data.copy()

            # Remove rows with NaN or null in the 'Emp No.' column
            if 'Emp No.' in excel_data.columns:
                null_emp_nos = excel_data['Emp No.'].isna()
                if null_emp_nos.any():
                    logging.warning(f"Warning: Found {null_emp_nos.sum()} rows with empty employee numbers. These will be skipped.")
                    excel_data = excel_data.dropna(subset=['Emp No.'])

                # Also remove rows where 'Emp No.' is 'nan' as a string
                nan_emp_nos = excel_data['Emp No.'] == 'nan'
                if nan_emp_nos.any():
                    logging.warning(f"Warning: Found {nan_emp_nos.sum()} rows with 'nan' as employee number. These will be skipped.")
                    excel_data = excel_data[~nan_emp_nos]

                # Ensure 'Emp No.' is treated as string
                excel_data['Emp No.'] = excel_data['Emp No.'].astype(str)

            # If SFNo exists in existing_records, make sure it's a string for comparison
            if not existing_records.empty and 'SFNo' in existing_records.columns:
                existing_records['SFNo'] = existing_records['SFNo'].astype(str)

            # Check for duplicate SFNo in Excel data
            if 'Emp No.' in excel_data.columns:
                duplicates = excel_data['Emp No.'].duplicated()
                if duplicates.any():
                    duplicate_emp_nos = excel_data.loc[duplicates, 'Emp No.'].tolist()
                    logging.warning(f"Warning: Duplicate employee numbers found in Excel: {duplicate_emp_nos}")
                    logging.warning("Only the last occurrence of each duplicate will be processed.")
                    # Keep only the last occurrence of each duplicate
                    excel_data = excel_data.drop_duplicates(subset=['Emp No.'], keep='last')

            # Prepare lists for operations
            updates_to_main_table = []
            rate_log_entries_to_process = [] # Stores dicts: {'emp_no': ..., 'new_rate': ..., 'is_initial': ...}

            # Debug info
            logging.info(f"Processing {len(excel_data)} rows from Excel")

            # First, check if the rate log table exists and is accessible
            # This check is informational; actual rate log operations depend on main table success for new emps
            try:
                rate_log_schema_check = self.get_existing_records(self.rate_log_table_name) # Use a different var name
                logging.debug(f"Rate log table is accessible with {len(rate_log_schema_check)} existing entries (schema check)")
            except Exception as e:
                logging.warning(f"Warning: Could not access rate log table for initial check: {e}")
                # Continue, as individual operations will handle errors.

            # Process each row from Excel
            for _, excel_row in excel_data.iterrows():
                emp_no = str(excel_row['Emp No.'])
                new_excel_rate = excel_row.get('Salary Rate (Per Day)') # Use .get for safety if column is missing

                # Map Excel data to Grist format for the main table
                grist_main_fields = {}
                for excel_col, grist_col in self.excel_to_grist_mapping.items():
                    if excel_col in excel_row.index: # Check if column exists in the row
                        # Handle date fields specially to ensure proper formatting
                        if excel_col == 'Date of Joining' and pd.notna(excel_row[excel_col]):
                            if isinstance(excel_row[excel_col], pd.Timestamp):
                                grist_main_fields[grist_col] = excel_row[excel_col].strftime('%Y-%m-%d')
                            else:
                                grist_main_fields[grist_col] = excel_row[excel_col]
                        else:
                            # Handle NaN values by converting to None (which becomes null in JSON)
                            value = excel_row[excel_col]
                            if pd.isna(value):
                                grist_main_fields[grist_col] = None
                            else:
                                grist_main_fields[grist_col] = value

                # Process 'Name' from Excel
                excel_full_name = excel_row.get('Name') # Changed from 'Emp Name' to 'Name'

                # Initialize name fields to None
                grist_main_fields['FirstName'] = None
                grist_main_fields['MiddleName'] = None
                grist_main_fields['LastName'] = None

                if pd.notna(excel_full_name):
                    first_name, middle_name, last_name = self._split_name(excel_full_name)
                    grist_main_fields['FirstName'] = first_name # Will be None if not found by _split_name
                    grist_main_fields['MiddleName'] = middle_name # Will be None if not found by _split_name
                    grist_main_fields['LastName'] = last_name  # Will be None if not found by _split_name
                else:
                    logging.warning(f"No 'Name' found for Emp No: {emp_no}. Name fields will be null.")  # Changed message

                # Find if employee exists in Grist main table
                matched_records = pd.DataFrame()
                if not existing_records.empty and 'SFNo' in existing_records.columns:
                    matched_records = existing_records[existing_records['SFNo'] == emp_no]

                if not existing_records.empty and 'SFNo' in existing_records.columns:
                    matched_records = existing_records[existing_records['SFNo'] == emp_no]

                if matched_records.empty:
                    # Scenario: New employee
                    logging.info(f"Attempting to add new employee {emp_no} to main table.")
                    add_payload = {'fields': grist_main_fields}
                    add_url = f"{self.base_url}/tables/{self.main_table_name}/records"

                    try:
                        response = requests.post(add_url, headers=self.headers, json={'records': [add_payload]})
                        response.raise_for_status() # Will raise HTTPError for bad responses (4xx or 5xx)

                        logging.info(f"Successfully added new employee {emp_no} to main table.")
                        self._new_emp_count += 1
                        if pd.notna(new_excel_rate):
                            rate_log_entries_to_process.append({
                                'emp_no': emp_no,
                                'new_rate': new_excel_rate,
                                'is_initial': True
                            })
                        else:
                            logging.warning(f"New employee {emp_no} has no salary rate in Excel; skipping initial rate log entry.")

                    except requests.RequestException as e:
                        logging.error(f"Failed to add new employee {emp_no} to main table. Error: {e}")
                        if hasattr(e.response, 'text'):
                            logging.error(f"Response: {e.response.text}")
                        logging.warning(f"Skipping rate log entry for new employee {emp_no} due to main table add failure.")
                        # Do not add to rate_log_entries_to_process if main table add fails

                else:
                    # Scenario: Existing employee
                    record_id = matched_records['id'].iloc[0]
                    current_grist_rate = None

                    if 'Salary_PerDay' in matched_records.columns:
                        current_grist_rate = matched_records['Salary_PerDay'].iloc[0]
                    else:
                        logging.warning(f"Warning: 'Salary_PerDay' column not found in existing Grist records for employee {emp_no}.")

                    # Prepare for rate comparison
                    grist_rate_float = None
                    excel_rate_float = None
                    rates_are_different = False

                    if pd.notna(current_grist_rate):
                        try:
                            grist_rate_float = float(current_grist_rate)
                        except (ValueError, TypeError):
                            logging.warning(f"Warning: Could not convert current Grist salary rate '{current_grist_rate}' to float for employee {emp_no}.")

                    if pd.notna(new_excel_rate):
                        try:
                            excel_rate_float = float(new_excel_rate)
                        except (ValueError, TypeError):
                            logging.warning(f"Warning: Could not convert new Excel salary rate '{new_excel_rate}' to float for employee {emp_no}.")

                    # Compare rates if both are valid numbers
                    if grist_rate_float is not None and excel_rate_float is not None:
                        if grist_rate_float != excel_rate_float:
                            rates_are_different = True
                    elif grist_rate_float is None and excel_rate_float is not None:
                        # Grist rate is null/invalid, Excel rate is valid -> consider it a change to log the new rate
                        rates_are_different = True
                        logging.info(f"Employee {emp_no}: Current Grist rate is missing/invalid, new Excel rate is {excel_rate_float}. Logging change.")
                    elif grist_rate_float is not None and excel_rate_float is None:
                        # Grist rate is valid, Excel rate is null/invalid -> typically means no change or data issue in Excel
                        # Not logging this as a "rate change" to null unless explicitly required.
                        logging.info(
                            f"Employee {emp_no}: Current Grist rate is {grist_rate_float}, new Excel rate is missing/invalid. Not logging as rate change.")
                    # If both are None/invalid, they are not "different" in a way that requires logging.

                    logging.debug(f"Employee {emp_no}: Grist rate (float) = {grist_rate_float}, Excel rate (float) = {excel_rate_float}, Different = {rates_are_different}")

                    if rates_are_different and pd.notna(new_excel_rate):  # Ensure new_excel_rate is valid before logging
                        rate_log_entries_to_process.append({
                            'emp_no': emp_no,
                            'new_rate': new_excel_rate,  # Log the original Excel value
                            'is_initial': False
                        })
                        logging.info(f"Rate change detected for employee {emp_no}. Queued for rate log.")

                    # --- Start of comparison logic for updates ---
                    needs_update = False
                    current_grist_record = matched_records.iloc[0] # Get the single row for this employee

                    # Fields to compare for updates (excluding SFNo, Designation, and Name-related)
                    fields_to_compare = {
                        'Emp Type : Temp / Perm': 'Perm_Temp',
                        'Salary Calculation on Fixed / Hourly': 'Fixed_Hourly',
                        'Date of Joining': 'DOJ'
                    }

                    for excel_col, grist_col in fields_to_compare.items():
                        if excel_col in excel_row.index and grist_col in current_grist_record:
                            excel_value = excel_row[excel_col]
                            grist_value = current_grist_record[grist_col]

                            # Handle date comparison specifically
                            if excel_col == 'Date of Joining':
                                # Convert both to datetime objects for comparison, handling potential errors
                                excel_date = None
                                grist_date = None
                                try:
                                    if pd.notna(excel_value):
                                        if isinstance(excel_value, pd.Timestamp):
                                            excel_date = excel_value.normalize() # Use normalize to remove time part
                                        else:
                                            excel_date = pd.to_datetime(excel_value).normalize()
                                except:
                                    pass # Ignore conversion errors

                                try:
                                    if pd.notna(grist_value):
                                        grist_date = pd.to_datetime(grist_value).normalize()
                                except:
                                    pass # Ignore conversion errors

                                if excel_date != grist_date:
                                    needs_update = True
                                    logging.debug(f"DEBUG: Update needed for {emp_no}: {grist_col} differs (Excel: {excel_date}, Grist: {grist_date})")
                                    # No break here, continue checking other fields for more detailed logging
                            else:
                                # Compare other field types, handling None/NaN
                                # Use pandas.isna for robust NaN/None check
                                if not pd.isna(excel_value) or not pd.isna(grist_value):
                                    # If either is not NaN, compare. If one is NaN and other is not, they are different.
                                    # If both are not NaN, compare values.
                                    # Convert to string for robust comparison, handling None as 'None' string
                                    excel_str = str(excel_value) if pd.notna(excel_value) else 'None'
                                    grist_str = str(grist_value) if pd.notna(grist_value) else 'None'

                                    if excel_str != grist_str:
                                        needs_update = True
                                        logging.debug(f"DEBUG: Update needed for {emp_no}: {grist_col} differs (Excel: '{excel_str}', Grist: '{grist_str}')")
                                        # No break here, continue checking other fields for more detailed logging


                    # --- End of comparison logic for updates ---

                    if needs_update:
                        # Create update payload excluding name fields for existing employees
                        update_payload_fields = grist_main_fields.copy()
                        update_payload_fields.pop('FirstName', None)
                        update_payload_fields.pop('MiddleName', None)
                        update_payload_fields.pop('LastName', None)

                        # Remove Designation field for existing employees to prevent updates
                        update_payload_fields.pop('Designation', None)

                        # Add to main table update list (updates other fields, Salary_PerDay is formula)
                        updates_to_main_table.append({
                            'id': int(record_id),
                            'fields': update_payload_fields # Use the new dictionary
                        })
                        logging.info(f"Employee {emp_no} queued for update in main table.")
                    else:
                        logging.info(f"Employee {emp_no}: No update needed for main table fields.")


            # Perform bulk updates to the main table if any
            if updates_to_main_table:
                update_url = f"{self.base_url}/tables/{self.main_table_name}/records"
                logging.info(f"Updating {len(updates_to_main_table)} existing employee records in main table.")
                if updates_to_main_table:  # Debug sample
                    logging.debug(f"Sample update record for main table: {updates_to_main_table[0]}")

                try:
                    update_response = requests.patch(
                        update_url,
                        headers=self.headers,
                        json={'records': updates_to_main_table}
                    )
                    update_response.raise_for_status()
                    logging.info(f"Successfully updated {len(updates_to_main_table)} existing employee records in main table.")
                    self._updated_emp_count += len(updates_to_main_table) # Increment updated count
                except requests.RequestException as e:
                    logging.error(f"Error updating records in main table: {e}")
                    if hasattr(e.response, 'text'):
                        logging.error(f"Response: {e.response.text}")

            # Process all queued rate log entries
            if rate_log_entries_to_process:
                logging.info(f"Processing {len(rate_log_entries_to_process)} rate log entries.")
                for entry_data in rate_log_entries_to_process:
                    # self.add_rate_log_entry handles its own try-except for the API call
                    self.add_rate_log_entry(
                        entry_data['emp_no'],
                        entry_data['new_rate'],
                        entry_data['is_initial']
                    )
            else:
                logging.info("No rate log entries to process.")

        except requests.RequestException as e:  # Catching general request exceptions earlier in the new logic
            logging.error(f"A Grist API request failed during the process: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response: {e.response.text}")
        except Exception as e:
            import traceback
            logging.error(f"Unexpected error: {e}")
            logging.error(traceback.format_exc())

        # Print summary of actions
        logging.info("\n--- Update Summary ---")
        logging.info(f"New employees added to {self.main_table_name}: {self._new_emp_count}")
        logging.info(f"Existing employees updated in {self.main_table_name}: {self._updated_emp_count}")
        logging.info(f"Rate log entries added to {self.rate_log_table_name}: {self._rate_log_count}")
        logging.info("----------------------\n")
