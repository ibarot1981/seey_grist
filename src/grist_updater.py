import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

class GristUpdater:
    def __init__(self,
                 api_key=None,
                 doc_id=None,
                 main_table_name=None,
                 rate_log_table_name=None,
                 base_url=None,
                 month_year=None):
        """
        Initialize Grist Updater

        :param api_key: Grist API key
        :param doc_id: Grist document ID
        :param main_table_name: Name of the main employee table to update
        :param rate_log_table_name: Name of the table for logging salary rate changes
        :param base_url: Optional base URL for custom Grist installations
        :param month_year: Month and year in MMM-YY format from the Excel file
        """
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.doc_id = doc_id or os.getenv('GRIST_DOC_ID')
        self.main_table_name = main_table_name or os.getenv('GRIST_TABLE_NAME')
        self.rate_log_table_name = rate_log_table_name or os.getenv('GRIST_RATE_LOG_TABLE', 'Emp_RateLog')

        # Support for custom Grist installations
        grist_url = base_url or os.getenv('GRIST_BASE_URL', 'https://docs.getgrist.com')
        self.base_url = f"{grist_url}/api/docs/{self.doc_id}"

        self.month_year = month_year

        # Read MarkAsLeft setting from environment
        self.mark_as_left = os.getenv('MarkAsLeft', 'No').upper()
        logger.info(f"MarkAsLeft setting: {self.mark_as_left}")

        logger.info(f"Using Grist API at: {self.base_url}")

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
        self._marked_as_left_count = 0

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
            first_name = parts[0]
            middle_name = None
            last_name = None
            logger.debug(f"1 part - Before sentence case: FirstName='{first_name}', MiddleName='{middle_name}', LastName='{last_name}'")
        elif len(parts) == 2:
            # Two parts, assume FirstName and LastName
            first_name = parts[0]
            middle_name = None
            last_name = parts[1]
            logger.debug(f"2 parts - Before sentence case: FirstName='{first_name}', MiddleName='{middle_name}', LastName='{last_name}'")
        elif len(parts) == 3:
            # Three parts, standard FirstName, MiddleName, LastName
            first_name = parts[0]
            middle_name = parts[1]
            last_name = parts[2]
            logger.debug(f"3 parts - Before sentence case: FirstName='{first_name}', MiddleName='{middle_name}', LastName='{last_name}'")
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
            logger.debug(f">3 parts - Before sentence case: FirstName='{first_name}', MiddleName='{middle_name}', LastName='{last_name}'")


        # Apply Sentence case formatting
        first_name = self._to_sentence_case(first_name) if first_name else None
        middle_name = self._to_sentence_case(middle_name) if middle_name else None
        last_name = self._to_sentence_case(last_name) if last_name else None

        logger.debug(f"After sentence case: FirstName='{first_name}', MiddleName='{middle_name}', LastName='{last_name}'")

        return first_name, middle_name, last_name

    def _to_sentence_case(self, name_part):
        """
        Converts a string to Sentence case (first letter of each word capitalized).
        """
        if not name_part:
            return None
        return " ".join(word.capitalize() for word in str(name_part).split())

    def _generate_record_history_entry(self, action, field_name=None, new_value=None):
        """
        Generates a formatted RecordHistory entry.
        """
        current_date_str = datetime.now().strftime('%d-%m-%Y')
        entry = f"{current_date_str} {self.month_year}: "
        if action == "Inserted New Record":
            entry += action
        elif action == "Updated":
            entry += f"Updated {field_name} to {new_value}"
        return entry

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

            logger.info(f"Fetching records from: {url}")

            # Make the GET request
            response = requests.get(url, headers=self.headers)

            # Check if request was successful
            response.raise_for_status()

            # Extract records
            records_data = response.json().get('records', [])

            logger.info(f"Fetched {len(records_data)} records from {table}")

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
                    # Convert from Unix timestamp, handling potential errors
                    # First convert to datetime, then to string, handling NaT properly
                    datetime_series = pd.to_datetime(records_df[field], unit='s', errors='coerce')
                    
                    # Convert to string format, replacing NaT with empty string
                    records_df[field] = datetime_series.dt.strftime('%Y-%m-%d %H:%M:%S')
                    records_df[field] = records_df[field].fillna('')
            # --- End of date conversion ---

            return records_df

        except requests.RequestException as e:
            logger.error(f"Error fetching existing records from {table}: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            return pd.DataFrame()

    def _prepare_rate_log_entry_payload(self, emp_no, new_rate, is_initial=False):
        """
        Prepares the payload for a single rate log entry.
        This method does NOT make an API call.

        :param emp_no: Employee number
        :param new_rate: New salary rate per day
        :param is_initial: Whether this is the initial rate entry for a new employee
        :return: Dictionary representing the 'fields' for a rate log record, or None if skipped
        """
        if pd.isna(new_rate):
            logger.warning(f"Skipping rate log entry preparation for employee {emp_no} due to missing/invalid rate")
            return None

        fields = {
            'SFNo': str(emp_no),  # Ensure string type
            'NewPerDayRate': float(new_rate),
            'Remarks': 'Initial Rate' if is_initial else 'Rate Change - AutoCode'
        }

        # As confirmed by the user, RecordHistory column is always present.
        fields['RecordHistory'] = self.month_year

        # Only add the LogDate field if we know it's needed/supported
        # fields['LogDate'] = datetime.now().strftime('%Y-%m-%d')

        return {'fields': fields}

    def bulk_add_rate_log_entries(self, records_payload_list):
        """
        Performs a bulk insert of rate log entries to the Grist table.

        :param records_payload_list: A list of dictionaries, where each dictionary
                                     represents a single rate log record's 'fields' payload.
        """
        if not records_payload_list:
            logger.info("No rate log entries to bulk add.")
            return

        add_url = f"{self.base_url}/tables/{self.rate_log_table_name}/records"
        payload = {'records': records_payload_list}

        logger.info(f"Attempting to bulk add {len(records_payload_list)} rate log entries.")
        logger.debug(f"Sample rate log bulk payload: {records_payload_list[0]}")

        try:
            add_response = requests.post(
                add_url,
                headers=self.headers,
                json=payload
            )
            add_response.raise_for_status()
            logger.info(f"Successfully bulk added {len(records_payload_list)} rate log entries.")
            self._rate_log_count += len(records_payload_list)
        except requests.RequestException as e:
            logger.error(f"Error bulk adding rate log entries: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            logger.error("Please check that:")
            logger.error("1. The Emp_RateLog table exists in your Grist document")
            logger.error("2. It has the columns: SFNo, NewPerDayRate, Remarks, and RecordHistory")
            logger.error("3. The API key has write permissions to this table")
        except Exception as e:
            logger.error(f"Unexpected error during bulk rate log add: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def compare_and_update(self, excel_data):
        """
        Compare Excel data with existing Grist records and update according to business rules

        :param excel_data: DataFrame with Excel data
        """
        try:
            # Fetch existing employee records
            existing_records = self.get_existing_records()

            if existing_records.empty and not excel_data.empty:
                logger.info("No existing records found in Grist table. All records will be added as new.")

            # Make a copy of the data to avoid modifying the original
            excel_data = excel_data.copy()

            # Remove rows with NaN or null in the 'Emp No.' column
            if 'Emp No.' in excel_data.columns:
                null_emp_nos = excel_data['Emp No.'].isna()
                if null_emp_nos.any():
                    logger.warning(f"Warning: Found {null_emp_nos.sum()} rows with empty employee numbers. These will be skipped.")
                    excel_data = excel_data.dropna(subset=['Emp No.'])

                # Also remove rows where 'Emp No.' is 'nan' as a string
                nan_emp_nos = excel_data['Emp No.'] == 'nan'
                if nan_emp_nos.any():
                    logger.warning(f"Warning: Found {nan_emp_nos.sum()} rows with 'nan' as employee number. These will be skipped.")
                    excel_data = excel_data[~nan_emp_nos]

                # Ensure 'Emp No.' is treated as string
                excel_data['Emp No.'] = excel_data['Emp No.'].astype(str)

            # If SFno exists in existing_records, make sure it's a string for comparison
            if not existing_records.empty and 'SFNo' in existing_records.columns:
                existing_records['SFNo'] = existing_records['SFNo'].astype(str)

            # Check for duplicate SFNo in Excel data
            if 'Emp No.' in excel_data.columns:
                duplicates = excel_data['Emp No.'].duplicated()
                if duplicates.any():
                    duplicate_emp_nos = excel_data.loc[duplicates, 'Emp No.'].tolist()
                    logger.warning(f"Warning: Duplicate employee numbers found in Excel: {duplicate_emp_nos}")
                    logger.warning("Only the last occurrence of each duplicate will be processed.")
                    # Keep only the last occurrence of each duplicate
                    excel_data = excel_data.drop_duplicates(subset=['Emp No.'], keep='last')

            # Prepare lists for operations
            updates_to_main_table = []
            rate_log_entries_to_process = [] # Stores dicts: {'emp_no': ..., 'new_rate': ..., 'is_initial': ...}

            # Debug info
            logger.info(f"Processing {len(excel_data)} rows from Excel")

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
                    logger.warning(f"No 'Name' found for Emp No: {emp_no}. Name fields will be null.")  # Changed message

                # Find if employee exists in Grist main table
                matched_records = pd.DataFrame()
                if not existing_records.empty and 'SFNo' in existing_records.columns:
                    matched_records = existing_records[existing_records['SFNo'] == emp_no]

                if not existing_records.empty and 'SFNo' in existing_records.columns:
                    matched_records = existing_records[existing_records['SFNo'] == emp_no]

                if matched_records.empty:
                    # Scenario: New employee
                    logger.info(f"Attempting to add new employee {emp_no} to main table.")
                    add_payload = {'fields': grist_main_fields}

                    # Add RecordHistory for new record
                    if self.month_year:
                        add_payload['fields']['RecordHistory'] = self._generate_record_history_entry("Inserted New Record")
                    else:
                        logger.warning("Month-year not available. Skipping RecordHistory entry for new record.")


                    add_url = f"{self.base_url}/tables/{self.main_table_name}/records"

                    try:
                        response = requests.post(add_url, headers=self.headers, json={'records': [add_payload]})
                        response.raise_for_status() # Will raise HTTPError for bad responses (4xx or 5xx)

                        logger.info(f"Successfully added new employee {emp_no} to main table.")
                        self._new_emp_count += 1
                        if pd.notna(new_excel_rate):
                            rate_log_entries_to_process.append({
                                'emp_no': emp_no,
                                'new_rate': new_excel_rate,
                                'is_initial': True
                            })
                        else:
                            logger.warning(f"New employee {emp_no} has no salary rate in Excel; skipping initial rate log entry.")

                    except requests.RequestException as e:
                        logger.error(f"Failed to add new employee {emp_no} to main table. Error: {e}")
                        if hasattr(e.response, 'text'):
                            logger.error(f"Response: {e.response.text}")
                        logger.warning(f"Skipping rate log entry for new employee {emp_no} due to main table add failure.")
                        # Do not add to rate_log_entries_to_process if main table add fails

                else:
                    # Scenario: Existing employee
                    record_id = matched_records['id'].iloc[0]
                    current_grist_rate = None

                    if 'Salary_PerDay' in matched_records.columns:
                        current_grist_rate = matched_records['Salary_PerDay'].iloc[0]
                    else:
                        logger.warning(f"Warning: 'Salary_PerDay' column not found in existing Grist records for employee {emp_no}.")

                    # Prepare for rate comparison
                    grist_rate_float = None
                    excel_rate_float = None
                    rates_are_different = False

                    if pd.notna(current_grist_rate):
                        try:
                            grist_rate_float = float(current_grist_rate)
                        except (ValueError, TypeError):
                            logger.warning(f"Warning: Could not convert current Grist salary rate '{current_grist_rate}' to float for employee {emp_no}.")

                    if pd.notna(new_excel_rate):
                        try:
                            excel_rate_float = float(new_excel_rate)
                        except (ValueError, TypeError):
                            logger.warning(f"Warning: Could not convert new Excel salary rate '{new_excel_rate}' to float for employee {emp_no}.")

                    # Compare rates if both are valid numbers
                    if grist_rate_float is not None and excel_rate_float is not None:
                        if grist_rate_float != excel_rate_float:
                            rates_are_different = True
                    elif grist_rate_float is None and excel_rate_float is not None:
                        # Grist rate is null/invalid, Excel rate is valid -> consider it a change to log the new rate
                        rates_are_different = True
                        logger.info(f"Employee {emp_no}: Current Grist rate is missing/invalid, new Excel rate is {excel_rate_float}. Logging change.")
                    elif grist_rate_float is not None and excel_rate_float is None:
                        # Grist rate is valid, Excel rate is null/invalid -> typically means no change or data issue in Excel
                        # Not logging this as a "rate change" to null unless explicitly required.
                        logger.info(
                            f"Employee {emp_no}: Current Grist rate is {grist_rate_float}, new Excel rate is missing/invalid. Not logging as rate change.")
                    # If both are None/invalid, they are not "different" in a way that requires logging.

                    logger.debug(f"Employee {emp_no}: Grist rate (float) = {grist_rate_float}, Excel rate (float) = {excel_rate_float}, Different = {rates_are_different}")

                    if rates_are_different and pd.notna(new_excel_rate):  # Ensure new_excel_rate is valid before logging
                        rate_log_entries_to_process.append({
                            'emp_no': emp_no,
                            'new_rate': new_excel_rate,  # Log the original Excel value
                            'is_initial': False
                        })
                        logger.info(f"Rate change detected for employee {emp_no}. Queued for rate log.")

                    # --- Start of comparison logic for updates ---
                    needs_update = False
                    updated_fields = [] # To track which fields were updated for RecordHistory
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
                                    updated_fields.append(grist_col)
                                    logger.debug(f"DEBUG: Update needed for {emp_no}: {grist_col} differs (Excel: {excel_date}, Grist: {grist_date})")
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
                                        updated_fields.append(grist_col)
                                        logger.debug(f"DEBUG: Update needed for {emp_no}: {grist_col} differs (Excel: '{excel_str}', Grist: '{grist_str}')")
                                        # No break here, continue checking other fields for more detailed logging

                    # Check for rate change as well, even though it's logged separately
                    if rates_are_different and 'Salary_PerDay' not in updated_fields:
                         updated_fields.append('Salary_PerDay')


                    # --- End of comparison logic for updates ---

                    if needs_update:
                        # Create update payload excluding name fields for existing employees
                        update_payload_fields = grist_main_fields.copy()
                        update_payload_fields.pop('FirstName', None)
                        update_payload_fields.pop('MiddleName', None)
                        update_payload_fields.pop('LastName', None)

                        # Remove Designation field for existing employees to prevent updates
                        update_payload_fields.pop('Designation', None)

                        # Generate and prepend RecordHistory entry for each updated field
                        if self.month_year and updated_fields:
                            history_entries = []
                            for field in updated_fields:
                                new_value = grist_main_fields.get(field, 'N/A') # Get new value, default to 'N/A' if not found
                                history_entry = self._generate_record_history_entry("Updated", field_name=field, new_value=new_value)
                                history_entries.append(history_entry)

                            new_history_content = "\n".join(history_entries)
                            existing_history = current_grist_record.get('RecordHistory', '')

                            # Prepend new entries, add newline if existing history is not empty
                            update_payload_fields['RecordHistory'] = f"{new_history_content}\n{existing_history}" if existing_history else new_history_content

                        elif self.month_year and not updated_fields:
                             # This case should ideally not happen if needs_update is True, but as a safeguard
                             logger.warning(f"Needs update is True for {emp_no} but no fields identified as updated. Skipping RecordHistory update.")
                        else:
                             logger.warning(f"Month-year not available. Skipping RecordHistory update for {emp_no}.")


                        # Add to main table update list (updates other fields, Salary_PerDay is formula)
                        updates_to_main_table.append({
                            'id': int(record_id),
                            'fields': update_payload_fields # Use the new dictionary
                        })
                        logger.info(f"Employee {emp_no} queued for update in main table.")
                    else:
                        logger.info(f"Employee {emp_no}: No update needed for main table fields.")


            # Perform bulk updates to the main table if any
            if updates_to_main_table:
                update_url = f"{self.base_url}/tables/{self.main_table_name}/records"
                logger.info(f"Updating {len(updates_to_main_table)} existing employee records in main table.")
                if updates_to_main_table:  # Debug sample
                    logger.debug(f"Sample update record for main table: {updates_to_main_table[0]}")

                try:
                    update_response = requests.patch(
                        update_url,
                        headers=self.headers,
                        json={'records': updates_to_main_table}
                    )
                    update_response.raise_for_status()
                    logger.info(f"Successfully updated {len(updates_to_main_table)} existing employee records in main table.")
                    self._updated_emp_count += len(updates_to_main_table) # Increment updated count
                except requests.RequestException as e:
                    logger.error(f"Error updating records in main table: {e}")
                    if hasattr(e.response, 'text'):
                        logger.error(f"Response: {e.response.text}")

            # Prepare all queued rate log entries for bulk insert
            rate_log_payloads_for_bulk = []
            if rate_log_entries_to_process:
                logger.info(f"Preparing {len(rate_log_entries_to_process)} rate log entries for bulk insert.")
                for entry_data in rate_log_entries_to_process:
                    payload = self._prepare_rate_log_entry_payload(
                        entry_data['emp_no'],
                        entry_data['new_rate'],
                        entry_data['is_initial']
                    )
                    if payload: # Only add if payload was successfully prepared (not skipped due to NaN rate)
                        rate_log_payloads_for_bulk.append(payload)

            # Perform bulk insert for rate log entries
            self.bulk_add_rate_log_entries(rate_log_payloads_for_bulk)

            # --- Mark employees as left if not in Excel and MarkAsLeft is "YES" ---
            if self.mark_as_left == "YES" and not existing_records.empty and not excel_data.empty:
                logger.info("MarkAsLeft is YES. Checking for employees in Grist but not in Excel.")
                # Get SFNo from Excel data
                excel_emp_nos = excel_data['Emp No.'].tolist()

                # Identify employees in Grist but not in Excel
                grist_only_employees = existing_records[~existing_records['SFNo'].isin(excel_emp_nos)]

                if not grist_only_employees.empty:
                    logger.info(f"Found {len(grist_only_employees)} employees in Grist not present in Excel.")
                    left_updates = []
                    for _, emp_row in grist_only_employees.iterrows():
                        record_id = emp_row['id']
                        current_left_status = emp_row.get('Left', False) # Get current 'Left' status, default to False

                        # Only update if the 'Left' field is not already True
                        if not current_left_status:
                            logger.info(f"Marking employee {emp_row['SFNo']} as Left.")
                            update_payload = {
                                'id': int(record_id),
                                'fields': {
                                    'Left': True
                                }
                            }
                            # Add RecordHistory entry for marking as Left
                            if self.month_year:
                                history_entry = self._generate_record_history_entry("Updated", field_name="Left", new_value=True)
                                existing_history = emp_row.get('RecordHistory', '')
                                update_payload['fields']['RecordHistory'] = f"{history_entry}\n{existing_history}" if existing_history else history_entry
                            else:
                                logger.warning(f"Month-year not available. Skipping RecordHistory entry for marking {emp_row['SFNo']} as Left.")

                            left_updates.append(update_payload)
                            self._marked_as_left_count += 1 # Increment the counter
                        else:
                            logger.info(f"Employee {emp_row['SFNo']} is already marked as Left. Skipping update.")


                    if left_updates:
                        update_url = f"{self.base_url}/tables/{self.main_table_name}/records"
                        logger.info(f"Updating 'Left' status for {len(left_updates)} employees in main table.")
                        try:
                            update_response = requests.patch(
                                update_url,
                                headers=self.headers,
                                json={'records': left_updates}
                            )
                            update_response.raise_for_status()
                            logger.info(f"Successfully updated 'Left' status for {len(left_updates)} employees.")
                        except requests.RequestException as e:
                            logger.error(f"Error updating 'Left' status for employees: {e}")
                            if hasattr(e.response, 'text'):
                                logger.error(f"Response: {e.response.text}")
                else:
                    logger.info("No employees found in Grist that are not present in Excel.")
            elif self.mark_as_left != "YES":
                logger.info("MarkAsLeft is not YES. Skipping marking employees as left.")
            # --- End of MarkAsLeft logic ---


        except requests.RequestException as e:  # Catching general request exceptions earlier in the new logic
            logger.error(f"A Grist API request failed during the process: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
        except Exception as e:
            import traceback
            logger.error(f"Unexpected error: {e}")
            logger.error(traceback.format_exc())

        # Print summary of actions
        logger.info("\n--- Update Summary ---")
        logger.info(f"New employees added to {self.main_table_name}: {self._new_emp_count}")
        logger.info(f"Existing employees updated in {self.main_table_name}: {self._updated_emp_count}")
        logger.info(f"Rate log entries added to {self.rate_log_table_name}: {self._rate_log_count}")
        logger.info(f"Employees marked as left in {self.main_table_name}: {self._marked_as_left_count}")
        logger.info("----------------------\n")
