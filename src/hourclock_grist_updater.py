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
        self._skipped_records_count = 0

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
            columns_url = f"{self.base_url}/tables/{self.hourclock_table_name}/columns"
            columns_response = requests.get(columns_url, headers=self.headers)
            columns_response.raise_for_status()
            columns_data = columns_response.json()

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

            else:
                logger.warning("Unexpected response format from /columns endpoint.")
                logger.warning(f"Raw response content: {columns_response.text}")
                self.table_columns = []

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching table columns from /columns endpoint: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            self.table_columns = []

        except Exception as e:
            import traceback
            logger.error(f"Unexpected error during table schema fetch: {e}")
            logger.error(traceback.format_exc())
            self.table_columns = []

    def check_month_year_exists(self):
        """
        Check if any records exist for the given Month_Year
        
        :return: Boolean indicating if Month_Year exists in Grist
        """
        try:
            filter_value_json = json.dumps({"Month_Year": [self.month_year]})
            filter_params = {
                "filter": filter_value_json,
                "expand": "1"  # Expand reference columns for consistency
            }
            url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"

            logger.info(f"Checking if Month_Year {self.month_year} exists in Grist")
            response = requests.get(url, headers=self.headers, params=filter_params)
            response.raise_for_status()

            records_data = response.json().get('records', [])
            exists = len(records_data) > 0
            
            logger.info(f"Month_Year {self.month_year} {'exists' if exists else 'does not exist'} in Grist (found {len(records_data)} records)")
            return exists

        except requests.RequestException as e:
            logger.error(f"Error checking if Month_Year exists: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return False

    def get_existing_sfnos_for_month(self):
        """
        Get all existing SFNos for the given Month_Year
        Since SFNo is a reference column, we need to fetch the actual values from Emp_Master
        
        :return: Set of existing SFNos
        """
        try:
            # First, get the HC_Detail records with SFNo reference IDs
            filter_value_json = json.dumps({"Month_Year": [self.month_year]})
            filter_params = {
                "filter": filter_value_json
            }
            url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"

            logger.info(f"Fetching HC_Detail records for Month_Year {self.month_year}")
            response = requests.get(url, headers=self.headers, params=filter_params)
            response.raise_for_status()

            records_data = response.json().get('records', [])
            
            if not records_data:
                logger.info(f"No records found for {self.month_year}")
                return set()
            
            # Debug: Print raw response structure for first record
            logger.debug(f"Sample HC_Detail record structure: {json.dumps(records_data[0], indent=2)}")
            
            # Extract all SFNo reference IDs
            sfno_ref_ids = set()
            for record in records_data:
                fields = record.get('fields', {})
                
                # Try multiple possible field names for SFNo
                sfno_ref_id = None
                possible_names = ['SFNo', 'SFno', 'sfno', 'SFNO', 'SF_No', 'sf_no']
                
                for field_name in possible_names:
                    if field_name in fields:
                        sfno_ref_id = fields[field_name]
                        logger.debug(f"Found SFNo reference ID using field '{field_name}': {sfno_ref_id}")
                        break
                
                if sfno_ref_id:
                    sfno_ref_ids.add(sfno_ref_id)
                else:
                    logger.warning(f"Could not find SFNo field in HC_Detail record. Available fields: {list(fields.keys())}")
            
            logger.info(f"Found {len(sfno_ref_ids)} unique SFNo reference IDs: {sorted(sfno_ref_ids)}")
            
            if not sfno_ref_ids:
                return set()
            
            # Now fetch the actual SFNo values from Emp_Master table
            return self._get_sfno_values_from_emp_master(sfno_ref_ids)

        except requests.RequestException as e:
            logger.error(f"Error fetching HC_Detail records: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return set()

    def _get_sfno_values_from_emp_master(self, sfno_ref_ids):
        """
        Fetch actual SFNo values from Emp_Master table using reference IDs
        
        :param sfno_ref_ids: Set of reference IDs to look up
        :return: Set of actual SFNo values
        """
        try:
            # Assume Emp_Master is the table name - you might need to adjust this
            emp_master_table = "Emp_Master"
            url = f"{self.base_url}/tables/{emp_master_table}/records"
            
            logger.info(f"Fetching SFNo values from {emp_master_table} table")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            emp_records = response.json().get('records', [])
            
            if emp_records:
                logger.debug(f"Sample Emp_Master record structure: {json.dumps(emp_records[0], indent=2)}")
            
            # Create a mapping of record ID to SFNo value
            existing_sfnos = set()
            
            for record in emp_records:
                record_id = record.get('id')
                fields = record.get('fields', {})
                
                # Only process records that are referenced in HC_Detail
                if record_id in sfno_ref_ids:
                    # Find the SFNo field in Emp_Master
                    sfno_value = None
                    possible_names = ['SFNo', 'SFno', 'sfno', 'SFNO', 'SF_No', 'sf_no']
                    
                    for field_name in possible_names:
                        if field_name in fields:
                            sfno_value = fields[field_name]
                            logger.debug(f"Found SFNo value for ID {record_id}: {sfno_value}")
                            break
                    
                    if sfno_value:
                        existing_sfnos.add(str(sfno_value).strip())
                    else:
                        logger.warning(f"Could not find SFNo field in Emp_Master record ID {record_id}. Available fields: {list(fields.keys())}")
            
            logger.info(f"Successfully resolved {len(existing_sfnos)} SFNo values: {sorted(existing_sfnos)}")
            return existing_sfnos
            
        except requests.RequestException as e:
            logger.error(f"Error fetching from Emp_Master table: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return set()

    def process_excel_data(self, excel_data):
        """
        Process Excel data and insert only new records based on SFNo
        
        :param excel_data: DataFrame containing Excel hour clock data
        """
        if self.month_year is None:
            logger.error("Month-year is not set. Cannot process HourClock data.")
            return

        if not self.table_columns:
            logger.error("Grist table schema not available. Cannot proceed with processing records.")
            return

        logger.info("Excel data columns received:")
        logger.info(excel_data.columns.tolist())
        logger.info(f"Processing {len(excel_data)} rows from Excel")

        # Clean the Excel data
        excel_data = excel_data.copy()

        # Remove rows with NaN or null in the 'SFNo' column
        if 'SFNo' in excel_data.columns:
            null_emp_nos = excel_data['SFNo'].isna()
            if null_emp_nos.any():
                logger.warning(f"Found {null_emp_nos.sum()} rows with empty employee numbers. These will be skipped.")
                excel_data = excel_data.dropna(subset=['SFNo'])

            # Remove rows where 'SFNo' is 'nan' as a string
            nan_emp_nos = excel_data['SFNo'] == 'nan'
            if nan_emp_nos.any():
                logger.warning(f"Found {nan_emp_nos.sum()} rows with 'nan' as employee number. These will be skipped.")
                excel_data = excel_data[~nan_emp_nos]

            # Ensure 'SFNo' is treated as string and strip whitespace
            excel_data['SFNo'] = excel_data['SFNo'].astype(str).str.strip()

            # Check for duplicates in Excel
            duplicates = excel_data['SFNo'].duplicated()
            if duplicates.any():
                duplicate_emp_nos = excel_data.loc[duplicates, 'SFNo'].tolist()
                logger.warning(f"Duplicate employee numbers found in Excel: {duplicate_emp_nos}")
                logger.warning("Only the last occurrence of each duplicate will be processed.")
                excel_data = excel_data.drop_duplicates(subset=['SFNo'], keep='last')

        # Check if Month_Year exists in Grist
        month_year_exists = self.check_month_year_exists()
        
        if month_year_exists:
            # Get existing SFNos for this month
            existing_sfnos = self.get_existing_sfnos_for_month()
        else:
            # No records exist for this month, so all records are new
            existing_sfnos = set()
            logger.info(f"No existing records found for {self.month_year}. All Excel records will be inserted.")

        # Prepare records to insert
        records_to_add = []
        skipped_sfnos = []

        for _, excel_row in excel_data.iterrows():
            emp_no = str(excel_row['SFNo'])
            
            # Check if this SFNo already exists for this month
            if emp_no in existing_sfnos:
                logger.info(f"Skipping SFNo {emp_no} - already exists for {self.month_year}")
                skipped_sfnos.append(emp_no)
                continue

            # Prepare Grist fields for new record
            grist_fields = {
                'Month_Year': self.month_year,
                'SFNo': emp_no,
            }

            # Add Sr_No if available
            sr_no = excel_row.get('No')
            if 'Sr_No' in self.table_columns and pd.notna(sr_no):
                grist_fields['Sr_No'] = sr_no

            # Process P columns (presence data)
            p_cols = [col for col in excel_row.index if col.startswith('P-') or col.startswith('P_')]
            for p_col in p_cols:
                # Extract day number
                if '-' in p_col:
                    day = p_col.split('-')[1]
                else:
                    day = p_col.split('_')[1]

                # Determine Grist column name (prefer underscore format)
                grist_p_col = None
                if f'P_{day}' in self.table_columns:
                    grist_p_col = f'P_{day}'
                elif f'P-{day}' in self.table_columns:
                    grist_p_col = f'P-{day}'

                if grist_p_col:
                    p_value = excel_row[p_col]
                    if pd.notna(p_value):
                        try:
                            grist_fields[grist_p_col] = int(p_value)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert P value '{p_value}' to integer for EmpNo {emp_no}, Day {day}")
                            grist_fields[grist_p_col] = None
                    else:
                        grist_fields[grist_p_col] = None

            # Process OT columns (overtime data)
            ot_cols = [col for col in excel_row.index if col.startswith('OT-') or col.startswith('OT_')]
            for ot_col in ot_cols:
                # Extract day number
                if '-' in ot_col:
                    day = ot_col.split('-')[1]
                else:
                    day = ot_col.split('_')[1]

                # Determine Grist column name (prefer underscore format)
                grist_ot_col = None
                if f'OT_{day}' in self.table_columns:
                    grist_ot_col = f'OT_{day}'
                elif f'OT-{day}' in self.table_columns:
                    grist_ot_col = f'OT-{day}'

                if grist_ot_col:
                    ot_value = excel_row[ot_col]
                    if pd.notna(ot_value):
                        try:
                            grist_fields[grist_ot_col] = float(ot_value)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert OT value '{ot_value}' to float for EmpNo {emp_no}, Day {day}")
                            grist_fields[grist_ot_col] = None
                    else:
                        grist_fields[grist_ot_col] = None

            records_to_add.append({'fields': grist_fields})
            logger.info(f"Prepared record for insertion: SFNo {emp_no} for {self.month_year}")

        # Insert new records
        if records_to_add:
            self._insert_records(records_to_add)
        else:
            logger.info("No new records to insert.")

        # Update counters
        self._skipped_records_count = len(skipped_sfnos)

        # Print summary
        self._print_summary(skipped_sfnos)

    def _insert_records(self, records_to_add):
        """
        Insert records into Grist
        
        :param records_to_add: List of records to insert
        """
        add_url = f"{self.base_url}/tables/{self.hourclock_table_name}/records"
        logger.info(f"Inserting {len(records_to_add)} new records into {self.hourclock_table_name}")

        try:
            add_response = requests.post(
                add_url,
                headers=self.headers,
                json={'records': records_to_add}
            )
            add_response.raise_for_status()
            logger.info(f"Successfully inserted {len(records_to_add)} new records.")
            self._new_records_count = len(records_to_add)
            
        except requests.RequestException as e:
            logger.error(f"Error inserting new records: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
                
                # Try to parse error details
                try:
                    error_data = json.loads(e.response.text)
                    error_message = error_data.get('error', '')
                    if "Invalid column" in error_message:
                        invalid_col = error_message.split('"')[1] if '"' in error_message else "unknown"
                        logger.error(f"The column '{invalid_col}' doesn't exist in the Grist table.")
                        logger.error(f"Available columns: {', '.join(self.table_columns)}")
                except:
                    pass

    def _print_summary(self, skipped_sfnos):
        """
        Print summary of the operation
        
        :param skipped_sfnos: List of SFNos that were skipped
        """
        logger.info("\n" + "="*50)
        logger.info("HOURCLOCK UPDATE SUMMARY")
        logger.info("="*50)
        logger.info(f"Month/Year processed: {self.month_year}")
        logger.info(f"New records inserted: {self._new_records_count}")
        logger.info(f"Records skipped (duplicates): {self._skipped_records_count}")
        
        if skipped_sfnos:
            logger.info(f"Skipped SFNos: {', '.join(sorted(skipped_sfnos))}")
        
        logger.info("="*50 + "\n")

    # Keep the original method name for backward compatibility
    def compare_and_update(self, excel_data):
        """
        Main method to process Excel data (backward compatibility)
        
        :param excel_data: DataFrame containing Excel hour clock data
        """
        self.process_excel_data(excel_data)