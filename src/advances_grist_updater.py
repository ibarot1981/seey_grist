import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging
import json

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class AdvancesGristUpdater:
    def __init__(self,
                 api_key=None,
                 doc_id=None,
                 advances_table_name=None, # Changed parameter name
                 base_url=None,
                 month_year=None):
        """
        Initialize AdvancesGristUpdater

        :param api_key: Grist API key
        :param doc_id: Grist document ID
        :param advances_table_name: Name of the Advances detail table to update
        :param base_url: Optional base URL for custom Grist installations
        :param month_year: Month and year in MMM-YY format from the Excel file
        """
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.doc_id = doc_id or os.getenv('GRIST_DOC_ID')
        self.advances_table_name = advances_table_name or os.getenv('GRIST_ADVANCES_TABLE_NAME', 'Emp_Advances') # Changed table name

        # Support for custom Grist installations
        grist_url = base_url or os.getenv('GRIST_BASE_URL', 'https://docs.getgrist.com')
        self.base_url = f"{grist_url}/api/docs/{self.doc_id}"

        self.month_year = month_year

        logger.info(f"Using Grist API at: {self.base_url}")
        logger.info(f"Targeting Advances table: {self.advances_table_name}")

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
            columns_url = f"{self.base_url}/tables/{self.advances_table_name}/columns" # Changed table name
            columns_response = requests.get(columns_url, headers=self.headers)
            columns_response.raise_for_status()
            columns_data = columns_response.json()

            column_list = columns_data.get('columns', [])

            if isinstance(column_list, list):
                self.table_columns = [col.get('id') for col in column_list if isinstance(col, dict) and 'id' in col]
                logger.info(f"Fetched table columns from /columns endpoint: {len(self.table_columns)} columns")
                logger.info(f"Available columns: {', '.join(sorted(self.table_columns))}")
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
            url = f"{self.base_url}/tables/{self.advances_table_name}/records" # Changed table name

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
        Get all existing SFNos for the given Month_Year from the Advances table.
        
        :return: Set of existing SFNos
        """
        try:
            filter_value_json = json.dumps({"Month_Year": [self.month_year]})
            filter_params = {
                "filter": filter_value_json
            }
            url = f"{self.base_url}/tables/{self.advances_table_name}/records" # Changed table name

            logger.info(f"Fetching Advances records for Month_Year {self.month_year}")
            response = requests.get(url, headers=self.headers, params=filter_params)
            response.raise_for_status()

            records_data = response.json().get('records', [])
            
            if not records_data:
                logger.info(f"No records found for {self.month_year}")
                return set()
            
            existing_sfnos = set()
            for record in records_data:
                fields = record.get('fields', {})
                
                # Assuming SFNo is directly in the Advances table and not a reference
                sfno_value = fields.get('SFNo') 
                
                if sfno_value:
                    existing_sfnos.add(str(sfno_value).strip())
                else:
                    logger.warning(f"Could not find SFNo field in Advances record. Available fields: {list(fields.keys())}")
            
            logger.info(f"Found {len(existing_sfnos)} unique SFNo values for {self.month_year}: {sorted(existing_sfnos)}")
            return existing_sfnos
            
        except requests.RequestException as e:
            logger.error(f"Error fetching Advances records: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return set()

    def process_excel_data(self, excel_data):
        """
        Process Excel data and insert only new records based on SFNo and Month_Year
        
        :param excel_data: DataFrame containing Excel advances data
        """
        if self.month_year is None:
            logger.error("Month-year is not set. Cannot process Advances data.")
            return

        if not self.table_columns:
            logger.error("Grist table schema not available. Cannot proceed with processing records.")
            return

        logger.info("Excel data columns received:")
        logger.info(excel_data.columns.tolist())
        logger.info(f"Processing {len(excel_data)} rows from Excel")

        # Check if Month_Year already exists in Grist
        if self.check_month_year_exists():
            logger.error(f"Error: Records for Month_Year '{self.month_year}' already exist in Grist table '{self.advances_table_name}'. Skipping insertion of all records for this month.")
            return # Exit the method, skipping all insertions

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

            # Check for duplicates in Excel (SFNo + Month_Year combination)
            duplicates = excel_data.duplicated(subset=['SFNo', 'Month_Year'])
            if duplicates.any():
                duplicate_emp_nos = excel_data.loc[duplicates, ['SFNo', 'Month_Year']].to_dict('records')
                logger.warning(f"Duplicate SFNo/Month_Year combinations found in Excel: {duplicate_emp_nos}")
                logger.warning("Only the last occurrence of each duplicate will be processed.")
                excel_data = excel_data.drop_duplicates(subset=['SFNo', 'Month_Year'], keep='last')

        # Get existing SFNos for this month from Grist
        existing_sfnos_for_month = self.get_existing_sfnos_for_month()
        
        # Prepare records to insert
        records_to_add = []
        skipped_sfnos = []

        for _, excel_row in excel_data.iterrows():
            emp_no = str(excel_row['SFNo'])
            
            # Check if this SFNo already exists for this month in Grist
            if emp_no in existing_sfnos_for_month:
                logger.info(f"Skipping SFNo {emp_no} - already exists for {self.month_year}")
                skipped_sfnos.append(emp_no)
                continue

            # Get advance and loan amounts, converting to numeric and handling NaNs
            # Use .get() to safely access columns that might not exist after initial filtering
            advance_amt = pd.to_numeric(excel_row.get('Advance_Amt'), errors='coerce')
            loan_amt = pd.to_numeric(excel_row.get('Loan_Amt'), errors='coerce')

            # Check if both advance and loan amounts are NaN or 0
            if (pd.isna(advance_amt) or advance_amt == 0) and \
               (pd.isna(loan_amt) or loan_amt == 0):
                logger.info(f"Skipping SFNo {emp_no} - no advance or loan amount for {self.month_year}")
                skipped_sfnos.append(emp_no)
                continue

            # Prepare Grist fields for new record
            grist_fields = {
                'Month_Year': self.month_year,
                'SFNo': emp_no,
            }

            # Map other columns
            if 'SrNo' in excel_row and 'SrNo' in self.table_columns:
                grist_fields['SrNo'] = excel_row['SrNo'] if pd.notna(excel_row['SrNo']) else None
            if 'Unit' in excel_row and 'Unit' in self.table_columns:
                grist_fields['Unit'] = excel_row['Unit'] if pd.notna(excel_row['Unit']) else None
            if 'Advance_Amt' in excel_row and 'Advance_Amt' in self.table_columns:
                advance_amt_value = excel_row['Advance_Amt']
                if pd.notna(advance_amt_value):
                    try:
                        grist_fields['Advance_Amt'] = float(advance_amt_value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert Advance_Amt '{advance_amt_value}' to float for EmpNo {emp_no}. Setting to None.")
                        grist_fields['Advance_Amt'] = None
                else:
                    grist_fields['Advance_Amt'] = None
            if 'Loan_Amt' in excel_row and 'Loan_Amt' in self.table_columns:
                loan_amt_value = excel_row['Loan_Amt']
                if pd.notna(loan_amt_value):
                    try:
                        grist_fields['Loan_Amt'] = float(loan_amt_value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert Loan_Amt '{loan_amt_value}' to float for EmpNo {emp_no}. Setting to None.")
                        grist_fields['Loan_Amt'] = None
                else:
                    grist_fields['Loan_Amt'] = None

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
        add_url = f"{self.base_url}/tables/{self.advances_table_name}/records" # Changed table name
        logger.info(f"Inserting {len(records_to_add)} new records into {self.advances_table_name}")

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
        logger.info("ADVANCES UPDATE SUMMARY") # Changed summary title
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
        
        :param excel_data: DataFrame containing Excel advances data
        """
        self.process_excel_data(excel_data)
