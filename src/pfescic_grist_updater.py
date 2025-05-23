import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class PFESICGristUpdater:
    def __init__(self,
                 api_key=None,
                 doc_id=None,
                 pfesic_table_name=None,
                 new_pfesic_table_name=None,
                 base_url=None,
                 month_year=None):
        """
        Initialize PFESICGristUpdater

        :param api_key: Grist API key
        :param doc_id: Grist document ID
        :param pfesic_table_name: Name of the PF-ESIC table to update
        :param new_pfesic_table_name: Name of the NEW PF-ESIC table to update
        :param base_url: Optional base URL for custom Grist installations
        :param month_year: Month and year in MMM-YY format from the Excel file
        """
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.doc_id = doc_id or os.getenv('GRIST_DOC_ID')
        self.pfesic_table_name = pfesic_table_name or os.getenv('GRIST_DUMP_PFESIC_TABLE_NAME')
        self.new_pfesic_table_name = new_pfesic_table_name or os.getenv('GRIST_DUMP_NW_PFESIC_TABLE_NAME')

        grist_url = base_url or os.getenv('GRIST_BASE_URL', 'https://docs.getgrist.com')
        self.base_url = f"{grist_url}/api/docs/{self.doc_id}"

        self.month_year = month_year

        logger.info(f"Using Grist API at: {self.base_url}")
        logger.info(f"PF-ESIC Table: {self.pfesic_table_name}")
        logger.info(f"NEW PF-ESIC Table: {self.new_pfesic_table_name}")

        # Column mappings from Excel to Grist (same for both tables)
        self.excel_to_grist_mapping = {
            "SrNo": "SrNo",
            "SFNo": "SFNo",
            "PresentDay": "PresentDay",
            "Basic_DA_PerDay": "Basic_DA_PerDay",
            "HRA_PerDay": "HRA_PerDay",
            "Conv_PerDay": "Conv_PerDay",
            "WA_PerDay": "WA_PerDay",
            "Basic_Amt": "Basic_Amt",
            "ActualBasic_Amt": "ActualBasic_Amt",
            "HRA_Amt": "HRA_Amt",
            "Conv_Amt": "Conv_Amt",
            "WA_Amt": "WA_Amt",
            "GrossAmt": "GrossAmt",
            "PF_Amt": "PF_Amt",
            "ESIC_Amt": "ESIC_Amt",
            "PTax_Amt": "PTax_Amt",
            "TotalDed_Amt": "TotalDed_Amt",
            "NetPayable_Amt": "NetPayable_Amt",
            "Month_Year": "Month_Year" # This column is added by the reader
        }

        self._total_records_inserted_pfesic = 0
        self._total_records_inserted_new_pfesic = 0

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def get_existing_records(self, table_name):
        """
        Fetch existing records from a specific Grist table.

        :param table_name: Name of the Grist table
        :return: DataFrame of existing records
        """
        try:
            url = f"{self.base_url}/tables/{table_name}/records"
            logger.info(f"Fetching records from: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            records_data = response.json().get('records', [])
            logger.info(f"Fetched {len(records_data)} records from {table_name}")

            if not records_data:
                try:
                    schema_url = f"{self.base_url}/tables/{table_name}"
                    schema_response = requests.get(schema_url, headers=self.headers)
                    schema_response.raise_for_status()
                    fields = schema_response.json().get('fields', {})
                    columns = list(fields.keys()) + ['id']
                    return pd.DataFrame(columns=columns)
                except Exception as e:
                    logger.warning(f"Could not fetch schema for {table_name}: {e}")
                    return pd.DataFrame()

            records_df = pd.DataFrame([
                {**record['fields'], 'id': record['id']}
                for record in records_data
            ])
            return records_df

        except requests.RequestException as e:
            logger.error(f"Error fetching existing records from {table_name}: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            return pd.DataFrame()

    def check_existing_month_year_records(self):
        """
        Checks if records for the current month_year already exist in Emp_Dump_PFESIC.
        If they exist, the script should not proceed further.

        :return: True if records exist, False otherwise.
        """
        if not self.month_year:
            logger.error("Month_Year is not set. Cannot check for existing records.")
            return True # Prevent execution if month_year is missing

        logger.info(f"Checking for existing records for Month_Year '{self.month_year}' in table '{self.pfesic_table_name}'...")
        existing_pfesic_records = self.get_existing_records(self.pfesic_table_name)

        if not existing_pfesic_records.empty and 'Month_Year' in existing_pfesic_records.columns:
            # Ensure 'Month_Year' column is string type for robust comparison
            existing_pfesic_records['Month_Year'] = existing_pfesic_records['Month_Year'].astype(str)
            
            # Check if any record matches the current month_year
            if (existing_pfesic_records['Month_Year'] == self.month_year).any():
                logger.warning(f"Records for Month_Year '{self.month_year}' already exist in '{self.pfesic_table_name}'. Skipping further insertion.")
                return True
        
        logger.info(f"No existing records found for Month_Year '{self.month_year}' in '{self.pfesic_table_name}'. Proceeding with insertion.")
        return False

    def _prepare_records_payload(self, df):
        """
        Prepares a list of record payloads for bulk insertion into Grist.
        """
        records_payload = []
        for _, row in df.iterrows():
            fields = {}
            for excel_col, grist_col in self.excel_to_grist_mapping.items():
                if excel_col in row.index:
                    value = row[excel_col]
                    # Convert pandas NaN to None for JSON null
                    fields[grist_col] = None if pd.isna(value) else value
            records_payload.append({'fields': fields})
        return records_payload

    def bulk_insert_records(self, df, table_name):
        """
        Performs a bulk insert of records to the specified Grist table.

        :param df: DataFrame containing records to insert
        :param table_name: Name of the Grist table to insert into
        :return: Number of records successfully inserted
        """
        if df is None or df.empty:
            logger.info(f"No records to insert into {table_name}.")
            return 0

        records_payload = self._prepare_records_payload(df)
        if not records_payload:
            logger.warning(f"No valid records payload generated for {table_name}.")
            return 0

        add_url = f"{self.base_url}/tables/{table_name}/records"
        payload = {'records': records_payload}

        logger.info(f"Attempting to bulk insert {len(records_payload)} records into {table_name}.")
        logger.debug(f"Sample bulk payload for {table_name}: {records_payload[0]}")

        try:
            add_response = requests.post(
                add_url,
                headers=self.headers,
                json=payload
            )
            add_response.raise_for_status()
            inserted_count = len(add_response.json().get('records', []))
            logger.info(f"Successfully bulk inserted {inserted_count} records into {table_name}.")
            return inserted_count
        except requests.RequestException as e:
            logger.error(f"Error bulk inserting records into {table_name}: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            logger.error(f"Please check that the table '{table_name}' exists and has the correct columns in your Grist document.")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error during bulk insert into {table_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0

    def update_grist_tables(self, pfesic_df, new_pfesic_df):
        """
        Inserts data from the two DataFrames into their respective Grist tables.

        :param pfesic_df: DataFrame for Emp_Dump_PFESIC table
        :param new_pfesic_df: DataFrame for Emp_Dump_NW_PFESIC table
        """
        try:
            # Check if records for the current month_year already exist
            if self.check_existing_month_year_records():
                logger.info("Skipping Grist update due to existing records for the current month_year.")
                return

            logger.info("Starting Grist update for PF-ESIC data...")
            self._total_records_inserted_pfesic = self.bulk_insert_records(pfesic_df, self.pfesic_table_name)

            logger.info("Starting Grist update for NEW PF-ESIC data...")
            self._total_records_inserted_new_pfesic = self.bulk_insert_records(new_pfesic_df, self.new_pfesic_table_name)

        except Exception as e:
            import traceback
            logger.error(f"An error occurred during Grist update process: {e}")
            logger.error(traceback.format_exc())

        self.print_summary()

    def print_summary(self):
        """
        Prints a summary of the insertion operations.
        """
        logger.info("\n--- PF-ESIC Grist Update Summary ---")
        logger.info(f"Total records inserted into '{self.pfesic_table_name}': {self._total_records_inserted_pfesic}")
        logger.info(f"Total records inserted into '{self.new_pfesic_table_name}': {self._total_records_inserted_new_pfesic}")
        logger.info("------------------------------------\n")
