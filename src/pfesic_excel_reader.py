import os
import pandas as pd
from dotenv import load_dotenv
import re
from datetime import datetime
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class PFESICExcelReader:
    def __init__(self, file_path=None):
        """
        Initialize PFESICExcelReader with optional file path.
        If not provided, uses values from .env
        """
        self.file_path = file_path or os.getenv('EXCEL_FILE_PATH')
        self.pfesic_sheet_name = os.getenv('PFESIC_SHEET_NAME', 'PF-ESIC Sheet')
        self.new_pfesic_sheet_name = os.getenv('NEW_PFESIC_SHEET_NAME', 'NEW PF ESIC')
        self.month_year = self._extract_month_year_from_filename()

        self.column_mapping = {
            "Sr. No.": "SrNo",
            "Emp No": "SFNo",
            "PRESENT DAY": "PresentDay",
            "BASIC WITH D.A": "Basic_DA_PerDay",
            "HRA    P DAY": "HRA_PerDay",
            "CONV P DAY": "Conv_PerDay",
            "W A   P DAY": "WA_PerDay",
            "BASIC": "Basic_Amt",
            "ACTUAL BASIC": "ActualBasic_Amt",
            "H.R A AMOUNT": "HRA_Amt",
            "CONV   AMOUNT": "Conv_Amt",
            "W. A     AMOUNT": "WA_Amt",
            "GROSS AMOUNT": "GrossAmt",
            "PF": "PF_Amt",
            "ESIC": "ESIC_Amt",
            "P TAX": "PTax_Amt",
            "TOTAL DED": "TotalDed_Amt",
            "NET PAYABLE": "NetPayable_Amt"
        }

    def _extract_month_year_from_filename(self):
        """
        Extracts month and year in MMM-YY format from the filename.
        Assumes filename contains a date in MM-DD-YYYY or YYYY-MM-DD format.
        """
        if not self.file_path:
            return None

        filename = os.path.basename(self.file_path)
        date_match = re.search(r'(\d{1,2}-\d{1,2}-\d{4})|(\d{4}-\d{1,2}-\d{1,2})', filename)

        if date_match:
            date_str = date_match.group(0)
            try:
                date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            except ValueError:
                try:
                    date_obj = datetime.strptime(date_str, '%m-%d-%Y')
                except ValueError:
                    try:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"Could not parse date from filename: {filename}")
                        return None

            return date_obj.strftime('%b-%y')
        else:
            logger.warning(f"No date found in filename: {filename}")
            return None

    def get_month_year(self):
        """
        Returns the extracted month-year string (MMM-YY).
        """
        return self.month_year

    def _process_sheet(self, df, sheet_name):
        """
        Applies common processing steps to a DataFrame:
        - Renames columns based on mapping
        - Adds Month_Year column
        - Filters by "Emp Type" == "Perm"
        - Cleans SFNo and other string columns
        """
        if df is None:
            return None

        logger.info(f"Processing sheet: {sheet_name}")
        logger.info(f"Original columns for {sheet_name}: {df.columns.tolist()}")

        # Rename columns
        df.rename(columns=self.column_mapping, inplace=True)
        logger.info(f"Columns after renaming for {sheet_name}: {df.columns.tolist()}")

        # Add Month_Year column
        if self.month_year:
            df['Month_Year'] = self.month_year
            logger.info(f"Added 'Month_Year' column with value: {self.month_year} to {sheet_name}")
        else:
            logger.warning(f"Month_Year could not be extracted from filename. 'Month_Year' column will not be added to {sheet_name}.")

        # Basic data cleaning: Ensure SFNo is string and strip whitespace
        if 'SFNo' in df.columns:
            df['SFNo'] = df['SFNo'].astype(str)
        
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].str.strip()
                except AttributeError:
                    df[col] = df[col].astype(str).str.strip()

        # Filter by "Emp Type" == "Perm"
        if 'Emp Type' in df.columns:
            initial_rows = len(df)
            df = df[df['Emp Type'].astype(str).str.strip().str.lower() == 'perm']
            filtered_rows = len(df)
            if initial_rows != filtered_rows:
                logger.info(f"Filtered {initial_rows - filtered_rows} rows from {sheet_name} where 'Emp Type' was not 'Perm'.")
        else:
            logger.warning(f"Column 'Emp Type' not found in {sheet_name}. Skipping 'Perm' filtering.")

        # Filter rows where 'SFNo' starts with 'SF' (as seen in other readers)
        if 'SFNo' in df.columns:
            if not pd.api.types.is_string_dtype(df['SFNo']):
                df['SFNo'] = df['SFNo'].astype(str)
            
            initial_rows = len(df)
            try:
                df = df[~df['SFNo'].isna()]
                df = df[df['SFNo'].str.startswith('SF')]
                filtered_rows = len(df)
                if initial_rows != filtered_rows:
                    logger.info(f"Filtered out {initial_rows - filtered_rows} rows from {sheet_name} where SFNo did not start with 'SF'.")
            except Exception as e:
                logger.error(f"Error during SFNo filtering for {sheet_name}: {e}")
                pass

        logger.info(f"Processed {len(df)} records for sheet: {sheet_name}")
        logger.debug(f"Sample data from {sheet_name} after processing:\n{df.head().to_string()}")
        return df

    def read_sheets(self):
        """
        Reads both PF-ESIC Sheet and NEW PF ESIC sheets from the Excel file,
        applies column mappings, adds Month_Year, and filters by "Emp Type" == "Perm".

        :return: Tuple of (pfesic_df, new_pfesic_df) or (None, None) if error
        """
        pfesic_df = None
        new_pfesic_df = None

        try:
            if not os.path.exists(self.file_path):
                logger.error(f"Excel file not found at {self.file_path}")
                return None, None

            # Read PF-ESIC Sheet
            logger.info(f"Attempting to read sheet: {self.pfesic_sheet_name}")
            try:
                df_pfesic_raw = pd.read_excel(
                    self.file_path,
                    sheet_name=self.pfesic_sheet_name,
                    engine='openpyxl'
                )
                pfesic_df = self._process_sheet(df_pfesic_raw, self.pfesic_sheet_name)
            except Exception as e:
                logger.error(f"Error reading '{self.pfesic_sheet_name}' sheet: {e}")

            # Read NEW PF ESIC Sheet
            logger.info(f"Attempting to read sheet: {self.new_pfesic_sheet_name}")
            try:
                df_new_pfesic_raw = pd.read_excel(
                    self.file_path,
                    sheet_name=self.new_pfesic_sheet_name,
                    engine='openpyxl'
                )
                new_pfesic_df = self._process_sheet(df_new_pfesic_raw, self.new_pfesic_sheet_name)
            except Exception as e:
                logger.error(f"Error reading '{self.new_pfesic_sheet_name}' sheet: {e}")

            return pfesic_df, new_pfesic_df

        except Exception as e:
            logger.error(f"Error during reading PFESIC Excel sheets: {e}")
            return None, None

    def validate_pfesic_data(self, df, sheet_name):
        """
        Validate the processed PFESIC data.

        :param df: DataFrame to validate
        :param sheet_name: Name of the sheet for logging purposes
        :return: Boolean indicating if validation passed
        """
        if df is None or df.empty:
            logger.warning(f"No data to validate for {sheet_name}.")
            return False

        required_columns = list(self.column_mapping.values()) + ["Month_Year"]
        
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            logger.error(f"Error: Missing required columns in {sheet_name} DataFrame: {missing_required}")
            return False

        if 'SFNo' in df.columns and df['SFNo'].isnull().any():
            logger.error(f"Error: Some employee numbers are missing in {sheet_name}")
            return False

        duplicates = df['SFNo'].duplicated()
        if duplicates.any():
            logger.warning(f"Warning: Duplicate employee numbers found in {sheet_name}: {df.loc[duplicates, 'SFNo'].tolist()}")
            # Optionally, remove duplicates if only unique SFNo are desired for Grist
            # df.drop_duplicates(subset=['SFNo'], keep='last', inplace=True)

        return True
