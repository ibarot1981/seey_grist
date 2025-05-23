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

class OTExcelReader:
    def __init__(self, file_path=None):
        """
        Initialize OTExcelReader with optional file path.
        If not provided, uses values from .env
        """
        self.file_path = file_path or os.getenv('EXCEL_FILE_PATH')
        self.sheet_name = os.getenv('OT_SHEET_NAME', 'OT') 
        self.month_year = self._extract_month_year_from_filename()

    def _extract_month_year_from_filename(self):
        """
        Extracts month and year in MMM-YY format from the filename.
        Assumes filename contains a date in MM-DD-YYYY or YYYY-MM-DD format.
        """
        if not self.file_path:
            return None

        filename = os.path.basename(self.file_path)
        # Regex to find dates in MM-DD-YYYY or YYYY-MM-DD format
        date_match = re.search(r'(\d{1,2}-\d{1,2}-\d{4})|(\d{4}-\d{1,2}-\d{1,2})', filename)

        if date_match:
            date_str = date_match.group(0)
            try:
                # Attempt to parse with DD-MM-YYYY
                date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            except ValueError:
                try:
                    # If DD-MM-YYYY fails, try MM-DD-YYYY
                    date_obj = datetime.strptime(date_str, '%m-%d-%Y')
                except ValueError:
                    try:
                        # If MM-DD-YYYY fails, try YYYY-MM-DD
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

    def read_sheet(self):
        """
        Read the OT sheet from the Excel file.
        Assumes a single-row header.

        :return: pandas DataFrame of the sheet
        """
        try:
            # Check if file exists
            if not os.path.exists(self.file_path):
                logger.error(f"Excel file not found at {self.file_path}")
                return None

            # Read the sheet with the first row as header
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                engine='openpyxl'
            )
            
            logger.info("DataFrame immediately after reading Excel:")
            logger.info(df.columns)
            logger.info(df.head().to_string())

            # Rename columns as per mapping
            column_mapping = {
                "Emp No.": "SFNo",
                "ESIC  Rate": "ESIC_Rate",
                "OT Hours": "OT_Hours",
                "OT Hours Calculated": "OT_Hours_Calc",
                "OT Rate": "OT_Rate",
                "OT Amount": "OT_Amt",
                "ESIC on OT (4%)": "ESIC_4pct_Amt",
                "ESIC on OT (0.75%)": "ESIC_075pct_Amt",
                "Emp Type : Temp / Perm": "Emp_Type" # Added for filtering
            }
            df.rename(columns=column_mapping, inplace=True)

            # Add Month_Year column
            if self.month_year:
                df['Month_Year'] = self.month_year
                logger.info(f"Added 'Month_Year' column with value: {self.month_year}")
            else:
                logger.warning("Month_Year could not be extracted from filename. 'Month_Year' column will not be added.")

            # Basic data cleaning
            # First convert SFNo to string explicitly to avoid errors
            if 'SFNo' in df.columns:
                df['SFNo'] = df['SFNo'].astype(str)
            
            # Clean up any whitespace in string columns (safely)
            for col in df.select_dtypes(include=['object']).columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = df[col].str.strip()
                    except AttributeError:
                        df[col] = df[col].astype(str).str.strip()

            logger.info("DataFrame before filtering:")
            logger.info(df.head().to_string())

            return df
        except Exception as e:
            logger.error(f"Error reading OT Excel sheet: {e}")
            return None

    def validate_ot_sheet(self, df):
        """
        Validate the OT sheet contains all required columns and data.

        :param df: DataFrame to validate
        :return: Boolean indicating if validation passed
        """
        if df is None:
            return False

        # Check for required columns based on mapping
        required_columns = [
            "SFNo", "ESIC_Rate", "OT_Hours", "OT_Hours_Calc", "OT_Rate", 
            "OT_Amt", "ESIC_4pct_Amt", "ESIC_075pct_Amt", "Emp_Type", "Month_Year"
        ]
        
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            logger.error(f"Error: Missing required columns in OT sheet DataFrame: {missing_required}")
            return False

        # Check for empty employee numbers
        if 'SFNo' in df.columns and df['SFNo'].isnull().any():
            logger.error("Error: Some employee numbers are missing in OT sheet")
            return False

        # Check numeric columns for non-numeric values
        numeric_cols = [
            "ESIC_Rate", "OT_Hours", "OT_Hours_Calc", "OT_Rate", 
            "OT_Amt", "ESIC_4pct_Amt", "ESIC_075pct_Amt"
        ]
        for col in numeric_cols:
            if col in df.columns:
                if not pd.to_numeric(df[col], errors='coerce').notna().all():
                    logger.warning(f"Warning: Non-numeric values found in column {col}")

        return True
