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

class SalaryStatementExcelReader:
    def __init__(self, file_path=None):
        """
        Initialize SalaryStatementExcelReader with optional file path.
        If not provided, uses values from .env
        """
        self.file_path = file_path or os.getenv('EXCEL_FILE_PATH')
        self.sheet_name = os.getenv('SALARY_STATEMENT_SHEET_NAME', 'SalaryStatement')
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
        Read the SalaryStatement sheet from the Excel file.
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
                'No.': 'SrNo',
                'Emp No.': 'SFNo',
                'Salary Rate (Per Day)': 'Rate_Per_Day',
                'No Of Days Present': 'Present_Days',
                'Basic Salary': 'BasicSalary_Amt',
                'Total OT Hours': 'TotalOT_Hours',
                'OT Rate Per Hour': 'OT_Rate_PerHour',
                'OT Salary': 'OTSalary_Amt',
                'Gross Salary': 'GrossSalary_Amt',
                'Adv Amt': 'Advance_Amt',
                'Loan Amt': 'Loan_Amt',
                'ESI Amt': 'ESI_Amt',
                'PF Amt': 'PF_Amt',
                'Prof Tax': 'ProfTax_Amt',
                'Total Deductions': 'TotalDeductions_Amt',
                'Net Salary': 'NetSalary_Amt',
                'Salary To Pay (Rounded Off)': 'TotalRoundOff_Amt'
            }
            df.rename(columns=column_mapping, inplace=True)
            logger.info("DataFrame columns after renaming:")
            logger.info(df.columns.tolist())

            # Convert numeric columns to appropriate types, coercing errors
            numeric_cols_to_convert = [
                'Rate_Per_Day', 'Present_Days', 'BasicSalary_Amt',
                'TotalOT_Hours', 'OT_Rate_PerHour', 'OTSalary_Amt', 'GrossSalary_Amt',
                'Advance_Amt', 'Loan_Amt', 'ESI_Amt', 'PF_Amt', 'ProfTax_Amt',
                'TotalDeductions_Amt', 'NetSalary_Amt', 'TotalRoundOff_Amt'
            ]
            for col in numeric_cols_to_convert:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Fill NaN values in these numeric columns with 0
                    df[col] = df[col].fillna(0)
                    logger.info(f"Converted column '{col}' to numeric and filled NaNs with 0.")

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

            logger.info("DataFrame before SFNo filtering:")
            logger.info(df.head().to_string())

            # No specific SFNo filtering like 'SF' prefix for SalaryStatement

            return df
        except Exception as e:
            logger.error(f"Error reading SalaryStatement Excel sheet: {e}")
            return None

    def validate_salary_statement_sheet(self, df):
        """
        Validate the salary statement sheet contains all required columns and data.

        :param df: DataFrame to validate
        :return: Boolean indicating if validation passed
        """
        if df is None:
            return False

        # Check for required columns based on mapping
        required_columns = [
            "SrNo", "SFNo", "Rate_Per_Day", "Present_Days", "BasicSalary_Amt",
            "TotalOT_Hours", "OT_Rate_PerHour", "OTSalary_Amt", "GrossSalary_Amt",
            "Advance_Amt", "Loan_Amt", "ESI_Amt", "PF_Amt", "ProfTax_Amt",
            "TotalDeductions_Amt", "NetSalary_Amt", "TotalRoundOff_Amt", "Month_Year"
        ]
        
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            logger.error(f"Error: Missing required columns in SalaryStatement sheet DataFrame: {missing_required}")
            return False

        # Check for empty employee numbers
        if 'SFNo' in df.columns and df['SFNo'].isnull().any():
            logger.error("Error: Some employee numbers are missing in SalaryStatement sheet")
            return False

        # Check for duplicate employee numbers (optional, but good practice)
        if 'SFNo' in df.columns:
            duplicates = df['SFNo'].duplicated()
            if duplicates.any():
                logger.warning(f"Warning: Duplicate employee numbers found in SalaryStatement sheet: {df.loc[duplicates, 'SFNo'].tolist()}")

        # Check numeric columns for non-numeric values
        numeric_cols = [
            "Rate_Per_Day", "Present_Days", "BasicSalary_Amt",
            "TotalOT_Hours", "OT_Rate_PerHour", "OTSalary_Amt", "GrossSalary_Amt",
            "Advance_Amt", "Loan_Amt", "ESI_Amt", "PF_Amt", "ProfTax_Amt",
            "TotalDeductions_Amt", "NetSalary_Amt", "TotalRoundOff_Amt"
        ]
        for col in numeric_cols:
            if col in df.columns:
                if not pd.to_numeric(df[col], errors='coerce').notna().all():
                    logger.warning(f"Warning: Non-numeric values found in column {col}")

        return True
