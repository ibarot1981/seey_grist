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

class HourClockExcelReader:
    def __init__(self, file_path=None):
        """
        Initialize HourClockExcelReader with optional file path.
        If not provided, uses values from .env
        """
        self.file_path = file_path or os.getenv('EXCEL_FILE_PATH')
        self.sheet_name = os.getenv('HOURCLOCK_SHEET_NAME', 'HourClock')
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
        Read the HourClock sheet from the Excel file with a two-row header.

        :return: pandas DataFrame of the sheet
        """
        try:
            # Check if file exists
            if not os.path.exists(self.file_path):
                logger.error(f"Excel file not found at {self.file_path}")
                return None

            # Read the entire sheet without header first to know the total number of columns
            full_df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=None,
                engine='openpyxl'
            )
            
            total_columns = full_df.shape[1]
            logger.info(f"Total columns in Excel sheet: {total_columns}")
            
            # Read the first two rows to get the headers
            header_df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=None,
                nrows=2,
                engine='openpyxl'
            )

            # Construct the new column names
            new_columns = []
            # Handle the first three columns
            new_columns.append('No')      # Mapping 'No.' to 'No'
            new_columns.append('SFNo')    # Mapping 'Emp No.' to 'SFNo'
            new_columns.append('Name')    # Mapping 'Name' to 'Name'

            # Generate P and OT columns for days
            day = 1
            column_index = 3  # Start from the 4th column (index 3)
            
            # Continue until we've generated enough columns or reached the end of the sheet
            while len(new_columns) < total_columns:
                if column_index < header_df.shape[1]:
                    day_number = header_df.iloc[0, column_index]
                    
                    # If day number is missing in current column, try the next column
                    if column_index + 1 < header_df.shape[1]:
                        day_number = header_df.iloc[0, column_index + 1]
                
                # Add P and OT columns regardless of whether day number was found
                new_columns.append(f'P-{day}')
                if len(new_columns) < total_columns:
                    new_columns.append(f'OT-{day}')
                
                day += 1
                column_index += 2
            
            # Ensure we have exactly the right number of columns
            if len(new_columns) > total_columns:
                new_columns = new_columns[:total_columns]
            
            logger.info(f"Generated column names: {len(new_columns)}")

            # Read the entire sheet without a header
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=None,
                engine='openpyxl'
            )

            # Skip the first two rows and reset index
            df = df.iloc[2:].copy().reset_index(drop=True)

            logger.info(f"Before assigning columns: len(new_columns)={len(new_columns)}, df.shape[1]={df.shape[1]}")
            
            # Assign column names
            df.columns = new_columns

            logger.info("DataFrame immediately after reading Excel:")
            logger.info(df.columns)
            logger.info(df.head().to_string())

            # Basic data cleaning
            # First convert SFNo to string explicitly to avoid errors
            if 'SFNo' in df.columns:
                df['SFNo'] = df['SFNo'].astype(str)
            
            # Clean up any whitespace in string columns (safely)
            for col in df.select_dtypes(include=['object']).columns:
                # Check if column contains actual strings before using str methods
                if df[col].dtype == 'object':
                    try:
                        df[col] = df[col].str.strip()
                    except AttributeError:
                        # If there's an AttributeError, convert to string first
                        df[col] = df[col].astype(str).str.strip()

            logger.info("DataFrame before SFNo filtering:")
            logger.info(df.head().to_string())

            # Filter rows where 'SFNo' starts with 'SF'
            if 'SFNo' in df.columns:
                # Ensure SFNo is string type before using string methods
                if not pd.api.types.is_string_dtype(df['SFNo']):
                    df['SFNo'] = df['SFNo'].astype(str)
                
                initial_rows = len(df)
                # Use a safer approach for filtering
                try:
                    # First check if we have any NaN values and handle them
                    df = df[~df['SFNo'].isna()]
                    # Then filter for 'SF' prefix
                    df = df[df['SFNo'].str.startswith('SF')]
                    filtered_rows = len(df)
                    if initial_rows != filtered_rows:
                        logger.info(f"Filtered out {initial_rows - filtered_rows} rows where SFNo did not start with 'SF'.")
                except Exception as e:
                    logger.error(f"Error during SFNo filtering: {e}")
                    # If filtering fails, log but continue with unfiltered data
                    pass

            return df
        except Exception as e:
            logger.error(f"Error reading HourClock Excel sheet: {e}")
            return None

    def validate_hourclock_sheet(self, df):
        """
        Validate the hour clock sheet contains all required columns and data.

        :param df: DataFrame to validate
        :return: Boolean indicating if validation passed
        """
        if df is None:
            return False

        # Check for required columns (minimum required)
        required_columns = ['No', 'SFNo', 'Name']
        
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            logger.error(f"Error: Missing required base columns in HourClock sheet DataFrame: {missing_required}")
            return False

        # Check for empty employee numbers
        if 'SFNo' in df.columns and df['SFNo'].isnull().any():
            logger.error("Error: Some employee numbers are missing in HourClock sheet")
            return False

        # Check for duplicate employee numbers
        if 'SFNo' in df.columns:
            duplicates = df['SFNo'].duplicated()
            if duplicates.any():
                logger.warning(f"Warning: Duplicate employee numbers found in HourClock sheet: {df.loc[duplicates, 'SFNo'].tolist()}")

        # Basic check for P and OT column data types (should be numeric or convertible)
        # Check only existing P/OT columns
        p_ot_cols = [col for col in df.columns if col.startswith('P-') or col.startswith('OT-')]
        for col in p_ot_cols:
            # Attempt to convert to numeric, coercing errors to NaN
            if not pd.to_numeric(df[col], errors='coerce').notna().all():
                logger.warning(f"Warning: Non-numeric values found in column {col}")

        return True
