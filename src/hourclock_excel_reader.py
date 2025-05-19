import os
import pandas as pd
from dotenv import load_dotenv
import re
from datetime import datetime
import logging # Import logging

# Load environment variables
load_dotenv()

# Configure logging (if not already configured elsewhere)
# This might be redundant if main script configures it, but safe to have
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO').upper()
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

class HourClockExcelReader:
    def __init__(self, file_path=None):
        """
        Initialize HourClockExcelReader with optional file path.
        If not provided, uses values from .env
        """
        self.file_path = file_path or os.getenv('EXCEL_FILE_PATH') # Assuming EXCEL_FILE_PATH is still relevant or will be set
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
                print(f"Warning: Could not parse date from filename using DD-MM-YYYY format: {filename}")
                return None # Return None if DD-MM-YYYY parsing fails

            return date_obj.strftime('%b-%y')
        else:
            print(f"Warning: No date found in filename: {filename}")
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
                print(f"Error: Excel file not found at {self.file_path}")
                return None

            # Read the first two rows to get the headers
            header_df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=None, # Read without header
                nrows=2,     # Read only the first two rows
                engine='openpyxl'
            )

            # Construct the new column names
            new_columns = []
            # Handle the first three columns
            new_columns.append('No') # Mapping 'No.' to 'No'
            new_columns.append('SFNo') # Mapping 'Emp No.' to 'SFNo'
            new_columns.append('Name') # Mapping 'Name' to 'Name'

            # Handle the daily P and OT columns
            # Iterate through columns starting from the 4th column (index 3)
            for i in range(3, header_df.shape[1], 2):
                day_number = header_df.iloc[0, i] # Get the day number from the first row
                if pd.isna(day_number):
                    # If day number is missing, try the next column (merged cell)
                    day_number = header_df.iloc[0, i+1]
                    if pd.isna(day_number):
                         # If day number is still missing, stop processing P/OT columns
                         break

                try:
                    day = int(day_number)
                    new_columns.append(f'P-{day}')
                    new_columns.append(f'OT-{day}')
                except ValueError:
                    # If invalid day number, stop processing P/OT columns
                    break

            # Read the entire sheet without a header
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=None, # Read without header
                engine='openpyxl'
            )

            # Skip the first two rows and reset index
            df = df.iloc[2:].copy().reset_index(drop=True)

            logging.info(f"Before assigning columns: len(new_columns)={len(new_columns)}, df.shape[1]={df.shape[1]}")
            # Assign column names
            df.columns = new_columns

            logging.info("DataFrame immediately after reading Excel:")
            logging.info(df.columns)
            logging.info(df.head().to_string())

            # Basic data cleaning
            # Clean up any whitespace in string columns
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]

            # Ensure 'SFNo' is treated as string (using the new column name)
            if 'SFNo' in df.columns:
                df['SFNo'] = df['SFNo'].astype(str) # Corrected to SFNo

            logging.info("DataFrame before SFNo filtering:")
            logging.info(df.head().to_string())

            # Filter rows where 'SFNo' starts with 'SF'
            if 'SFNo' in df.columns:
                initial_rows = len(df)
                df = df[df['SFNo'].astype(str).str.match(r'^SF', na=False)]
                filtered_rows = len(df)
                if initial_rows != filtered_rows:
                    print(f"Filtered out {initial_rows - filtered_rows} rows where SFNo did not start with 'SF'.")

            # Check if required columns exist (using the new column names)
            required_columns = ['No', 'SFNo', 'Name'] # Corrected to SFNo
            for day in range(1, 32):
                required_columns.append(f'P-{day}')
                required_columns.append(f'OT-{day}')

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Warning: Missing expected columns in HourClock sheet after reading: {missing_columns}")
                # Depending on requirements, you might return None or an empty DataFrame here
                # For now, we'll proceed with available columns but warn the user
                # return None

            return df
        except Exception as e:
            print(f"Error reading HourClock Excel sheet: {e}")
            return None

    def validate_hourclock_sheet(self, df):
        """
        Validate the hour clock sheet contains all required columns and data
        (using the new column names).

        :param df: DataFrame to validate
        :return: Boolean indicating if validation passed
        """
        if df is None:
            return False

        # Check for required columns (using the new column names)
        required_columns = ['No', 'SFNo', 'Name'] # Corrected to SFNo
        for day in range(1, 32):
            required_columns.append(f'P-{day}')
            required_columns.append(f'OT-{day}')

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns in HourClock sheet DataFrame: {missing_columns}")
            return False

        # Check for empty employee numbers (using the new column name)
        if 'SFNo' in df.columns and df['SFNo'].isnull().any(): # Corrected to SFNo
            print("Error: Some employee numbers are missing in HourClock sheet")
            return False

        # Check for duplicate employee numbers (using the new column name)
        if 'SFNo' in df.columns: # Corrected to SFNo
            duplicates = df['SFNo'].duplicated() # Corrected to SFNo
            if duplicates.any():
                print(f"Warning: Duplicate employee numbers found in HourClock sheet: {df.loc[duplicates, 'SFNo'].tolist()}") # Corrected to SFNo

        # Basic check for P and OT column data types (should be numeric or convertible)
        for day in range(1, 32):
            p_col = f'P-{day}'
            ot_col = f'OT-{day}'
            if p_col in df.columns:
                # Attempt to convert to numeric, coercing errors to NaN
                if not pd.to_numeric(df[p_col], errors='coerce').notna().all():
                     print(f"Warning: Non-numeric values found in column {p_col}")
            if ot_col in df.columns:
                 if not pd.to_numeric(df[ot_col], errors='coerce').notna().all():
                     print(f"Warning: Non-numeric values found in column {ot_col}")


        return True

# Example usage removed to simplify the file and avoid replace_in_file issues.
# You can test this class by creating a separate test script or using an interactive session.
