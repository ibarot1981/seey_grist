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

class ExcelReader:
    def __init__(self, file_path=None, sheet_name=None):
        """
        Initialize ExcelReader with optional file path and sheet name.
        If not provided, uses values from .env
        """
        self.file_path = file_path or os.getenv('EXCEL_FILE_PATH')
        self.sheet_name = sheet_name or os.getenv('MASTER_SHEET_NAME', 'MasterSalarySheet')
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
                logger.warning(f"Could not parse date from filename using DD-MM-YYYY format: {filename}")
                return None # Return None if DD-MM-YYYY parsing fails

            return date_obj.strftime('%b-%y')
        else:
            logger.warning(f"No date found in filename: {filename}")
            return None

    def get_month_year(self):
        """
        Returns the extracted month-year string (MMM-YY).
        """
        return self.month_year

    def read_sheet(self, sheet_name=None):
        """
        Read a specific sheet from the Excel file

        
        :param sheet_name: Optional sheet name to override default
        :return: pandas DataFrame of the sheet
        """
        try:
            sheet_to_read = sheet_name or self.sheet_name
            
            # Check if file exists
            if not os.path.exists(self.file_path):
                logger.error(f"Excel file not found at {self.file_path}")
                return None
                
            # Read Excel file
            df = pd.read_excel(
                self.file_path, 
                sheet_name=sheet_to_read, 
                engine='openpyxl'
            )
            
            # Basic data cleaning
            # Convert date columns to datetime if they exist
            if 'Date of Joining' in df.columns:
                df['Date of Joining'] = pd.to_datetime(df['Date of Joining'], errors='coerce')
                
            # Clean up any whitespace in string columns
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
                
            # Ensure 'Emp No.' is treated as string to avoid numerical comparison issues
            if 'Emp No.' in df.columns:
                df['Emp No.'] = df['Emp No.'].astype(str)
                
            # Check if required columns exist
            required_columns = [
                'Emp No.',
                'Salary Rate (Per Day)',
                'Emp Type : Temp / Perm',
                'Salary Calculation on Fixed / Hourly',
                'Date of Joining'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing columns in Excel file: {missing_columns}")
            
            return df
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            return None

    def list_sheets(self):
        """
        List all sheets in the Excel file
        
        :return: List of sheet names
        """
        try:
            if not os.path.exists(self.file_path):
                logger.error(f"Excel file not found at {self.file_path}")
                return []
                
            xls = pd.ExcelFile(self.file_path, engine='openpyxl')
            return xls.sheet_names
        except Exception as e:
            logger.error(f"Error listing sheets: {e}")
            return []

    def validate_master_sheet(self, df):
        """
        Validate the master salary sheet contains all required columns and data
        
        :param df: DataFrame to validate
        :return: Boolean indicating if validation passed
        """
        if df is None:
            return False
            
        # Check for required columns
        required_columns = [
            'Emp No.',
            'Salary Rate (Per Day)',
            'Emp Type : Temp / Perm',
            'Salary Calculation on Fixed / Hourly',
            'Date of Joining'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        # Check for empty employee numbers
        if df['Emp No.'].isnull().any():
            logger.error("Some employee numbers are missing")
            return False
            
        # Check for duplicate employee numbers
        duplicates = df['Emp No.'].duplicated()
        if duplicates.any():
            logger.warning(f"Duplicate employee numbers found: {df.loc[duplicates, 'Emp No.'].tolist()}")
            
        return True

# Example usage
if __name__ == "__main__":
    # Example usage - this part might not be used when imported by main.py
    # but it's good practice to update its logging as well.
    # Note: In a real application, you might want a separate logging config
    # for standalone script execution vs. when imported as a module.
    # For this task, we'll assume main.py's config is sufficient.
    
    # Basic console logging for standalone execution if not configured by main
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
    logger.info("Running ExcelReader example.")
    
    reader = ExcelReader()
    
    # List all sheets
    logger.info(f"Available sheets: {reader.list_sheets()}")
    
    # Read master salary sheet
    master_sheet_df = reader.read_sheet()
    
    if master_sheet_df is not None:
        # Validate the sheet
        if reader.validate_master_sheet(master_sheet_df):
            logger.info("Master salary sheet is valid.")
            
            # Print first few rows
            logger.info("First few rows:")
            logger.info(master_sheet_df.head().to_string()) # Use to_string() for better formatting in logs
        else:
            logger.error("Master salary sheet validation failed.")
    else:
        logger.error("Failed to read master salary sheet.")
