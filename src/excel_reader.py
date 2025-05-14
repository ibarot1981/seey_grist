import os
import pandas as pd
from dotenv import load_dotenv

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
                print(f"Error: Excel file not found at {self.file_path}")
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
                print(f"Warning: Missing columns in Excel file: {missing_columns}")
            
            return df
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            return None

    def list_sheets(self):
        """
        List all sheets in the Excel file
        
        :return: List of sheet names
        """
        try:
            if not os.path.exists(self.file_path):
                print(f"Error: Excel file not found at {self.file_path}")
                return []
                
            xls = pd.ExcelFile(self.file_path, engine='openpyxl')
            return xls.sheet_names
        except Exception as e:
            print(f"Error listing sheets: {e}")
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
            print(f"Error: Missing required columns: {missing_columns}")
            return False
            
        # Check for empty employee numbers
        if df['Emp No.'].isnull().any():
            print("Error: Some employee numbers are missing")
            return False
            
        # Check for duplicate employee numbers
        duplicates = df['Emp No.'].duplicated()
        if duplicates.any():
            print(f"Warning: Duplicate employee numbers found: {df.loc[duplicates, 'Emp No.'].tolist()}")
            
        return True

# Example usage
if __name__ == "__main__":
    reader = ExcelReader()
    
    # List all sheets
    print("Available sheets:", reader.list_sheets())
    
    # Read master salary sheet
    master_sheet_df = reader.read_sheet()
    
    if master_sheet_df is not None:
        # Validate the sheet
        if reader.validate_master_sheet(master_sheet_df):
            print("Master salary sheet is valid.")
            
            # Print first few rows
            print(master_sheet_df.head())
        else:
            print("Master salary sheet validation failed.")