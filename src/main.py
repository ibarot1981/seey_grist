import os
import sys
from dotenv import load_dotenv

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules (assuming they're in a src directory)
from src.excel_reader import ExcelReader
from src.grist_updater import GristUpdater

def main():
    # Load environment variables
    load_dotenv()

    try:
        print("Starting salary update process...")
        
        # Check if required environment variables are set
        required_env_vars = [
            'GRIST_API_KEY', 
            'GRIST_DOC_ID', 
            'GRIST_TABLE_NAME',
            'EXCEL_FILE_PATH'
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
            print("Please check your .env file")
            return
        
        # Initialize Excel Reader
        print("Initializing Excel Reader...")
        excel_reader = ExcelReader()
        
        # Show available sheets for troubleshooting
        available_sheets = excel_reader.list_sheets()
        print(f"Available sheets in Excel file: {available_sheets}")
        
        # Read the master salary sheet
        print("Reading master salary sheet...")
        master_sheet_df = excel_reader.read_sheet()
        
        if master_sheet_df is not None:
            print(f"Successfully read {len(master_sheet_df)} rows from Excel")
            
            # Check if required columns exist in the Excel file
            required_columns = [
                'Emp No.',
                'Salary Rate (Per Day)',
                'Emp Type : Temp / Perm',
                'Salary Calculation on Fixed / Hourly',
                'Date of Joining'
            ]
            
            missing_columns = [col for col in required_columns if col not in master_sheet_df.columns]
            
            if missing_columns:
                print(f"Error: Missing required columns in Excel file: {missing_columns}")
                print(f"Available columns: {master_sheet_df.columns.tolist()}")
                return
            
            # Display a sample of the data for verification
            print("\nSample data from Excel:")
            print(master_sheet_df.head(3))
            
            # Initialize Grist Updater
            print("\nInitializing Grist Updater...")
            grist_updater = GristUpdater()
            
            # Compare and update Grist tables
            print("Starting Grist update process...")
            grist_updater.compare_and_update(master_sheet_df)
            
            print("\nMonthly salary update process completed.")
        else:
            print("Failed to read Excel file. Exiting.")
    
    except Exception as e:
        import traceback
        print(f"An error occurred during update process: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()