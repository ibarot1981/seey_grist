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
            'EXCEL_FILES_DIR' # Changed from EXCEL_FILE_PATH
        ]

        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
            print("Please check your .env file")
            return

        excel_files_dir = os.getenv('EXCEL_FILES_DIR')

        if not os.path.isdir(excel_files_dir):
            print(f"Error: Excel files directory not found at {excel_files_dir}")
            return

        # List all .xlsx files in the directory
        excel_files = [f for f in os.listdir(excel_files_dir) if f.endswith('.xlsx')]

        if not excel_files:
            print(f"No .xlsx files found in {excel_files_dir}. Exiting.")
            return

        print(f"Found {len(excel_files)} Excel files to process in {excel_files_dir}")

        # Process each Excel file
        for excel_file in excel_files:
            file_path = os.path.join(excel_files_dir, excel_file)
            print(f"\nProcessing file: {file_path}")

            # Initialize Excel Reader for the current file
            excel_reader = ExcelReader(file_path=file_path)

            # Get month-year from filename
            month_year = excel_reader.get_month_year()
            if not month_year:
                print(f"Warning: Could not extract month and year from filename {excel_file}. Skipping this file.")
                continue # Skip to the next file

            print(f"Extracted month-year from filename: {month_year}")

            # Show available sheets for troubleshooting (optional, can be removed if not needed per file)
            # available_sheets = excel_reader.list_sheets()
            # print(f"Available sheets in {excel_file}: {available_sheets}")

            # Read the master salary sheet
            print("Reading master salary sheet...")
            master_sheet_df = excel_reader.read_sheet()

            if master_sheet_df is not None:
                print(f"Successfully read {len(master_sheet_df)} rows from {excel_file}")

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
                    print(f"Error: Missing required columns in Excel file {excel_file}: {missing_columns}")
                    print(f"Available columns: {master_sheet_df.columns.tolist()}")
                    continue # Skip to the next file

                # Display a sample of the data for verification (optional)
                # print(f"\nSample data from {excel_file}:")
                # print(master_sheet_df.head(3))

                # Initialize Grist Updater, passing the extracted month-year
                print("\nInitializing Grist Updater...")
                grist_updater = GristUpdater(month_year=month_year)

                # Compare and update Grist tables
                print("Starting Grist update process for this file...")
                grist_updater.compare_and_update(master_sheet_df)

                print(f"Finished processing file: {excel_file}")
            else:
                print(f"Failed to read Excel file {excel_file}. Skipping.")

        print("\nAll Excel files processed.")
    
    except Exception as e:
        import traceback
        print(f"An error occurred during update process: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
