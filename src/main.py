import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules (assuming they're in a src directory)
from src.excel_reader import ExcelReader
from src.grist_updater import GristUpdater
from src.hourclock_excel_reader import HourClockExcelReader
from src.hourclock_grist_updater import HourClockGristUpdater
from src.advances_excel_reader import AdvancesExcelReader
from src.advances_grist_updater import AdvancesGristUpdater
from src.pfesic_excel_reader import PFESICExcelReader
from src.pfescic_grist_updater import PFESICGristUpdater
from src.ot_excel_reader import OTExcelReader
from src.ot_grist_updater import OTGristUpdater
from src.salary_statement_excel_reader import SalaryStatementExcelReader
from src.salary_statement_grist_updater import SalaryStatementGristUpdater

def main():
    # Load environment variables
    load_dotenv()

    # Configure logging
    log_file = os.getenv('LOG_FILE', 'application.log')
    log_level = os.getenv('LOGGING_LEVEL', 'INFO').upper()
    max_log_size_mb = int(os.getenv('MAX_LOG_SIZE_MB', 5))
    log_backup_count = int(os.getenv('LOG_BACKUP_COUNT', 5))

    # Convert max size to bytes
    max_log_size_bytes = max_log_size_mb * 1024 * 1024

    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_log_size_bytes,
        backupCount=log_backup_count
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Create a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Add handlers to the root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Get a logger for the main script
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting salary update process...")
        
        # Check if required environment variables are set
        required_env_vars = [
            'GRIST_API_KEY',
            'GRIST_DOC_ID',
            'GRIST_TABLE_NAME',
            'EXCEL_FILES_DIR' # Changed from EXCEL_FILE_PATH
        ]

        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            logger.error("Please check your .env file")
            return

        excel_files_dir = os.getenv('EXCEL_FILES_DIR')

        if not os.path.isdir(excel_files_dir):
            logger.error(f"Excel files directory not found at {excel_files_dir}")
            return

        # List all .xlsx files in the directory
        excel_files = [f for f in os.listdir(excel_files_dir) if f.endswith('.xlsx')]

        if not excel_files:
            logger.info(f"No .xlsx files found in {excel_files_dir}. Exiting.")
            return

        logger.info(f"Found {len(excel_files)} Excel files to process in {excel_files_dir}")

        # Process each Excel file
        for excel_file in excel_files:
            file_path = os.path.join(excel_files_dir, excel_file)
            logger.info(f"\nProcessing file: {file_path}")

            # Initialize Excel Reader for the current file
            excel_reader = ExcelReader(file_path=file_path)

            # Get month-year from filename
            month_year = excel_reader.get_month_year()
            if not month_year:
                logger.warning(f"Could not extract month and year from filename {excel_file}. Skipping this file.")
                continue # Skip to the next file

            logger.info(f"Extracted month-year from filename: {month_year}")

            # Show available sheets for troubleshooting (optional, can be removed if not needed per file)
            # available_sheets = excel_reader.list_sheets()
            # logger.info(f"Available sheets in {excel_file}: {available_sheets}")

            # Read the master salary sheet
            logger.info("Reading master salary sheet...")
            master_sheet_df = excel_reader.read_sheet()

            if master_sheet_df is not None:
                logger.info(f"Successfully read {len(master_sheet_df)} rows from {excel_file}")

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
                    logger.error(f"Missing required columns in Excel file {excel_file}: {missing_columns}")
                    logger.error(f"Available columns: {master_sheet_df.columns.tolist()}")
                    continue # Skip to the next file

                # Display a sample of the data for verification (optional)
                # logger.info(f"\nSample data from {excel_file}:")
                # logger.info(master_sheet_df.head(3))

                # Initialize Grist Updater, passing the extracted month-year
                logger.info("\nInitializing Grist Updater...")
                grist_updater = GristUpdater(month_year=month_year)

                # Compare and update Grist tables
                logger.info("Starting Grist update process for this file...")
                grist_updater.compare_and_update(master_sheet_df)

                logger.info(f"Finished processing master sheet for file: {excel_file}")

            # --- Process HourClock Sheet ---
            logger.info("\nProcessing HourClock sheet...")
            hourclock_excel_reader = HourClockExcelReader(file_path=file_path)

            # Read the hour clock sheet
            hourclock_sheet_df = hourclock_excel_reader.read_sheet()

            if hourclock_sheet_df is not None:
                logger.info(f"Successfully read {len(hourclock_sheet_df)} rows from HourClock sheet in {excel_file}")

                # Initialize HourClock Grist Updater, passing the extracted month-year
                logger.info("\nInitializing HourClock Grist Updater...")
                hourclock_grist_updater = HourClockGristUpdater(month_year=month_year)

                # Compare and update Grist HC_Detail table
                logger.info("Starting HourClock Grist update process for this file...")
                hourclock_grist_updater.compare_and_update(hourclock_sheet_df)

                logger.info(f"Finished processing HourClock sheet for file: {excel_file}")
            else:
                logger.warning(f"Failed to read HourClock sheet from {excel_file}. Skipping HourClock processing for this file.")
            # --- End of HourClock Sheet Processing ---

            # --- Process Advances Sheet ---
            logger.info("\nProcessing Advances sheet...")
            advances_excel_reader = AdvancesExcelReader(file_path=file_path)

            # Read the advances sheet
            advances_sheet_df = advances_excel_reader.read_sheet()

            if advances_sheet_df is not None:
                logger.info(f"Successfully read {len(advances_sheet_df)} rows from Advances sheet in {excel_file}")

                # Initialize Advances Grist Updater, passing the extracted month-year
                logger.info("\nInitializing Advances Grist Updater...")
                advances_grist_updater = AdvancesGristUpdater(month_year=month_year)

                # Compare and update Grist Emp_Advances table
                logger.info("Starting Advances Grist update process for this file...")
                advances_grist_updater.compare_and_update(advances_sheet_df)

                logger.info(f"Finished processing Advances sheet for file: {excel_file}")
            else:
                logger.warning(f"Failed to read Advances sheet from {excel_file}. Skipping Advances processing for this file.")
            # --- End of Advances Sheet Processing ---

            # --- Process PF-ESIC Sheets ---
            logger.info("\nProcessing PF-ESIC sheets...")
            pfesic_excel_reader = PFESICExcelReader(file_path=file_path)

            # Read both PF-ESIC sheets
            pfesic_sheet_df, new_pfesic_sheet_df = pfesic_excel_reader.read_sheets()

            if pfesic_sheet_df is not None or new_pfesic_sheet_df is not None:
                if pfesic_sheet_df is not None:
                    logger.info(f"Successfully read {len(pfesic_sheet_df)} rows from PF-ESIC Sheet in {excel_file}")
                if new_pfesic_sheet_df is not None:
                    logger.info(f"Successfully read {len(new_pfesic_sheet_df)} rows from NEW PF ESIC Sheet in {excel_file}")

                # Initialize PFESIC Grist Updater, passing the extracted month-year
                logger.info("\nInitializing PF-ESIC Grist Updater...")
                pfesic_grist_updater = PFESICGristUpdater(month_year=month_year)

                # Update Grist tables
                logger.info("Starting PF-ESIC Grist update process for this file...")
                pfesic_grist_updater.update_grist_tables(pfesic_sheet_df, new_pfesic_sheet_df)

                logger.info(f"Finished processing PF-ESIC sheets for file: {excel_file}")
            else:
                logger.warning(f"Failed to read any PF-ESIC sheets from {excel_file}. Skipping PF-ESIC processing for this file.")
            # --- End of PF-ESIC Sheets Processing ---

            # --- Process OT Sheet ---
            logger.info("\nProcessing OT sheet...")
            ot_excel_reader = OTExcelReader(file_path=file_path)

            # Read the OT sheet
            ot_sheet_df = ot_excel_reader.read_sheet()

            if ot_sheet_df is not None:
                logger.info(f"Successfully read {len(ot_sheet_df)} rows from OT sheet in {excel_file}")

                # Initialize OT Grist Updater, passing the extracted month-year
                logger.info("\nInitializing OT Grist Updater...")
                ot_grist_updater = OTGristUpdater(month_year=month_year)

                # Compare and update Grist Emp_Dump_OT2 table
                logger.info("Starting OT Grist update process for this file...")
                ot_grist_updater.compare_and_update(ot_sheet_df)

                logger.info(f"Finished processing OT sheet for file: {excel_file}")
            else:
                logger.warning(f"Failed to read OT sheet from {excel_file}. Skipping OT processing for this file.")
            # --- End of OT Sheet Processing ---

            # --- Process Salary Statement Sheet ---
            logger.info("\nProcessing Salary Statement sheet...")
            salary_statement_excel_reader = SalaryStatementExcelReader(file_path=file_path)

            # Read the Salary Statement sheet
            salary_statement_sheet_df = salary_statement_excel_reader.read_sheet()

            if salary_statement_sheet_df is not None:
                logger.info(f"Successfully read {len(salary_statement_sheet_df)} rows from Salary Statement sheet in {excel_file}")

                # Initialize Salary Statement Grist Updater, passing the extracted month-year
                logger.info("\nInitializing Salary Statement Grist Updater...")
                salary_statement_grist_updater = SalaryStatementGristUpdater(month_year=month_year)

                # Process and update Grist Emp_Dump_SS table
                logger.info("Starting Salary Statement Grist update process for this file...")
                salary_statement_grist_updater.process_excel_data(salary_statement_sheet_df)

                logger.info(f"Finished processing Salary Statement sheet for file: {excel_file}")
            else:
                logger.warning(f"Failed to read Salary Statement sheet from {excel_file}. Skipping Salary Statement processing for this file.")
            # --- End of Salary Statement Sheet Processing ---

        logger.info("\nAll Excel files processed.")
    
    except Exception as e:
        import traceback
        logger.error(f"An error occurred during update process: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
