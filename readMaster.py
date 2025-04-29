import pandas as pd

# Change this to your actual Excel file path
excel_file_path = 'monthly.xlsx'

# Name of the master sheet
master_sheet_name = 'MasterSalarySheet'  # Adjust if different

# Output CSV file path
output_csv_path = 'employee_master_output.csv'

def read_and_save_master_sheet(excel_path, sheet_name, csv_output_path):
    try:
        # Read the specified sheet from the Excel file
        df = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')

        # Save to CSV
        df.to_csv(csv_output_path, index=False)
        print(f"Master sheet successfully saved to: {csv_output_path}")
    except Exception as e:
        print(f"Error: {e}")

# Run the function
read_and_save_master_sheet(excel_file_path, master_sheet_name, output_csv_path)
