import os
from dotenv import load_dotenv
from pyairtable import Api
import pandas as pd
from pathlib import Path

# Load environment variables
load_dotenv()

# Airtable setup
API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('SANDBOX_BASE_ID')
EMPLOYEES_TABLE_ID = os.getenv('EMPLOYEES_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
employees_table = airtable.table(BASE_ID, EMPLOYEES_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def load_employees_data():
    """Load employees data from CSV file."""
    file_path = DATA_DIR / "filtered_active_employees.csv"
    return pd.read_csv(file_path)

# Example mappings if needed (optional)
STATUS_MAPPING = {
    "Active": "Active",
    "Inactive": "Inactive",
    "On Leave": "On Leave"
}

EMPLOYEE_TYPE_MAPPING = {
    "Full-Time": "Full-Time",
    "Part-Time": "Part-Time",
    "Contractor": "Contractor"
}

def create_or_update_employee(row):
    """Create or update an employee record in Airtable."""

    # Map values from the CSV to valid Airtable options if necessary
    status_value = STATUS_MAPPING.get(row['status'], row['status'])
    employee_type_value = EMPLOYEE_TYPE_MAPPING.get(row['employee_type'], row['employee_type'])

    fields = {
        "employee_id": str(row['employee_id']),
        "first_name": row['first_name'],
        "last_name": row['last_name'],
        "masterworks_email": row['masterworks_email'] if pd.notna(row['masterworks_email']) else None,
        "status": status_value,  # Ensure it's one of the valid single select options
        "employee_type": employee_type_value,  # Ensure it's one of the valid single select options
        "title": row['title'],
        "position_start_date": row['position_start_date'] if pd.notna(row['position_start_date']) else None,
        "termination_date": row['termination_date'] if pd.notna(row['termination_date']) else None,
    }

    # Remove any fields with None values
    fields = {k: v for k, v in fields.items() if v is not None}

    # Check if record already exists
    existing_records = employees_table.all(fields=['employee_id'], formula=f"{{employee_id}}='{fields['employee_id']}'")

    if existing_records:
        # Update existing record
        record_id = existing_records[0]['id']
        employees_table.update(record_id, fields)
        print(f"Updated employee: {fields['first_name']} {fields['last_name']}")
    else:
        # Create new record
        new_record = employees_table.create(fields)
        print(f"Created new employee: {fields['first_name']} {fields['last_name']}")

def main():
    employees_df = load_employees_data()

    for _, row in employees_df.iterrows():
        try:
            create_or_update_employee(row)
        except Exception as e:
            print(f"Error processing employee {row['first_name']} {row['last_name']}: {str(e)}")

    print("Employees upload completed.")

if __name__ == "__main__":
    main()
