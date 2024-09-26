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
DEPARTMENTS_TABLE_ID = os.getenv('DEPARTMENTS_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
departments_table = airtable.table(BASE_ID, DEPARTMENTS_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def load_departments_data():
    """Load departments data from CSV file."""
    file_path = DATA_DIR / "departments_data.csv"
    return pd.read_csv(file_path)

def create_or_update_department(row):
    """Create or update a department record in Airtable."""
    fields = {
        "name": row['name'],
        "department_id": str(row['id']),
        "description": row['description'] if pd.notna(row['description']) else None,
    }

    # Remove any fields with None values
    fields = {k: v for k, v in fields.items() if v is not None}

    # Check if record already exists
    existing_records = departments_table.all(fields=['department_id'], formula=f"{{department_id}}='{fields['department_id']}'")

    if existing_records:
        # Update existing record
        record_id = existing_records[0]['id']
        departments_table.update(record_id, fields)
        print(f"Updated department: {fields['name']}")
    else:
        # Create new record
        new_record = departments_table.create(fields)
        print(f"Created new department: {fields['name']}")

def main():
    departments_df = load_departments_data()

    for _, row in departments_df.iterrows():
        try:
            create_or_update_department(row)
        except Exception as e:
            print(f"Error processing department {row['name']}: {str(e)}")

    print("Departments upload completed.")

if __name__ == "__main__":
    main()
