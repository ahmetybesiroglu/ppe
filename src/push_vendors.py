import os
import ast
from dotenv import load_dotenv
from pyairtable import Api
import pandas as pd
from pathlib import Path

# Load environment variables
load_dotenv()

# Airtable setup
API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('SANDBOX_BASE_ID')
VENDORS_TABLE_ID = os.getenv('VENDORS_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
vendors_table = airtable.table(BASE_ID, VENDORS_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def load_vendors_data():
    """Load vendors data from CSV file."""
    file_path = DATA_DIR / "vendors_data.csv"
    return pd.read_csv(file_path)

def parse_address(address_str):
    """Parse the address string into a dictionary."""
    try:
        return ast.literal_eval(address_str)
    except:
        return {}

def create_or_update_vendor(row):
    """Create or update a vendor record in Airtable."""
    address = parse_address(row['address'])

    fields = {
        "name": row['name'],
        "vendor_id": str(row['id']),
        "contact_name": row['contact_name'] if pd.notna(row['contact_name']) else None,
        "email": row['email'] if pd.notna(row['email']) else None,
        "mobile": row['mobile'] if pd.notna(row['mobile']) else None,
        "address": ', '.join(filter(None, [
            address.get('line1'),
            address.get('city'),
            address.get('state'),
            address.get('country'),
            address.get('zipcode')
        ]))
    }

    # Remove any fields with None or empty string values
    fields = {k: v for k, v in fields.items() if v not in (None, '')}

    # Check if record already exists
    existing_records = vendors_table.all(fields=['vendor_id'], formula=f"{{vendor_id}}='{fields['vendor_id']}'")

    if existing_records:
        # Update existing record
        record_id = existing_records[0]['id']
        vendors_table.update(record_id, fields)
        print(f"Updated vendor: {fields['name']}")
    else:
        # Create new record
        new_record = vendors_table.create(fields)
        print(f"Created new vendor: {fields['name']}")

def main():
    vendors_df = load_vendors_data()

    for _, row in vendors_df.iterrows():
        try:
            create_or_update_vendor(row)
        except Exception as e:
            print(f"Error processing vendor {row['name']}: {str(e)}")

    print("Vendors upload completed.")

if __name__ == "__main__":
    main()
