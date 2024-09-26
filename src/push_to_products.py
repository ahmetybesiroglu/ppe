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
PRODUCTS_TABLE_ID = os.getenv('PRODUCTS_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
products_table = airtable.table(BASE_ID, PRODUCTS_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def load_products_data():
    """Load products data from CSV file."""
    file_path = DATA_DIR / "products_data.csv"
    return pd.read_csv(file_path)

def create_or_update_product(row):
    """Create or update a product record in Airtable."""
    fields = {
        "name": row['name'],
        "product_id": str(row['id']),
        "manufacturer": row['manufacturer'] if pd.notna(row['manufacturer']) else None,
        "description": row['description_text'] if pd.notna(row['description_text']) else None,
    }

    # Remove any fields with None values
    fields = {k: v for k, v in fields.items() if v is not None}

    # Check if record already exists
    existing_records = products_table.all(fields=['product_id'], formula=f"{{product_id}}='{fields['product_id']}'")

    if existing_records:
        # Update existing record
        record_id = existing_records[0]['id']
        products_table.update(record_id, fields)
        print(f"Updated product: {fields['name']}")
    else:
        # Create new record
        new_record = products_table.create(fields)
        print(f"Created new product: {fields['name']}")

def main():
    products_df = load_products_data()

    for _, row in products_df.iterrows():
        try:
            create_or_update_product(row)
        except Exception as e:
            print(f"Error processing product {row['name']}: {str(e)}")

    print("Products upload completed.")

if __name__ == "__main__":
    main()
