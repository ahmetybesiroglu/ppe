# src/push_to_products.py

import os
import logging
from dotenv import load_dotenv
from pyairtable import Api
import pandas as pd
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    try:
        products_df = pd.read_csv(file_path)
        logging.info(f"Loaded products data from {file_path}")
        return products_df
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Error: {e}")
        raise

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

    try:
        # Check if record already exists
        existing_records = products_table.all(fields=['product_id'], formula=f"{{product_id}}='{fields['product_id']}'")

        if existing_records:
            # Update existing record
            record_id = existing_records[0]['id']
            products_table.update(record_id, fields)
            logging.info(f"Updated product: {fields['name']}")
        else:
            # Create new record
            new_record = products_table.create(fields)
            logging.info(f"Created new product: {fields['name']}")
    except Exception as e:
        logging.error(f"Error creating/updating product: {row['name']}. Error: {e}")

def main():
    try:
        products_df = load_products_data()

        for _, row in products_df.iterrows():
            try:
                create_or_update_product(row)
            except Exception as e:
                logging.error(f"Error processing product {row['name']}: {str(e)}")

        logging.info("Products upload completed.")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
