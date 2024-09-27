# src/push_to_asset_types.py

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
ASSET_TYPES_TABLE_ID = os.getenv('ASSET_TYPES_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
asset_types_table = airtable.table(BASE_ID, ASSET_TYPES_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Dictionary to store mapping between asset_type_id and Airtable record ID
id_mapping = {}

def load_asset_types_data():
    """Load asset types data from CSV file."""
    file_path = DATA_DIR / "asset_types_data.csv"
    try:
        asset_types_df = pd.read_csv(file_path)
        logging.info(f"Loaded asset types data from {file_path}")
        return asset_types_df
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Error: {e}")
        raise

def create_or_update_asset_type(row):
    """Create or update an asset type record in Airtable."""
    fields = {
        "name": row['name'],
        "asset_type_id": str(row['id']),
        "note": row['description'] if pd.notna(row['description']) else None,
    }

    try:
        # Check if record already exists
        existing_records = asset_types_table.all(fields=['asset_type_id'], formula=f"{{asset_type_id}}='{fields['asset_type_id']}'")

        if existing_records:
            # Update existing record
            record_id = existing_records[0]['id']
            asset_types_table.update(record_id, fields)
            logging.info(f"Updated asset type: {fields['name']}")
            id_mapping[str(row['id'])] = record_id
        else:
            # Create new record
            new_record = asset_types_table.create(fields)
            logging.info(f"Created new asset type: {fields['name']}")
            id_mapping[str(row['id'])] = new_record['id']
    except Exception as e:
        logging.error(f"Error creating/updating asset type: {row['name']}. Error: {e}")

def update_parent_links(asset_types_df):
    """Update parent asset type links after all records are created."""
    for _, row in asset_types_df.iterrows():
        if pd.notna(row['parent_asset_type_id']):
            parent_id = str(row['parent_asset_type_id']).split('.')[0]  # Remove decimal point if present
            if parent_id in id_mapping:
                fields = {
                    "parent_asset_type": [id_mapping[parent_id]]
                }
                try:
                    asset_types_table.update(id_mapping[str(row['id'])], fields)
                    logging.info(f"Updated parent link for asset type: {row['name']}")
                except Exception as e:
                    logging.error(f"Error updating parent link for asset type: {row['name']}. Error: {e}")

def main():
    try:
        asset_types_df = load_asset_types_data()

        # First pass: create or update all records without parent links
        for _, row in asset_types_df.iterrows():
            create_or_update_asset_type(row)

        # Second pass: update parent links
        update_parent_links(asset_types_df)

        logging.info("Asset types upload completed.")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
