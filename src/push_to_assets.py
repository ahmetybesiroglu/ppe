# src/push_to_assets.py

import os
import logging
from dotenv import load_dotenv
from pyairtable import Api
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Airtable setup
API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('SANDBOX_BASE_ID')
ASSETS_TABLE_ID = os.getenv('ASSETS_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
assets_table = airtable.table(BASE_ID, ASSETS_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def parse_date(date_string):
    if pd.isna(date_string):
        return None
    try:
        # Parse ISO 8601 format
        dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        # Convert to UTC and format as YYYY-MM-DD
        return dt.astimezone(timezone.utc).strftime('%Y-%m-%d')
    except ValueError:
        try:
            # Fallback to parsing just the date portion
            return datetime.strptime(date_string[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            logging.warning(f"Could not parse date: {date_string}")
            return None

def load_assets_data():
    """Load assets data from CSV file."""
    file_path = DATA_DIR / "assets_data_cleaned.csv"
    try:
        assets_df = pd.read_csv(file_path)
        logging.info(f"Loaded assets data from {file_path}")
        return assets_df
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Error: {e}")
        raise

def create_or_update_asset(row):
    """Create or update an asset record in Airtable."""
    fields = {
        "asset_id": str(row['asset_id']),  # Convert to string
        "name": row['name'],
        "cost": float(row['cost']) if pd.notna(row['cost']) else None,
        "description": row['description'] if pd.notna(row['description']) else None,
        "serial_number": row['serial_number'] if pd.notna(row['serial_number']) else None,
        "acquisition_date": parse_date(row['acquisition_date']),
        "created_at": parse_date(row['created_at']),
        "assigned_on": parse_date(row['assigned_on']),
        "asset_state": row['asset_state'] if pd.notna(row['asset_state']) else None,
        "display_id": str(row['display_id']) if pd.notna(row['display_id']) else None  # Add display_id field
    }

    # Remove any fields with None values
    fields = {k: v for k, v in fields.items() if v is not None}

    try:
        # Check if record already exists
        existing_records = assets_table.all(fields=['asset_id'], formula=f"{{asset_id}}='{fields['asset_id']}'")

        if existing_records:
            # Update existing record
            record_id = existing_records[0]['id']
            assets_table.update(record_id, fields)
            logging.info(f"Updated asset: {fields['name']}")
        else:
            # Create new record
            new_record = assets_table.create(fields)
            logging.info(f"Created new asset: {fields['display_id']}")
    except Exception as e:
        logging.error(f"Error creating/updating asset {row['display_id']}: {e}")

def main():
    try:
        assets_df = load_assets_data()

        for _, row in assets_df.iterrows():
            create_or_update_asset(row)

        logging.info("Assets upload completed.")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
