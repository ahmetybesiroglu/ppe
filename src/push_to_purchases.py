# src/push_to_purchases.py

import os
import logging
from dotenv import load_dotenv
from pyairtable import Api
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Airtable setup
API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('SANDBOX_BASE_ID')
PURCHASES_TABLE_ID = os.getenv('PURCHASES_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
purchases_table = airtable.table(BASE_ID, PURCHASES_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def load_purchases_data():
    """Load purchases data from CSV file."""
    file_path = DATA_DIR / "netsuite_data_cleaned.csv"
    try:
        purchases_df = pd.read_csv(file_path)
        logging.info(f"Loaded purchases data from {file_path}")
        return purchases_df
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Error: {e}")
        raise

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
            logging.warning(f"Could not parse date {date_string}")
            return None

def create_or_update_purchase(row):
    """Create or update a purchase record in Airtable."""
    fields = {
        "purchase_id": str(row['purchase_id']),  # Convert to string
        "reference": row['reference'] if pd.notna(row['reference']) else None,
        "date": parse_date(row['date']),
        "cost": float(row['cost']) if pd.notna(row['cost']) else None,
        "description": row['description'] if pd.notna(row['description']) else None,
        "count": int(row['count']) if pd.notna(row['count']) else None,
        "note": row['note'] if pd.notna(row['note']) else None,
        "item": row['item'] if pd.notna(row['item']) else None,
    }

    # Remove any fields with None values
    fields = {k: v for k, v in fields.items() if v is not None}

    try:
        # Check if record already exists
        existing_records = purchases_table.all(fields=['purchase_id'], formula=f"{{purchase_id}}='{fields['purchase_id']}'")

        if existing_records:
            # Update existing record
            record_id = existing_records[0]['id']
            purchases_table.update(record_id, fields)
            logging.info(f"Updated purchase: {fields['purchase_id']}")
        else:
            # Create new record
            new_record = purchases_table.create(fields)
            logging.info(f"Created new purchase: {fields['purchase_id']}")
    except Exception as e:
        logging.error(f"Error creating/updating purchase: {row['purchase_id']}. Error: {e}")

def main():
    try:
        purchases_df = load_purchases_data()

        for _, row in purchases_df.iterrows():
            try:
                create_or_update_purchase(row)
            except Exception as e:
                logging.error(f"Error processing purchase {row['purchase_id']}: {str(e)}")

        logging.info("Purchases upload completed.")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
