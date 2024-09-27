# src/link_tables.py

import os
import logging
from dotenv import load_dotenv
from pyairtable import Api
import pandas as pd
from pathlib import Path

# Set up logging with a cleaner message format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Load environment variables
load_dotenv()

# Airtable setup
API_KEY = os.getenv('AIRTABLE_API_KEY')
BASE_ID = os.getenv('SANDBOX_BASE_ID')
ASSETS_TABLE_ID = os.getenv('ASSETS_TABLE_ID')
EMPLOYEES_TABLE_ID = os.getenv('EMPLOYEES_TABLE_ID')
DEPARTMENTS_TABLE_ID = os.getenv('DEPARTMENTS_TABLE_ID')
PRODUCTS_TABLE_ID = os.getenv('PRODUCTS_TABLE_ID')
VENDORS_TABLE_ID = os.getenv('VENDORS_TABLE_ID')
ASSET_TYPES_TABLE_ID = os.getenv('ASSET_TYPES_TABLE_ID')
PURCHASES_TABLE_ID = os.getenv('PURCHASES_TABLE_ID')

# Setup Airtable client
airtable = Api(API_KEY)
assets_table = airtable.table(BASE_ID, ASSETS_TABLE_ID)
employees_table = airtable.table(BASE_ID, EMPLOYEES_TABLE_ID)
departments_table = airtable.table(BASE_ID, DEPARTMENTS_TABLE_ID)
products_table = airtable.table(BASE_ID, PRODUCTS_TABLE_ID)
vendors_table = airtable.table(BASE_ID, VENDORS_TABLE_ID)
asset_types_table = airtable.table(BASE_ID, ASSET_TYPES_TABLE_ID)
purchases_table = airtable.table(BASE_ID, PURCHASES_TABLE_ID)

# Path to your data directory
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def load_data(filename):
    """Load data from CSV file."""
    file_path = DATA_DIR / filename
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Loaded data from {file_path}")
        return data
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Error: {e}")
        raise

def get_or_create_record_by_id(table, id_field, record_id, extra_fields=None):
    """Get or create a record by ID."""
    records = table.all(fields=[id_field], formula=f"{{{id_field}}}='{record_id}'")
    if records:
        return records[0]['id']
    else:
        fields = {id_field: record_id}
        if extra_fields:
            fields.update(extra_fields)
        new_record = table.create(fields)
        return new_record['id']

# Link functions
def link_asset_to_employee(assets_df):
    """Link assets to employees using matched_employee_id."""
    logging.info("Starting to link employees to assets")
    count = 0
    for _, row in assets_df.iterrows():
        if pd.notna(row['matched_employee_id']):
            employee_id = str(int(row['matched_employee_id']))
            asset_id = str(int(row['asset_id']))

            try:
                employee_airtable_id = get_or_create_record_by_id(employees_table, 'employee_id', employee_id)
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
                assets_table.update(asset_airtable_id, {'assigned_to': [employee_airtable_id]})
                logging.info(f"Linked employee '{employee_id}' to asset '{asset_id}'")
                count += 1
            except Exception as e:
                logging.error(f"Failed to link employee '{employee_id}' to asset '{asset_id}': {str(e)}")
    logging.info(f"Finished linking {count} employees to assets")

def link_employee_to_department(assets_df, departments_df):
    """Link employees to departments using the department_id from assets data."""
    logging.info("Starting to link employees to departments")
    count = 0
    for _, row in assets_df.iterrows():
        employee_id = str(int(row['matched_employee_id'])) if pd.notna(row['matched_employee_id']) else None
        department_id = str(int(row['department_id'])) if pd.notna(row['department_id']) else None

        if employee_id and department_id:
            try:
                department_airtable_id = get_or_create_record_by_id(departments_table, 'department_id', department_id)
                employee_airtable_id = get_or_create_record_by_id(employees_table, 'employee_id', employee_id)
                employees_table.update(employee_airtable_id, {'department': [department_airtable_id]})
                logging.info(f"Linked employee '{employee_id}' to department '{department_id}'")
                count += 1
            except Exception as e:
                logging.error(f"Failed to link employee '{employee_id}' to department '{department_id}': {str(e)}")
    logging.info(f"Finished linking {count} employees to departments")

def link_asset_to_product(assets_df):
    """Link assets to products using product_id."""
    logging.info("Starting to link assets to products")
    count = 0
    for _, row in assets_df.iterrows():
        if pd.notna(row['product_id']):
            product_id = str(int(row['product_id']))
            asset_id = row['asset_id']

            try:
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
                product_airtable_id = get_or_create_record_by_id(products_table, 'product_id', product_id)
                assets_table.update(asset_airtable_id, {'product': [product_airtable_id]})
                logging.info(f"Linked asset '{asset_id}' to product '{product_id}'")
                count += 1
            except Exception as e:
                logging.error(f"Failed to link asset '{asset_id}' to product '{product_id}': {str(e)}")
    logging.info(f"Finished linking {count} assets to products")

def link_asset_to_vendor(assets_df):
    """Link assets to vendors using vendor_id."""
    logging.info("Starting to link assets to vendors")
    count = 0
    for _, row in assets_df.iterrows():
        if pd.notna(row['vendor_id']):
            vendor_id = str(int(row['vendor_id']))
            asset_id = row['asset_id']

            try:
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
                vendor_airtable_id = get_or_create_record_by_id(vendors_table, 'vendor_id', vendor_id)
                assets_table.update(asset_airtable_id, {'vendor': [vendor_airtable_id]})
                logging.info(f"Linked asset '{asset_id}' to vendor '{vendor_id}'")
                count += 1
            except Exception as e:
                logging.error(f"Failed to link asset '{asset_id}' to vendor '{vendor_id}': {str(e)}")
    logging.info(f"Finished linking {count} assets to vendors")

def link_product_to_asset_type(assets_df):
    """Link products to asset types using asset_type_id."""
    logging.info("Starting to link products to asset types")
    count = 0
    for _, row in assets_df.iterrows():
        if pd.notna(row['asset_type_id']):
            asset_type_id = str(int(row['asset_type_id']))
            product_id = str(int(row['product_id']))

            try:
                product_airtable_id = get_or_create_record_by_id(products_table, 'product_id', product_id)
                asset_type_airtable_id = get_or_create_record_by_id(asset_types_table, 'asset_type_id', asset_type_id)
                products_table.update(product_airtable_id, {'asset_type': [asset_type_airtable_id]})
                logging.info(f"Linked product '{product_id}' to asset type '{asset_type_id}'")
                count += 1
            except Exception as e:
                logging.error(f"Failed to link product '{product_id}' to asset type '{asset_type_id}': {str(e)}")
    logging.info(f"Finished linking {count} products to asset types")

def link_asset_to_purchase(assets_df):
    """Link assets to purchases using asset_id and purchase_id."""
    logging.info("Starting to link assets to purchases")
    count = 0
    for _, row in assets_df.iterrows():
        asset_id = row['asset_id']
        purchase_id = row['purchase_assignment']

        if pd.notna(purchase_id) and asset_id:
            try:
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
                purchase_airtable_id = get_or_create_record_by_id(purchases_table, 'purchase_id', str(int(purchase_id)))
                assets_table.update(asset_airtable_id, {'fldUb6ku52ZBWbM6y': [purchase_airtable_id]})
                logging.info(f"Linked asset '{asset_id}' to purchase '{purchase_id}'")
                count += 1
            except Exception as e:
                logging.error(f"Failed to link asset '{asset_id}' to purchase '{purchase_id}': {str(e)}")
    logging.info(f"Finished linking {count} assets to purchases")

# Main function to link all specified entities
def main():
    logging.info("Loading assets data")
    assets_df = load_data("linked_assets_data.csv")
    departments_df = load_data("departments_data.csv")

    # Link assets to employees
    link_asset_to_employee(assets_df)

    # Link employees to departments
    link_employee_to_department(assets_df, departments_df)

    # Link assets to products
    link_asset_to_product(assets_df)

    # Link assets to vendors
    link_asset_to_vendor(assets_df)

    # Link products to asset types
    link_product_to_asset_type(assets_df)

    # Link assets to purchases
    link_asset_to_purchase(assets_df)

    logging.info("All linking operations completed.")

if __name__ == "__main__":
    main()
