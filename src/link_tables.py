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
    return pd.read_csv(file_path)

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
    for _, row in assets_df.iterrows():
        if pd.notna(row['matched_employee_id']):
            employee_id = str(int(row['matched_employee_id'])) if pd.notna(row['matched_employee_id']) else None
            asset_id = str(int(row['asset_id'])) if pd.notna(row['asset_id']) else None

            if employee_id and asset_id:
                employee_airtable_id = get_or_create_record_by_id(employees_table, 'employee_id', employee_id)
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)

                try:
                    assets_table.update(asset_airtable_id, {'assigned_to': [employee_airtable_id]})
                    print(f"Linked employee {employee_id} to asset {asset_id}")
                except Exception as e:
                    print(f"Error linking employee {employee_id} to asset {asset_id}: {str(e)}")
            else:
                print(f"Invalid employee_id or asset_id for asset: {row['asset_id']}")

def link_employee_to_department(assets_df, departments_df):
    """Link employees to departments using the department_id from assets data."""
    for _, row in assets_df.iterrows():
        employee_id = str(int(row['matched_employee_id'])) if pd.notna(row['matched_employee_id']) else None
        department_id = str(int(row['department_id'])) if pd.notna(row['department_id']) else None

        if employee_id and department_id:
            # Find the department record by department_id in the departments dataframe
            matching_department = departments_df[departments_df['id'] == int(department_id)]

            if not matching_department.empty:
                # If department is found, link the employee to the department
                department_airtable_id = get_or_create_record_by_id(departments_table, 'department_id', department_id)
                employee_airtable_id = get_or_create_record_by_id(employees_table, 'employee_id', employee_id)

                try:
                    employees_table.update(employee_airtable_id, {'department': [department_airtable_id]})
                    print(f"Linked department '{department_id}' to employee {employee_id}")
                except Exception as e:
                    print(f"Error linking department '{department_id}' to employee {employee_id}: {str(e)}")
            else:
                # If no matching department, create a new department and link it
                new_department_data = {
                    'id': department_id,
                    'name': row['department_name'] if pd.notna(row['department_name']) else f"Department {department_id}"
                }
                department_airtable_id = get_or_create_record_by_id(departments_table, 'department_id', department_id, extra_fields=new_department_data)
                employee_airtable_id = get_or_create_record_by_id(employees_table, 'employee_id', employee_id)

                try:
                    employees_table.update(employee_airtable_id, {'department': [department_airtable_id]})
                    print(f"Created and linked new department '{department_id}' to employee {employee_id}")
                except Exception as e:
                    print(f"Error linking new department '{department_id}' to employee {employee_id}: {str(e)}")
        else:
            print(f"Invalid department or employee_id for asset: {row['asset_id']}")

def link_asset_to_product(assets_df):
    """Link assets to products using product_id."""
    for _, row in assets_df.iterrows():
        if pd.notna(row['product_id']):
            product_id = str(int(row['product_id'])) if pd.notna(row['product_id']) else None
            asset_id = row['asset_id']

            if product_id and asset_id:
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
                product_airtable_id = get_or_create_record_by_id(products_table, 'product_id', product_id)

                try:
                    assets_table.update(asset_airtable_id, {'product': [product_airtable_id]})
                    print(f"Linked asset {asset_id} to product {product_id}")
                except Exception as e:
                    print(f"Error linking asset {asset_id} to product {product_id}: {str(e)}")
            else:
                print(f"Invalid asset or product_id for asset: {asset_id}")

def link_asset_to_vendor(assets_df):
    """Link assets to vendors using vendor_id."""
    for _, row in assets_df.iterrows():
        if pd.notna(row['vendor_id']):
            vendor_id = str(int(row['vendor_id'])) if pd.notna(row['vendor_id']) else None
            asset_id = row['asset_id']

            if vendor_id and asset_id:
                asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
                vendor_airtable_id = get_or_create_record_by_id(vendors_table, 'vendor_id', vendor_id)

                try:
                    assets_table.update(asset_airtable_id, {'vendor': [vendor_airtable_id]})
                    print(f"Linked asset {asset_id} to vendor {vendor_id}")
                except Exception as e:
                    print(f"Error linking asset {asset_id} to vendor {vendor_id}: {str(e)}")
            else:
                print(f"Invalid asset or vendor_id for asset: {asset_id}")

def link_product_to_asset_type(assets_df):
    """Link products to asset types using asset_type_id."""
    for _, row in assets_df.iterrows():
        if pd.notna(row['asset_type_id']):
            asset_type_id = str(int(row['asset_type_id'])) if pd.notna(row['asset_type_id']) else None
            product_id = str(int(row['product_id'])) if pd.notna(row['product_id']) else None

            if asset_type_id and product_id:
                product_airtable_id = get_or_create_record_by_id(products_table, 'product_id', product_id)
                asset_type_airtable_id = get_or_create_record_by_id(asset_types_table, 'asset_type_id', asset_type_id)

                try:
                    products_table.update(product_airtable_id, {'asset_type': [asset_type_airtable_id]})
                    print(f"Linked product {product_id} to asset type {asset_type_id}")
                except Exception as e:
                    print(f"Error linking product {product_id} to asset type {asset_type_id}: {str(e)}")
            else:
                print(f"Invalid product or asset_type_id for product: {product_id}")

def link_asset_to_purchase(assets_df):
    """Link assets to purchases using asset_id and purchase_id."""
    for _, row in assets_df.iterrows():
        asset_id = row['asset_id']
        purchase_id = row['purchase_assignment']  # This field corresponds to purchase_id in your CSV

        if pd.notna(purchase_id) and asset_id:
            # Get or create the record in Airtable for the asset and purchase
            asset_airtable_id = get_or_create_record_by_id(assets_table, 'asset_id', asset_id)
            purchase_airtable_id = get_or_create_record_by_id(purchases_table, 'purchase_id', str(int(purchase_id)))

            try:
                # Update the asset record to link it to the correct purchase in Airtable
                # Replace 'purchase_id' with the correct field ID 'fldUb6ku52ZBWbM6y'
                assets_table.update(asset_airtable_id, {'fldUb6ku52ZBWbM6y': [purchase_airtable_id]})
                print(f"Linked asset {asset_id} to purchase {purchase_id}")
            except Exception as e:
                print(f"Error linking asset {asset_id} to purchase {purchase_id}: {str(e)}")
        else:
            print(f"Invalid asset or purchase_id for asset: {asset_id}")




# Main function to link all specified entities

def main():
    # Load the necessary data from CSV files
    assets_df = load_data("linked_assets_data.csv")
    employees_df = load_data("filtered_active_employees.csv")
    departments_df = load_data("departments_data.csv")  # Load the departments data
    products_df = load_data("products_data.csv")

    # # Link assets to employees
    # link_asset_to_employee(assets_df)

    # # # Link employees to departments
    # link_employee_to_department(assets_df, departments_df)  # Pass the departments data

    # # # Link assets to products
    # link_asset_to_product(assets_df)

    # # # Link assets to vendors
    # link_asset_to_vendor(assets_df)

    # # Link products to asset types
    link_product_to_asset_type(assets_df)

    # # Link assets to purchases
    # link_asset_to_purchase(assets_df)

    print("All specified links completed.")

if __name__ == "__main__":
    main()
