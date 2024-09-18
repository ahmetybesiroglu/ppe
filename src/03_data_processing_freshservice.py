# 03_data_processing_freshservice.py
# freshservice_processing.py
#
#
import pandas as pd
import ast
import re
from pathlib import Path

# Function to load the CSV file
def load_csv(file_path):
    """Load a CSV file into a pandas DataFrame, handle file not found gracefully."""
    if not file_path or not file_path.exists():
        print(f"Warning: File {file_path} does not exist. Skipping.")
        return None
    print(f"Loaded {file_path}")
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.lower()
    return df

# Function to flatten the type_fields column
def flatten_type_fields(df, column_name='type_fields'):
    def flatten_row(row):
        try:
            type_fields_dict = ast.literal_eval(row[column_name])
            return pd.Series(type_fields_dict)
        except (ValueError, SyntaxError):
            return pd.Series()

    flattened_type_fields = df.apply(flatten_row, axis=1)
    return pd.concat([df.drop(columns=[column_name]), flattened_type_fields], axis=1)

# Function to clean column names
def clean_column_names(df):
    def clean_column(col_name):
        return re.sub(r'_\d+$', '', col_name)

    df.columns = [clean_column(col) for col in df.columns]
    return df

# Individual mapping functions with pre-merge renaming
def map_departments(assets_df, departments_df):
    if departments_df is not None and 'department_id' in assets_df.columns:
        print("Mapping departments...")
        departments_df = departments_df.rename(columns={'id': 'department_id', 'name': 'department_name'})
        return assets_df.merge(departments_df[['department_id', 'department_name']], how='left', on='department_id')
    print("Warning: Missing department mapping.")
    return assets_df

def map_vendors(assets_df, vendors_df):
    if vendors_df is not None and 'vendor' in assets_df.columns:
        print("Mapping vendors...")
        vendors_df = vendors_df.rename(columns={'id': 'vendor_id', 'name': 'vendor_name'})
        return assets_df.merge(vendors_df[['vendor_id', 'vendor_name']], how='left', left_on='vendor', right_on='vendor_id')
    print("Warning: Missing vendor mapping.")
    return assets_df

def map_requesters(assets_df, requesters_df):
    if requesters_df is not None and 'user_id' in assets_df.columns:
        print("Mapping requesters...")
        if 'name' in requesters_df.columns:
            name_column = 'name'
        elif 'first_name' in requesters_df.columns and 'last_name' in requesters_df.columns:
            requesters_df['name'] = requesters_df['first_name'] + ' ' + requesters_df['last_name']
            name_column = 'name'
        else:
            print("Warning: Missing name columns in requesters data.")
            return assets_df
        requesters_df = requesters_df.rename(columns={'id': 'user_id', name_column: 'requester_name'})
        return assets_df.merge(requesters_df[['user_id', 'requester_name']], how='left', on='user_id')
    print("Warning: Missing requester mapping.")
    return assets_df

def map_asset_types(assets_df, asset_types_df):
    if asset_types_df is not None and 'asset_type_id' in assets_df.columns:
        print("Mapping asset types...")
        asset_types_df = asset_types_df.rename(columns={'id': 'asset_type_id', 'name': 'asset_type_name'})
        return assets_df.merge(asset_types_df[['asset_type_id', 'asset_type_name']], how='left', on='asset_type_id')
    print("Warning: Missing asset type mapping.")
    return assets_df

def map_filewave(assets_df, filewave_df):
    if filewave_df is not None and 'name' in filewave_df.columns:
        print("Mapping filewave data...")
        assets_df['temp_match'] = assets_df['hostname'].fillna(assets_df['name'])
        filewave_df = filewave_df.rename(columns={'name': 'filewave_name'})
        merged_df = assets_df.merge(filewave_df[['filewave_name', 'platform', 'version', 'last_logged_username', 'last_connect']],
                                    how='left', left_on='temp_match', right_on='filewave_name')
        return merged_df.drop(columns=['temp_match'])
    print("Warning: Missing filewave mapping.")
    return assets_df

def map_products(assets_df, products_df):
    if products_df is not None and 'product' in assets_df.columns:
        print("Mapping product data...")
        products_df = products_df.rename(columns={
            'id': 'product_id',
            'name': 'product_name',
            'manufacturer': 'manufacturer_name',
            'description': 'product_description',
            'description_text': 'product_description2'
        })
        return assets_df.merge(products_df[['product_id', 'product_name', 'manufacturer_name', 'product_description', 'product_description2']],
                               how='left', left_on='product', right_on='product_id')
    print("Warning: Missing product mapping.")
    return assets_df

# Function to save the DataFrame to a CSV file
def save_csv(df, output_file_path):
    df.to_csv(output_file_path, index=False)
    print(f'Cleaned and flattened CSV saved to {output_file_path}')

# Main function to process the CSV file
def process_csv(input_file_path, output_file_path, departments_file_path=None, vendors_file_path=None,
                requesters_file_path=None, asset_types_file_path=None, filewave_file_path=None,
                products_file_path=None, column_name='type_fields'):
    df = load_csv(input_file_path)
    if df is None:
        return

    df_flattened = flatten_type_fields(df, column_name)
    df_cleaned = clean_column_names(df_flattened)

    departments_df = load_csv(departments_file_path) if departments_file_path else None
    vendors_df = load_csv(vendors_file_path) if vendors_file_path else None
    requesters_df = load_csv(requesters_file_path) if requesters_file_path else None
    asset_types_df = load_csv(asset_types_file_path) if asset_types_file_path else None
    filewave_df = load_csv(filewave_file_path) if filewave_file_path else None
    products_df = load_csv(products_file_path) if products_file_path else None

    df_mapped = map_departments(df_cleaned, departments_df)
    df_mapped = map_vendors(df_mapped, vendors_df)
    df_mapped = map_requesters(df_mapped, requesters_df)
    df_mapped = map_asset_types(df_mapped, asset_types_df)
    df_mapped = map_filewave(df_mapped, filewave_df)
    df_mapped = map_products(df_mapped, products_df)

    save_csv(df_mapped, output_file_path)

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"

    input_file_path = data_dir / "assets_data.csv"
    output_file_path = data_dir / "assets_data_flattened_cleaned_mapped.csv"
    departments_file_path = data_dir / "departments_data.csv"
    vendors_file_path = data_dir / "vendors_data.csv"
    requesters_file_path = data_dir / "requesters_data.csv"
    asset_types_file_path = data_dir / "asset_types_data.csv"
    filewave_file_path = data_dir / "filewave_data.csv"
    products_file_path = data_dir / "products_data.csv"

    process_csv(
        input_file_path,
        output_file_path,
        departments_file_path,
        vendors_file_path,
        requesters_file_path,
        asset_types_file_path,
        filewave_file_path,
        products_file_path
    )
