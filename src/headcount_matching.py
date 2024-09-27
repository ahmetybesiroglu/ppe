# src/headcount_matching.py

import pandas as pd
from rapidfuzz import fuzz, process
from pathlib import Path
import json
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def enforce_data_types(df):
    """
    Ensure that ID columns, asset tags, and other relevant fields are integers (or strings if needed),
    while preserving empty values as NaN (and not filling them with 0).
    """
    int_columns = ['purchase_id', 'asset_type_id', 'asset_id', 'vendor', 'vendor_id', 'product_id', 'display_id', 'count', 'purchase_assignment']
    str_columns = ['asset_tag', 'serial_number', 'uuid', 'vendor_name', 'product_name']

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    for col in str_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df


def load_data(data_dir):
    """
    Load the employee and asset data from CSV files.
    """
    employees_file = data_dir / "filtered_active_employees.csv"
    assets_file = data_dir / "assets_data_with_assignments.csv"

    try:
        employees_df = pd.read_csv(employees_file)
        assets_df = pd.read_csv(assets_file)
        logging.info("CSV files loaded successfully.")
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        raise
    except Exception as e:
        logging.error(f"Error loading CSV files: {e}")
        raise

    return employees_df, assets_df

def clean_text(text):
    """
    Remove HTML tags and extra whitespace from a string.
    """
    if pd.isna(text):
        return ""
    clean = re.compile('<.*?>')
    text_no_html = re.sub(clean, '', text)
    text_clean = ' '.join(text_no_html.strip().split())
    return text_clean.lower()

def fuzzy_match(name_to_match, choices, threshold=80):
    """
    Perform fuzzy matching between a name and a list of choices using RapidFuzz.
    """
    if not name_to_match:
        return None

    match, score, _ = process.extractOne(name_to_match, choices, scorer=fuzz.partial_ratio)
    if score >= threshold:
        return match
    return None

def link_employees_to_assets(employees_df, assets_df):
    """
    Link employee data to assets using 'last_logged_username' and 'requester_name'.
    """
    employees_df['full_name_clean'] = employees_df['full_name'].apply(clean_text)
    employee_names = employees_df['full_name_clean'].tolist()
    name_to_id = dict(zip(employees_df['full_name_clean'], employees_df['employee_id']))

    asset_employee_map = {}

    assets_df['matched_employee_id'] = None
    assets_df['match_source'] = None

    for index, row in assets_df.iterrows():
        asset_id = row.get('asset_id')
        last_logged_username = row.get('last_logged_username', "")
        requester_name = row.get('requester_name', "")

        last_logged_username_clean = clean_text(last_logged_username)
        requester_name_clean = clean_text(requester_name)

        matched_employee = None

        if last_logged_username_clean:
            matched_employee = fuzzy_match(last_logged_username_clean, employee_names)
            if matched_employee:
                matched_employee_id = name_to_id.get(matched_employee)
                assets_df.at[index, 'matched_employee_id'] = matched_employee_id
                assets_df.at[index, 'match_source'] = 'last_logged_username'
                asset_employee_map[str(asset_id)] = matched_employee_id
                continue

        if requester_name_clean:
            matched_employee = fuzzy_match(requester_name_clean, employee_names)
            if matched_employee:
                matched_employee_id = name_to_id.get(matched_employee)
                assets_df.at[index, 'matched_employee_id'] = matched_employee_id
                assets_df.at[index, 'match_source'] = 'requester_name'
                asset_employee_map[str(asset_id)] = matched_employee_id

    logging.info(f"Total assets matched to employees: {len(asset_employee_map)}")
    return assets_df, asset_employee_map

def save_linked_data(assets_df, data_dir):
    """
    Save the linked assets data with matched employees to a new CSV.
    """
    output_file = data_dir / "linked_assets_data.csv"
    try:
        assets_df.to_csv(output_file, index=False)
        logging.info(f"Linked data saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save linked data to {output_file}: {e}")
        raise

def save_mapping(mapping, filename):
    """
    Save a dictionary mapping to a JSON file.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(mapping, f, indent=4)
        logging.info(f"Mapping saved to {filename}")
    except Exception as e:
        logging.error(f"Failed to save mapping to {filename}: {e}")

def load_mapping(filename):
    """
    Load a dictionary mapping from a JSON file.
    """
    if not filename.exists():
        logging.warning(f"Mapping file {filename} not found. A new one will be created.")
        return {}
    try:
        with open(filename, 'r') as f:
            mapping = json.load(f)
        logging.info(f"Mapping loaded from {filename}")
        return mapping
    except Exception as e:
        logging.error(f"Failed to load mapping from {filename}: {e}")
        return {}

def main():
    """
    Main function to orchestrate the headcount matching and mapping process.
    """
    try:
        logging.info("Starting headcount matching process...")

        script_path = Path(__file__).resolve()
        data_dir = script_path.parent.parent / "data"

        data_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Data directory set at {data_dir}")

        employees_df, assets_df = load_data(data_dir)
        logging.info(f"Loaded {len(employees_df)} employee records and {len(assets_df)} asset records.")

        linked_assets_df, asset_employee_map = link_employees_to_assets(employees_df, assets_df)
        logging.info(f"Linked {len(asset_employee_map)} assets to employees.")

        linked_assets_df = enforce_data_types(linked_assets_df)
        save_linked_data(linked_assets_df, data_dir)

        mapping_filename = data_dir / "asset_employee_mapping.json"

        existing_mapping = load_mapping(mapping_filename)
        logging.info(f"Loaded {len(existing_mapping)} existing mappings.")

        combined_mapping = {**existing_mapping, **asset_employee_map}
        logging.info(f"Total mappings after update: {len(combined_mapping)}")

        save_mapping(combined_mapping, mapping_filename)
        logging.info("Headcount matching and mapping completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during headcount matching: {e}")

if __name__ == "__main__":
    main()
