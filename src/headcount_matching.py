# src/headcount_matching.py

import pandas as pd
from rapidfuzz import fuzz, process
from pathlib import Path
import json
import logging
import re

# Configure logging
logging.basicConfig(
    filename='headcount_matching.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


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

    Args:
        data_dir (Path): Path object pointing to the data directory.

    Returns:
        tuple: A tuple containing employees_df and assets_df DataFrames.
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

    Args:
        text (str): The text to clean.

    Returns:
        str: Cleaned text.
    """
    if pd.isna(text):
        return ""
    # Remove HTML tags
    clean = re.compile('<.*?>')
    text_no_html = re.sub(clean, '', text)
    # Remove leading/trailing whitespace and reduce multiple spaces to single
    text_clean = ' '.join(text_no_html.strip().split())
    return text_clean.lower()

def fuzzy_match(name_to_match, choices, threshold=80):
    """
    Perform fuzzy matching between a name and a list of choices using RapidFuzz.

    Args:
        name_to_match (str): The name to match against the choices.
        choices (list): A list of candidate names to match with.
        threshold (int, optional): The minimum similarity score required to consider a match. Defaults to 80.

    Returns:
        str or None: The best match if the score exceeds the threshold, otherwise None.
    """
    if not name_to_match:
        return None

    # Perform fuzzy matching using RapidFuzz
    match, score, _ = process.extractOne(name_to_match, choices, scorer=fuzz.partial_ratio)
    if score >= threshold:
        return match
    return None

def link_employees_to_assets(employees_df, assets_df):
    """
    Link employee data to assets using 'last_logged_username' and 'requester_name'.

    Args:
        employees_df (DataFrame): DataFrame containing employee data.
        assets_df (DataFrame): DataFrame containing asset data.

    Returns:
        tuple: Updated assets_df and a mapping dictionary (asset_id to employee_id).
    """
    # Clean and standardize employee full names
    employees_df['full_name_clean'] = employees_df['full_name'].apply(clean_text)

    # Create a list of employee names in lowercase for matching
    employee_names = employees_df['full_name_clean'].tolist()

    # Create a dictionary to map cleaned employee names to their employee_id
    name_to_id = dict(zip(employees_df['full_name_clean'], employees_df['employee_id']))

    # Initialize mapping dictionary
    asset_employee_map = {}

    # Initialize columns in assets_df
    assets_df['matched_employee_id'] = None
    assets_df['match_source'] = None

    # Iterate over the asset data and attempt to match using last_logged_username or requester_name
    for index, row in assets_df.iterrows():
        asset_id = row.get('asset_id')
        last_logged_username = row.get('last_logged_username', "")
        requester_name = row.get('requester_name', "")

        # Clean and standardize names
        last_logged_username_clean = clean_text(last_logged_username)
        requester_name_clean = clean_text(requester_name)

        matched_employee = None
        match_source = None

        # First, try to match based on last_logged_username (fuzzy match against full_name)
        if last_logged_username_clean:
            matched_employee = fuzzy_match(last_logged_username_clean, employee_names)
            if matched_employee:
                matched_employee_id = name_to_id.get(matched_employee)
                assets_df.at[index, 'matched_employee_id'] = matched_employee_id
                assets_df.at[index, 'match_source'] = 'last_logged_username'
                asset_employee_map[str(asset_id)] = matched_employee_id
                continue  # If a match is found, skip to the next row

        # If no match found with last_logged_username, try matching based on requester_name
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

    Args:
        assets_df (DataFrame): DataFrame containing linked asset data.
        data_dir (Path): Path object pointing to the data directory.
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

    Args:
        mapping (dict): The mapping dictionary to save.
        filename (Path): Path object pointing to the JSON file.
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

    Args:
        filename (Path): Path object pointing to the JSON file.

    Returns:
        dict: The loaded mapping dictionary.
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

# ... (previous code remains the same)

def main():
    """
    Main function to orchestrate the headcount matching and mapping process.
    """
    try:
        print("Starting headcount matching process...")

        # Define the data directory
        script_path = Path(__file__).resolve()
        data_dir = script_path.parent.parent / "data"

        # Ensure the data directory exists
        data_dir.mkdir(parents=True, exist_ok=True)
        print(f"Data directory set at {data_dir}")
        logging.info(f"Data directory set at {data_dir}")

        # Load the employee and asset data
        print("Loading employee and asset data...")
        employees_df, assets_df = load_data(data_dir)
        print(f"Loaded {len(employees_df)} employee records and {len(assets_df)} asset records.")
        logging.info("Employee and asset data loaded successfully.")

        # Link the employees to the assets and generate mapping
        print("Linking employees to assets...")
        linked_assets_df, asset_employee_map = link_employees_to_assets(employees_df, assets_df)
        print(f"Linked {len(asset_employee_map)} assets to employees.")
        logging.info("Employees linked to assets successfully.")

        # Apply data type enforcement again to ensure consistency
        linked_assets_df = enforce_data_types(linked_assets_df)

        # Save the linked data to a new CSV
        linked_data_file = data_dir / "linked_assets_data.csv"
        print(f"Saving linked data to {linked_data_file}...")
        save_linked_data(linked_assets_df, data_dir)
        print(f"Linked data saved successfully to {linked_data_file}")

        # Define the mapping filename
        mapping_filename = data_dir / "asset_employee_mapping.json"

        # Load existing mapping if it exists
        print("Loading existing mapping...")
        existing_mapping = load_mapping(mapping_filename)
        print(f"Loaded {len(existing_mapping)} existing mappings.")

        # Update the existing mapping with new mappings
        print("Updating mapping...")
        combined_mapping = {**existing_mapping, **asset_employee_map}
        print(f"Total mappings after update: {len(combined_mapping)}")

        # Save the updated mapping to JSON
        print(f"Saving updated mapping to {mapping_filename}...")
        save_mapping(combined_mapping, mapping_filename)
        print(f"Mapping saved successfully to {mapping_filename}")

        print("Headcount matching and mapping completed successfully.")
        print(f"Final outputs:")
        print(f"1. Linked assets data: {linked_data_file}")
        print(f"2. Asset-employee mapping: {mapping_filename}")
        logging.info("Headcount matching and mapping completed successfully.")

    except Exception as e:
        print(f"An error occurred during headcount matching: {e}")
        logging.error(f"An error occurred during headcount matching: {e}")

if __name__ == "__main__":
    main()
