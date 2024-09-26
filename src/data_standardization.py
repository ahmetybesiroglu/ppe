# src/data_standardization.py
import os
import pandas as pd
import ast
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
from collections import Counter
import re
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def enforce_data_types(df):
    """
    Ensure that ID columns, asset tags, and other relevant fields are integers (or strings if needed),
    while preserving empty values as NaN (and not filling them with 0).
    """
    # Define columns that should be integers or strings
    int_columns = ['purchase_id', 'asset_type_id', 'asset_id', 'vendor_id', 'product_id', 'display_id', 'count']
    str_columns = ['asset_tag', 'serial_number', 'uuid', 'vendor_name', 'product_name']

    # Ensure columns that should be integers are integers but keep NaNs as NaNs
    for col in int_columns:
        if col in df.columns:
            # Convert to numeric, allowing NaNs, and then to Int64 to support NaNs
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Ensure columns that should be strings are strings
    for col in str_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df

def load_env_variables():
    """Load environment variables from the .env file."""
    load_dotenv()
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY is not set. Please check your .env file.")
    return openai_api_key

def load_data(file_path, column):
    """Load the dataset and focus on the specified column."""
    df = pd.read_csv(file_path)
    column_counts = df[column].value_counts().to_dict()
    return df, column_counts

def combine_counts(*counts_dicts):
    """Combine counts from multiple dictionaries."""
    combined_counts = Counter()
    for counts in counts_dicts:
        combined_counts.update(counts)
    return dict(combined_counts)

def extract_dict_from_text(text):
    """Extract and return only the dictionary part from a string containing extra text."""
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        return match.group(0)  # Return the matched dictionary part
    else:
        raise ValueError("No valid dictionary found in the text.")

def consolidate_duplicate_columns(df, base_column, method='sum'):
    """
    Consolidate duplicate columns (e.g., 'memory', 'os_version', etc.) into a single column.
    The strategy used here is specified by the 'method' parameter.
    Supported methods: 'sum', 'max', 'first_non_null'
    """
    pattern = rf'^{re.escape(base_column)}(?:\.\d+)?$'
    duplicate_columns = [col for col in df.columns if re.match(pattern, col)]

    logging.info(f"Found duplicate columns for '{base_column}': {duplicate_columns}")

    if len(duplicate_columns) <= 1:
        logging.info(f"No duplicate columns found for '{base_column}'. No consolidation needed.")
        return df

    logging.info(f"Consolidating columns {duplicate_columns} into '{base_column}' using method '{method}'.")

    if method == 'sum':
        df[base_column] = df[duplicate_columns].fillna(0).sum(axis=1)
    elif method == 'max':
        if pd.api.types.is_numeric_dtype(df[duplicate_columns].dtypes.iloc[0]):
            df[base_column] = df[duplicate_columns].fillna(0).max(axis=1)
        else:
            df[base_column] = df[duplicate_columns].bfill(axis=1).iloc[:, 0].infer_objects()
    elif method == 'first_non_null':
        df[base_column] = df[duplicate_columns].bfill(axis=1).iloc[:, 0]
    else:
        raise ValueError(f"Unsupported consolidation method: {method}")

    if df[base_column].isnull().all():
        logging.warning(f"Consolidation resulted in an empty '{base_column}' column. Retaining original columns.")
        return df

    columns_to_drop = [col for col in duplicate_columns if col != base_column]
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    logging.info(f"Consolidated '{base_column}' and dropped original duplicate columns: {columns_to_drop}.")
    return df

def send_to_gpt_for_analysis(client, counts, column_name):
    """Send the combined counts to GPT for standardization."""
    system_prompt = f"""
    You are a helpful assistant who standardizes text data.
    Below is a list of {column_name} from multiple datasets along with their counts.
    Please analyze this and provide a dictionary to map these values to standardized names. When you provide your answer, only provide the dictionary.
    """

    cleaned_content = f"{counts}"

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": cleaned_content}
            ],
            temperature=0,
            max_tokens=2000,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )

        response_text = extract_dict_from_text(response.choices[0].message.content.strip())
        return response_text

    except Exception as e:
        logging.error(f"Error during GPT request: {e}")
        return None

def save_to_txt(content, output_file):
    """Save the GPT response to a txt file."""
    with open(output_file, 'w') as f:
        f.write(content)
    logging.info(f"Mapping saved to {output_file}")

def load_mapping(file_path):
    """Load the mapping from a .txt file."""
    with open(file_path, 'r') as f:
        mapping_str = f.read().strip()
        mapping_dict = ast.literal_eval(mapping_str)  # Safely evaluate the string as a dictionary
    return mapping_dict

def apply_mapping_to_dataset(df, column, mapping):
    """Apply the mapping to the dataset."""
    df[column] = df[column].map(mapping).fillna(df[column])
    return df

def format_dates(df, date_columns):
    """Ensure date columns are formatted as YYYY-MM-DD."""
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d')
    return df

def assign_asset_id(df):
    """Rename 'id' column to 'asset_id'."""
    df.rename(columns={'id': 'asset_id'}, inplace=True)
    return df

def clean_quotes(df, columns):
    """Remove all double quotes, including escaped ones, and strip surrounding quotes from specified columns in a DataFrame."""
    for column in columns:
        if column in df.columns:
            df[column] = df[column].str.replace(r'\"', '', regex=True)
            df[column] = df[column].str.replace(r'"', '', regex=True)
            df[column] = df[column].str.strip()
    return df

def main():
    # Load environment variables
    openai_api_key = load_env_variables()

    # Initialize OpenAI client
    client = OpenAI(api_key=openai_api_key)

    # Define file paths and columns
    data_dir = Path('data')
    netsuite_file_path = data_dir / 'netsuite_data.csv'
    assets_file_path = data_dir / 'assets_data_flattened_cleaned_mapped.csv'

    # File paths for saved mappings
    combined_vendor_mapping_file_path = data_dir / 'combined_vendor_mapping.txt'
    combined_asset_class_type_mapping_file_path = data_dir / 'combined_asset_class_type_mapping.txt'
    combined_item_product_mapping_file_path = data_dir / 'combined_item_product_mapping.txt'

    ### Step 1: Load and clean data ###
    try:
        netsuite_df, netsuite_vendor_counts = load_data(netsuite_file_path, 'vendor')
        assets_df, assets_vendor_counts = load_data(assets_file_path, 'vendor_name')
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return

    # Consolidate 'memory' columns
    try:
        assets_df = consolidate_duplicate_columns(assets_df, 'memory', method='max')
    except Exception as e:
        logging.error(f"Error consolidating 'memory' columns: {e}")
        return

    # Consolidate 'os' columns
    try:
        assets_df = consolidate_duplicate_columns(assets_df, 'os', method='max')
    except Exception as e:
        logging.error(f"Error consolidating 'os' columns: {e}")
        return

    # Consolidate 'os_version' columns
    try:
        assets_df = consolidate_duplicate_columns(assets_df, 'os_version', method='max')
    except Exception as e:
        logging.error(f"Error consolidating 'os_version' columns: {e}")
        return

    ### Step 2: Process vendors ###
    if not combined_vendor_mapping_file_path.exists():
        logging.info(f"Vendor mapping does not exist, generating it...")
        combined_vendor_counts = combine_counts(netsuite_vendor_counts, assets_vendor_counts)
        combined_vendor_mapping = send_to_gpt_for_analysis(client, combined_vendor_counts, 'vendor')

        if combined_vendor_mapping:
            save_to_txt(combined_vendor_mapping, combined_vendor_mapping_file_path)
    else:
        logging.info(f"Using cached vendor mapping from {combined_vendor_mapping_file_path}")

    try:
        vendor_mapping = load_mapping(combined_vendor_mapping_file_path)
        netsuite_df = apply_mapping_to_dataset(netsuite_df, 'vendor', vendor_mapping)
        assets_df = apply_mapping_to_dataset(assets_df, 'vendor_name', vendor_mapping)
        logging.info("Vendor mapping applied successfully.")
    except Exception as e:
        logging.error(f"Error applying vendor mapping: {e}")

    ### Step 3: Process asset class and asset type combined ###
    if not combined_asset_class_type_mapping_file_path.exists():
        logging.info(f"Asset class/type mapping does not exist, generating it...")
        try:
            _, assets_type_counts = load_data(assets_file_path, 'asset_type_name')
            _, netsuite_asset_class_counts = load_data(netsuite_file_path, 'asset_class')
        except Exception as e:
            logging.error(f"Error loading asset class/type data: {e}")
            return

        combined_asset_class_type_counts = combine_counts(assets_type_counts, netsuite_asset_class_counts)
        combined_asset_class_type_mapping = send_to_gpt_for_analysis(client, combined_asset_class_type_counts, 'asset_class and asset_type_name')

        if combined_asset_class_type_mapping:
            save_to_txt(combined_asset_class_type_mapping, combined_asset_class_type_mapping_file_path)
    else:
        logging.info(f"Using cached asset class/type mapping from {combined_asset_class_type_mapping_file_path}")

    try:
        asset_class_type_mapping = load_mapping(combined_asset_class_type_mapping_file_path)
        netsuite_df = apply_mapping_to_dataset(netsuite_df, 'asset_class', asset_class_type_mapping)
        assets_df = apply_mapping_to_dataset(assets_df, 'asset_type_name', asset_class_type_mapping)
        logging.info("Asset class/type mapping applied successfully.")
    except Exception as e:
        logging.error(f"Error applying asset class/type mapping: {e}")

    ### Step 4: Process items and product names ###
    if not combined_item_product_mapping_file_path.exists():
        logging.info(f"Item/Product mapping does not exist, generating it...")
        try:
            _, assets_product_counts = load_data(assets_file_path, 'product_name')
            _, netsuite_item_counts = load_data(netsuite_file_path, 'item')
        except Exception as e:
            logging.error(f"Error loading item/product data: {e}")
            return

        combined_item_product_counts = combine_counts(assets_product_counts, netsuite_item_counts)
        combined_item_product_mapping = send_to_gpt_for_analysis(client, combined_item_product_counts, 'item and product_name')

        if combined_item_product_mapping:
            save_to_txt(combined_item_product_mapping, combined_item_product_mapping_file_path)
    else:
        logging.info(f"Using cached item/product mapping from {combined_item_product_mapping_file_path}")

    try:
        item_product_mapping = load_mapping(combined_item_product_mapping_file_path)
        netsuite_df = apply_mapping_to_dataset(netsuite_df, 'item', item_product_mapping)
        assets_df = apply_mapping_to_dataset(assets_df, 'product_name', item_product_mapping)
        logging.info("Item/Product mapping applied successfully.")
    except Exception as e:
        logging.error(f"Error applying item/product mapping: {e}")

    ### Step 5: Format date columns ###
    date_columns = ['created_at', 'updated_at', 'acquisition_date', 'warranty_expiry_date']
    try:
        assets_df = format_dates(assets_df, date_columns)
        logging.info("Date columns formatted successfully.")
    except Exception as e:
        logging.error(f"Error formatting date columns: {e}")

    ### Step 6: Sort data by 'created_at' and assign 'asset_id' ###
    try:
        assets_df = assets_df.sort_values(by='created_at')
        assets_df = assign_asset_id(assets_df)
        logging.info("Data sorted and 'asset_id' assigned successfully.")
    except Exception as e:
        logging.error(f"Error sorting data or assigning 'asset_id': {e}")

    ### Step 7: Enforce correct data types ###
    try:
        netsuite_df = enforce_data_types(netsuite_df)
        assets_df = enforce_data_types(assets_df)
        logging.info("Data types enforced successfully.")
    except Exception as e:
        logging.error(f"Error enforcing data types: {e}")

    ### Step 8: Save the cleaned datasets ###
    try:
        cleaned_netsuite_file_path = data_dir / 'netsuite_data_cleaned.csv'
        cleaned_assets_file_path = data_dir / 'assets_data_cleaned.csv'

        netsuite_df.to_csv(cleaned_netsuite_file_path, index=False)
        assets_df.to_csv(cleaned_assets_file_path, index=False)

        logging.info(f"Cleaned Netsuite data saved to {cleaned_netsuite_file_path}")
        logging.info(f"Cleaned Assets data saved to {cleaned_assets_file_path}")
    except Exception as e:
        logging.error(f"Error saving cleaned data: {e}")

if __name__ == '__main__':
    main()
