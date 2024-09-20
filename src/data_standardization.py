import os
import pandas as pd
import ast
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
from collections import Counter
import re

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
    # Use regular expression to capture everything between the first '{' and the last '}'
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        return match.group(0)  # Return the matched dictionary part
    else:
        raise ValueError("No valid dictionary found in the text.")

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

        # Extract and return the response text
        response_text = extract_dict_from_text(response.choices[0].message.content.strip())
        return response_text

    except Exception as e:
        print(f"Error during GPT request: {e}")
        return None

def save_to_txt(content, output_file):
    """Save the GPT response to a txt file."""
    with open(output_file, 'w') as f:
        f.write(content)
    print(f"Mapping saved to {output_file}")

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
    """Assign a sequential 'asset_id' starting from 1."""
    df.insert(0, 'asset_id', range(1, len(df) + 1))
    return df

def clean_quotes(df, columns):
    """Remove all double quotes, including escaped ones, and strip surrounding quotes from specified columns in a DataFrame."""
    for column in columns:
        if column in df.columns:
            # Replace escaped quotes with nothing
            df[column] = df[column].str.replace(r'\"', '', regex=True)
            # Remove all non-escaped double quotes
            df[column] = df[column].str.replace(r'"', '', regex=True)
            # Strip any leading or trailing spaces or quotes
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

    # Load data
    netsuite_df, netsuite_vendor_counts = load_data(netsuite_file_path, 'vendor')
    assets_df, assets_vendor_counts = load_data(assets_file_path, 'vendor_name')

    # # Clean quotes from `item` and `product_name` columns
    # netsuite_df = clean_quotes(netsuite_df, ['item'])
    # assets_df = clean_quotes(assets_df, ['product_name'])

    ### Step 2: Process vendors ###

    # Check if the vendor mapping already exists
    if not combined_vendor_mapping_file_path.exists():
        print(f"Vendor mapping does not exist, generating it...")
        # Combine vendor counts and send to GPT
        combined_vendor_counts = combine_counts(netsuite_vendor_counts, assets_vendor_counts)
        combined_vendor_mapping = send_to_gpt_for_analysis(client, combined_vendor_counts, 'vendor')

        if combined_vendor_mapping:
            save_to_txt(combined_vendor_mapping, combined_vendor_mapping_file_path)
    else:
        print(f"Using cached vendor mapping from {combined_vendor_mapping_file_path}")

    # Load and apply vendor mapping
    vendor_mapping = load_mapping(combined_vendor_mapping_file_path)
    netsuite_df = apply_mapping_to_dataset(netsuite_df, 'vendor', vendor_mapping)
    assets_df = apply_mapping_to_dataset(assets_df, 'vendor_name', vendor_mapping)

    ### Step 3: Process asset class and asset type combined ###

    # Check if the asset class/type mapping already exists
    if not combined_asset_class_type_mapping_file_path.exists():
        print(f"Asset class/type mapping does not exist, generating it...")
        # Load asset class and asset type data
        _, assets_type_counts = load_data(assets_file_path, 'asset_type_name')
        _, netsuite_asset_class_counts = load_data(netsuite_file_path, 'asset_class')

        # Combine asset class and asset type counts and send to GPT
        combined_asset_class_type_counts = combine_counts(assets_type_counts, netsuite_asset_class_counts)
        combined_asset_class_type_mapping = send_to_gpt_for_analysis(client, combined_asset_class_type_counts, 'asset_class and asset_type_name')

        if combined_asset_class_type_mapping:
            save_to_txt(combined_asset_class_type_mapping, combined_asset_class_type_mapping_file_path)
    else:
        print(f"Using cached asset class/type mapping from {combined_asset_class_type_mapping_file_path}")

    # Load and apply asset class/type mapping to both asset_class and asset_type_name columns
    asset_class_type_mapping = load_mapping(combined_asset_class_type_mapping_file_path)
    netsuite_df = apply_mapping_to_dataset(netsuite_df, 'asset_class', asset_class_type_mapping)
    assets_df = apply_mapping_to_dataset(assets_df, 'asset_type_name', asset_class_type_mapping)

    ### Step 4: Process items and product names ###

    # Check if the item/product mapping already exists
    if not combined_item_product_mapping_file_path.exists():
        print(f"Item/Product mapping does not exist, generating it...")
        # Load item and product name data
        _, assets_product_counts = load_data(assets_file_path, 'product_name')
        _, netsuite_item_counts = load_data(netsuite_file_path, 'item')

        # Combine item and product counts and send to GPT
        combined_item_product_counts = combine_counts(assets_product_counts, netsuite_item_counts)
        combined_item_product_mapping = send_to_gpt_for_analysis(client, combined_item_product_counts, 'item and product_name')

        if combined_item_product_mapping:
            save_to_txt(combined_item_product_mapping, combined_item_product_mapping_file_path)
    else:
        print(f"Using cached item/product mapping from {combined_item_product_mapping_file_path}")

    # Load and apply item/product mapping
    item_product_mapping = load_mapping(combined_item_product_mapping_file_path)
    netsuite_df = apply_mapping_to_dataset(netsuite_df, 'item', item_product_mapping)
    assets_df = apply_mapping_to_dataset(assets_df, 'product_name', item_product_mapping)

    ### Step 5: Format date columns ###

    # Define date columns to be formatted
    date_columns = ['created_at', 'updated_at', 'acquisition_date', 'warranty_expiry_date']
    assets_df = format_dates(assets_df, date_columns)

    ### Step 6: Sort data by 'created_at' and assign 'asset_id' ###
    assets_df = assets_df.sort_values(by='created_at')
    assets_df = assign_asset_id(assets_df)

    ### Step 7: Save the cleaned datasets ###

    cleaned_netsuite_file_path = data_dir / 'netsuite_data_cleaned.csv'
    cleaned_assets_file_path = data_dir / 'assets_data_cleaned.csv'

    netsuite_df.to_csv(cleaned_netsuite_file_path, index=False)
    assets_df.to_csv(cleaned_assets_file_path, index=False)

    print(f"Cleaned Netsuite data saved to {cleaned_netsuite_file_path}")
    print(f"Cleaned Assets data saved to {cleaned_assets_file_path}")

if __name__ == '__main__':
    main()
