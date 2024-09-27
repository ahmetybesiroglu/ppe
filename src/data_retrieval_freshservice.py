# src/data_retrieval_freshservice.py

import os
import requests
import pandas as pd
from dotenv import load_dotenv
import base64
from pathlib import Path
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_env_variables():
    """Load environment variables from the .env file."""
    load_dotenv()
    env_vars = {
        'FRESHSERVICE_DOMAIN': os.getenv('FRESHSERVICE_DOMAIN'),
        'FRESHSERVICE_API_KEY': os.getenv('FRESHSERVICE_API_KEY')
    }

    # Check if necessary environment variables are loaded
    if not env_vars['FRESHSERVICE_DOMAIN']:
        logging.error("Missing Freshservice domain. Please check your .env file.")
        raise ValueError("FRESHSERVICE_DOMAIN is not set. Please check your .env file.")

    if not env_vars['FRESHSERVICE_API_KEY']:
        logging.error("Missing Freshservice API key. Please check your .env file.")
        raise ValueError("FRESHSERVICE_API_KEY is not set. Please check your .env file.")

    logging.info("Environment variables loaded successfully.")
    return env_vars

# Ensure 'data' folder exists
def ensure_data_dir():
    """Ensure the 'data' folder exists."""
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Data directory is set up at {data_dir}")
    return data_dir

# Configure retries with backoff
def configure_retry_session(retries=5, backoff_factor=1, status_forcelist=(502, 503, 504)):
    """Create a session with retry behavior for robust data fetching."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    logging.info("Retry session configured for API requests.")
    return session

# Create headers for API request
def create_headers(api_key):
    """Generate headers for API requests."""
    auth_header_value = base64.b64encode(f"{api_key}:X".encode()).decode()
    logging.info("API request headers created.")
    return {
        'Authorization': f'Basic {auth_header_value}',
        'Content-Type': 'application/json'
    }

# Convert column names to snake_case
def convert_columns_to_snake_case(df):
    """Convert DataFrame column names to snake_case."""
    df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_').str.lower()
    logging.info("Column names converted to snake_case.")
    return df

# Fetch paginated data with dynamic response key detection
def fetch_paginated_data(url, headers, session):
    """Fetch paginated data from a specific URL."""
    all_data = []
    page = 1
    while True:
        paginated_url = f"{url}&page={page}"
        logging.info(f"Fetching page {page} from Freshservice API...")
        try:
            response = session.get(paginated_url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Find the first key that contains a list of records
            response_key = next((key for key, value in data.items() if isinstance(value, list)), None)

            if not response_key or not data[response_key]:
                logging.info(f"No more records found. Total pages fetched: {page-1}")
                break

            all_data.extend(data[response_key])
            page += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data from page {page}: {e}")
            break

    logging.info(f"Total records fetched: {len(all_data)}")
    return all_data

# Save data to CSV with merging
def save_to_csv(new_data, filename, data_dir):
    """Save new data to CSV and merge with existing data if needed."""
    csv_path = data_dir / filename
    if csv_path.exists():
        logging.info(f"File {filename} already exists. Merging new data...")
        existing_df = pd.read_csv(csv_path)
        if new_data:
            new_df = pd.DataFrame(new_data)
            new_df = convert_columns_to_snake_case(new_df)
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates().reset_index(drop=True)
            combined_df.to_csv(csv_path, index=False)
            logging.info(f"Data merged and saved to {csv_path}")
            return combined_df
        else:
            logging.info(f"No new data to merge for {filename}.")
            return existing_df
    else:
        if new_data:
            new_df = pd.DataFrame(new_data)
            new_df = convert_columns_to_snake_case(new_df)
            new_df.to_csv(csv_path, index=False)
            logging.info(f"New data saved to {csv_path}")
            return new_df
        else:
            logging.warning(f"No data available for {filename}.")
            return None

# Download data from a specified endpoint
def download_data(endpoint, filename, base_url, headers, data_dir, session):
    """Download data from an API endpoint and save to CSV."""
    csv_path = data_dir / filename
    if csv_path.exists():
        logging.info(f"Loading existing data from {filename}.")
        return pd.read_csv(csv_path)

    # Determine whether to add query parameters
    if '?' in endpoint:
        url = base_url + f"{endpoint}&per_page=30"
    else:
        url = base_url + f"{endpoint}?per_page=30"

    new_data = fetch_paginated_data(url, headers, session)
    return save_to_csv(new_data, filename, data_dir)

# Fetch additional data (e.g., components, requests, contracts)
def fetch_additional_data(display_id, data_types, base_url, headers, session):
    """Fetch additional asset-related data."""
    additional_data = {}

    for data_type in data_types:
        url = f"{base_url}assets/{display_id}/{data_type}"
        try:
            data = fetch_data_from_url(url, display_id, headers, session)
            additional_data[data_type] = data if data else None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {data_type} for asset {display_id}: {e}")
            additional_data[data_type] = None

    return additional_data

# General function to fetch data from a URL
def fetch_data_from_url(url, display_id, headers, session):
    """Fetch data from a specific URL."""
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.SSLError as e:
        logging.error(f"SSL error for asset {display_id}: {e}")
        return None
    except requests.exceptions.RequestException as err:
        logging.error(f"Error fetching data for asset {display_id}: {err}")
        return None

# Create a unified DataFrame for all assets and their additional data
def create_unified_dataframe(asset_df, data_types, base_url, headers, session):
    """Create a unified DataFrame for all assets with additional data."""
    asset_data = []
    data_dir = ensure_data_dir()

    unified_filename = 'assets_data_associates.csv'
    unified_file_path = data_dir / unified_filename
    if unified_file_path.exists():
        logging.info(f"Unified data file {unified_filename} already exists. Loading data.")
        return pd.read_csv(unified_file_path)

    for _, row in asset_df.iterrows():
        display_id = row['display_id']
        logging.info(f"Fetching additional data for asset with Display ID: {display_id}...")

        asset_row = {'display_id': display_id}
        additional_data = fetch_additional_data(display_id, data_types, base_url, headers, session)

        for data_type, data_value in additional_data.items():
            asset_row[data_type] = json.dumps(data_value)

        asset_data.append(asset_row)

    unified_df = pd.DataFrame(asset_data)
    unified_df = convert_columns_to_snake_case(unified_df)
    unified_df.to_csv(unified_file_path, index=False)
    logging.info(f"Unified data saved to {unified_file_path}")

    return unified_df

def main():
    logging.info("Starting data pipeline...")

    # Step 0: Ensure data directory exists
    data_dir = ensure_data_dir()

    # Step 1: Load environment variables
    env_vars = load_env_variables()

    # Step 2: Create API headers
    headers = create_headers(env_vars['FRESHSERVICE_API_KEY'])

    # Step 3: Configure retry session for API requests
    session = configure_retry_session()

    # Set Freshservice base URL
    base_url = f"https://{env_vars['FRESHSERVICE_DOMAIN']}/api/v2/"

    # Step 4: Download various data
    logging.info("Starting to download asset data...")
    asset_df = download_data('assets?include=type_fields&order_by=created_at&order_type=asc', 'assets_data.csv', base_url, headers, data_dir, session)

    logging.info("Downloading requesters data...")
    download_data('requesters', 'requesters_data.csv', base_url, headers, data_dir, session)

    logging.info("Downloading vendors data...")
    download_data('vendors', 'vendors_data.csv', base_url, headers, data_dir, session)

    logging.info("Downloading products data...")
    download_data('products', 'products_data.csv', base_url, headers, data_dir, session)

    logging.info("Downloading asset types data...")
    download_data('asset_types', 'asset_types_data.csv', base_url, headers, data_dir, session)

    logging.info("Downloading departments data...")
    download_data('departments', 'departments_data.csv', base_url, headers, data_dir, session)

    # # Step 5: Fetch additional data for assets
    # additional_data_types = ["components", "requests", "contracts", "relationships"]

    # if asset_df is not None:
    #     logging.info("Creating a unified dataframe with additional asset data...")
    #     create_unified_dataframe(asset_df, additional_data_types, base_url, headers, session)

    logging.info("Data pipeline completed successfully.")

if __name__ == "__main__":
    main()
