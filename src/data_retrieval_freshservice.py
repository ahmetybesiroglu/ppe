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

def load_env_variables():
    """Load environment variables from the .env file."""
    load_dotenv()
    env_vars = {
        'FRESHSERVICE_DOMAIN': os.getenv('FRESHSERVICE_DOMAIN'),
        'FRESHSERVICE_API_KEY': os.getenv('FRESHSERVICE_API_KEY')
    }

    # Check if necessary environment variables are loaded
    if not env_vars['FRESHSERVICE_DOMAIN']:
        raise ValueError("FRESHSERVICE_DOMAIN is not set. Please check your .env file.")

    if not env_vars['FRESHSERVICE_API_KEY']:
        raise ValueError("FRESHSERVICE_API_KEY is not set. Please check your .env file.")

    return env_vars

# Function to ensure the 'data' directory exists
def ensure_data_dir():
    """Ensure the 'data' folder exists."""
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

# Function to configure retries with backoff
def configure_retry_session(retries=5, backoff_factor=1, status_forcelist=(502, 503, 504)):
    """Return a session with retry behavior."""
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
    return session

# Function to create headers for the API request
def create_headers(api_key):
    """Create headers for API requests."""
    auth_header_value = base64.b64encode(f"{api_key}:X".encode()).decode()
    return {
        'Authorization': f'Basic {auth_header_value}',
        'Content-Type': 'application/json'
    }

# Function to convert column names to snake_case
def convert_columns_to_snake_case(df):
    """Convert DataFrame column names to snake_case."""
    df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_').str.lower()
    return df

# General function to fetch paginated data from a URL with dynamic response key detection
def fetch_paginated_data(url, headers, session):
    """Fetch paginated data from a specific URL and dynamically find the correct response key."""
    all_data = []
    page = 1
    while True:
        paginated_url = f"{url}&page={page}"
        print(f"Fetching page {page} of data from {url}...")
        try:
            response = session.get(paginated_url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Find the first key that contains a list
            response_key = next((key for key, value in data.items() if isinstance(value, list)), None)

            if not response_key or len(data[response_key]) == 0:
                break  # Stop if no data found

            all_data.extend(data[response_key])
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data on page {page}: {e}")
            break

    return all_data

# Function to save data to CSV
def save_to_csv(new_data, filename, data_dir):
    """Helper function to save data to a CSV file and append new data without duplicates."""
    csv_path = data_dir / filename
    if csv_path.exists():
        print(f"{filename} already exists. Merging new data with existing data.")
        existing_df = pd.read_csv(csv_path)
        if new_data:
            new_df = pd.DataFrame(new_data)
            new_df = convert_columns_to_snake_case(new_df)

            # Merge new data with existing data (avoiding duplicates)
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates().reset_index(drop=True)
            combined_df.to_csv(csv_path, index=False)
            print(f"Updated data saved to {csv_path}")
            return combined_df
        else:
            print(f"No new data to merge for {filename}.")
            return existing_df
    else:
        if new_data:
            new_df = pd.DataFrame(new_data)
            new_df = convert_columns_to_snake_case(new_df)
            new_df.to_csv(csv_path, index=False)
            print(f"Data saved to {csv_path}")
            return new_df
        else:
            print(f"No data found for {filename}.")
            return None

# Function to download data from an endpoint and save it to CSV
def download_data(endpoint, filename, base_url, headers, data_dir, session):
    """Generic function to download data from a specific endpoint and save to CSV."""
    csv_path = data_dir / filename
    if csv_path.exists():
        print(f"{filename} already exists. Loading existing data.")
        return pd.read_csv(csv_path)

    # Check if the endpoint already has query parameters (contains '?')
    if '?' in endpoint:
        url = base_url + f"{endpoint}&per_page=30"
    else:
        url = base_url + f"{endpoint}?per_page=30"

    new_data = fetch_paginated_data(url, headers, session)
    return save_to_csv(new_data, filename, data_dir)

# Function to fetch additional data for an asset (components, requests, contracts, etc.)
def fetch_additional_data(display_id, data_types, base_url, headers, session):
    """Fetch components, requests, contracts, or relationships for an asset and return as a dictionary."""
    additional_data = {}

    for data_type in data_types:
        url = f"{base_url}assets/{display_id}/{data_type}"
        try:
            data = fetch_data_from_url(url, display_id, headers, session)
            if data and data_type in data:
                additional_data[data_type] = data[data_type]  # Return as dictionary (or list of dictionaries)
            else:
                additional_data[data_type] = None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {data_type} for asset {display_id}: {e}")
            additional_data[data_type] = None

    return additional_data

# General function to fetch data from a URL
def fetch_data_from_url(url, display_id, headers, session):
    """Fetch data from a specific URL and handle response."""
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.SSLError as e:
        print(f"SSL Error fetching data for asset {display_id}: {e}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Error fetching data for asset {display_id}: {err}")
        return None

# Function to create a unified DataFrame for all assets and their additional data
def create_unified_dataframe(asset_df, data_types, base_url, headers, session):
    """Create a DataFrame with display_id as rows and additional data as columns."""
    asset_data = []
    data_dir = ensure_data_dir()

    # Check if unified data file exists
    unified_filename = 'assets_data_associates.csv'
    unified_file_path = data_dir / unified_filename
    if unified_file_path.exists():
        print(f"{unified_filename} already exists. Skipping unified data creation.")
        return pd.read_csv(unified_file_path)

    for _, row in asset_df.iterrows():
        display_id = row['display_id']
        asset_row = {'display_id': display_id}

        print(f"Processing asset Display ID: {display_id}")
        # Fetch additional data dynamically for each asset
        additional_data = fetch_additional_data(display_id, data_types, base_url, headers, session)

        # Store additional data as JSON strings to maintain structure in CSV
        for data_type, data_value in additional_data.items():
            asset_row[data_type] = json.dumps(data_value)

        asset_data.append(asset_row)

    # Convert to DataFrame
    unified_df = pd.DataFrame(asset_data)
    # Convert column names to snake_case
    unified_df = convert_columns_to_snake_case(unified_df)

    # Save the unified DataFrame as a CSV file
    unified_df.to_csv(unified_file_path, index=False)
    print(f"Unified data saved to {unified_file_path}")

    return unified_df

def main():
    # Step 0: Ensure data directory exists
    data_dir = ensure_data_dir()

    # Step 1: Load environment variables
    env_vars = load_env_variables()

    # Step 2: Create the API headers
    headers = create_headers(env_vars['FRESHSERVICE_API_KEY'])

    # Step 3: Initialize the session with retry logic
    session = configure_retry_session()

    # Set the Freshservice base URL
    base_url = f"https://{env_vars['FRESHSERVICE_DOMAIN']}/api/v2/"

    # Step 4: Download asset data and other datasets
    asset_df = download_data('assets?include=type_fields&order_by=created_at&order_type=asc', 'assets_data.csv', base_url, headers, data_dir, session)
    download_data('requesters', 'requesters_data.csv', base_url, headers, data_dir, session)
    download_data('vendors', 'vendors_data.csv', base_url, headers, data_dir, session)
    download_data('products', 'products_data.csv', base_url, headers, data_dir, session)
    download_data('asset_types', 'asset_types_data.csv', base_url, headers, data_dir, session)
    download_data('departments', 'departments_data.csv', base_url, headers, data_dir, session)

    # Step 5: Define what additional data to fetch for each asset
    additional_data_types = ["components", "requests", "contracts", "relationships"]

    # Step 6: Create a unified dataframe with additional data and save it
    if asset_df is not None:
        create_unified_dataframe(asset_df, additional_data_types, base_url, headers, session)

if __name__ == "__main__":
    main()
