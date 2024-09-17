import os
import pandas as pd
from dotenv import load_dotenv
from pyairtable import Api
from pathlib import Path

# Function to load environment variables
def load_env_variables():
    """Load environment variables from the .env file."""
    load_dotenv()
    env_vars = {
        'AIRTABLE_API_KEY': os.getenv('AIRTABLE_API_KEY'),
        'SANDBOX_BASE_ID': os.getenv('SANDBOX_BASE_ID'),
        'HEADCOUNTTRACKER_BASE_ID': os.getenv('HEADCOUNTTRACKER_BASE_ID'),
        'NETSUITE_TABLE_ID': os.getenv('NETSUITE_TABLE_ID'),
        'HEADCOUNT_TABLE_ID': os.getenv('HEADCOUNT_TABLE_ID'),
        'FILEWAVE_TABLE_ID': os.getenv('FILEWAVE_TABLE_ID'),
    }

    # Check if necessary environment variables are loaded
    for key, value in env_vars.items():
        if not value:
            raise ValueError(f"{key} is not set. Please check your .env file.")

    return env_vars

# Function to initialize the Airtable API
def init_airtable_api(api_key):
    """Initialize the Airtable API."""
    return Api(api_key)

# Function to fetch data from Airtable and save it to a CSV file
def fetch_and_save_airtable_data(api, base_id, table_id, data_dir, filename):
    """Fetch data from Airtable table and save it as a CSV."""
    print(f"Fetching data from table {table_id}...")

    # Get records from the specified table
    table = api.table(base_id, table_id)
    records = table.all()

    # Extract fields from records
    data = [record['fields'] for record in records]

    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(data)
    csv_path = data_dir / filename
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")

# Main function to load environment variables, fetch and save data
def main(data_dir=None):
    # If data_dir is not provided, use the default path
    if data_dir is None:
        data_dir = Path(__file__).resolve().parent.parent / "data"

    # Ensure the data directory exists
    data_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load environment variables
    env_vars = load_env_variables()

    # Step 2: Initialize the Airtable API
    api = init_airtable_api(env_vars['AIRTABLE_API_KEY'])

    # Step 3: Fetch and save data for each table
    fetch_and_save_airtable_data(api, env_vars['SANDBOX_BASE_ID'], env_vars['NETSUITE_TABLE_ID'], data_dir, 'netsuite_data.csv')
    fetch_and_save_airtable_data(api, env_vars['HEADCOUNTTRACKER_BASE_ID'], env_vars['HEADCOUNT_TABLE_ID'], data_dir, 'headcount_data.csv')
    fetch_and_save_airtable_data(api, env_vars['SANDBOX_BASE_ID'], env_vars['FILEWAVE_TABLE_ID'], data_dir, 'filewave_data.csv')

# Run the script if it's the main program
if __name__ == "__main__":
    main()  # This will use the default data directory when run standalone
