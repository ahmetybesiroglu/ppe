# src/data_retrieval_airtable.py
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

# Function to convert column names to snake_case
def convert_columns_to_snake_case(df):
    """Convert DataFrame column names to snake_case."""
    df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_').str.lower()
    return df

# Function to clean double quotes from specific columns
def clean_quotes(df, columns):
    """Remove all double quotes from specified columns in a DataFrame."""
    for column in columns:
        if column in df.columns:
            df[column] = df[column].str.replace(r'"', '', regex=True).str.strip()
    return df

# Function to fetch data from Airtable and save it to a CSV file
def fetch_and_save_airtable_data(api, base_id, table_id, data_dir, filename, add_purchase_id=False, date_column=None):
    """Fetch data from Airtable table, sort by date, and save it as a CSV with optional purchase_id."""
    csv_path = data_dir / filename
    if csv_path.exists():
        print(f"{filename} already exists. Skipping API call.")
        return pd.read_csv(csv_path)

    print(f"Fetching data from table {table_id}...")

    # Get records from the specified table
    table = api.table(base_id, table_id)
    records = table.all()

    # Extract fields from records
    data = [record['fields'] for record in records]

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Convert column names to snake_case
    df = convert_columns_to_snake_case(df)

    # Sort the data by date if a date_column is provided
    if date_column and date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')  # Ensure the column is in datetime format
        df = df.sort_values(by=date_column, ascending=True)  # Sort from old to new

    # Add purchase_id column if requested
    if add_purchase_id:
        df.insert(0, 'purchase_id', range(1, len(df) + 1))

    # Save to CSV
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")
    return df

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
    # For NetSuite, we are adding purchase_id and sorting by the 'date' column (adjust this to your actual column name)
    fetch_and_save_airtable_data(api, env_vars['SANDBOX_BASE_ID'], env_vars['NETSUITE_TABLE_ID'], data_dir, 'netsuite_data.csv', add_purchase_id=True, date_column='date')
    fetch_and_save_airtable_data(api, env_vars['HEADCOUNTTRACKER_BASE_ID'], env_vars['HEADCOUNT_TABLE_ID'], data_dir, 'headcount_data.csv')
    fetch_and_save_airtable_data(api, env_vars['SANDBOX_BASE_ID'], env_vars['FILEWAVE_TABLE_ID'], data_dir, 'filewave_data.csv')

# Run the script if it's the main program
if __name__ == "__main__":
    main()  # This will use the default data directory when run standalone
