#main.py

import os
from pathlib import Path
from dotenv import load_dotenv

# Import functions from your modules
from src.freshservice_data import main as freshservice_main
from src.freshservice_mapping import process_csv as process_freshservice_csv
from src.airtable_data import main as airtable_main
from src.headcount_processing import main as headcount_main

def load_env_variables():
    load_dotenv()
    required_vars = [
        'FRESHSERVICE_DOMAIN',
        'FRESHSERVICE_API_KEY',
        'AIRTABLE_API_KEY',
        'SANDBOX_BASE_ID',
        'HEADCOUNTTRACKER_BASE_ID',
        'NETSUITE_TABLE_ID',
        'HEADCOUNT_TABLE_ID',
        'FILEWAVE_TABLE_ID'
    ]
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} is not set in the .env file.")
    print("Environment variables loaded successfully.")

def setup_paths():
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(exist_ok=True)
    print(f"Data directory set up at: {data_dir}")
    return base_dir, data_dir

def main():
    print("Starting the data processing pipeline...")

    # Load environment variables
    load_env_variables()

    # Setup paths
    base_dir, data_dir = setup_paths()

    # Step 1: Run Freshservice data collection
    print("\nStep 1: Collecting Freshservice data...")
    freshservice_main(data_dir)

    # Step 2: Process Freshservice CSV
    print("\nStep 2: Processing Freshservice CSV...")
    input_file_path = data_dir / "assets_data.csv"
    output_file_path = data_dir / "assets_data_flattened_cleaned.csv"
    departments_file_path = data_dir / "departments_data.csv"
    vendors_file_path = data_dir / "vendors_data.csv"
    requesters_file_path = data_dir / "requesters_data.csv"
    asset_types_file_path = data_dir / "asset_types_data.csv"

    process_freshservice_csv(
        input_file_path,
        output_file_path,
        departments_file_path,
        vendors_file_path,
        requesters_file_path,
        asset_types_file_path
    )

    # Step 3: Collect Airtable data
    print("\nStep 3: Collecting Airtable data...")
    airtable_main(data_dir)

    # Step 4: Process headcount data
    print("\nStep 4: Processing headcount data...")
    headcount_main(data_dir)

    print("\nAll steps completed successfully.")

if __name__ == "__main__":
    main()
