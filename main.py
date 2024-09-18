import os
from pathlib import Path
from dotenv import load_dotenv
import importlib

def import_module(module_name):
    return importlib.import_module(f"src.{module_name}")

def setup_environment():
    """Load environment variables and set up necessary directories."""
    load_dotenv()
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return base_dir, data_dir

def main():
    """Main function to orchestrate the data processing pipeline."""
    print("Starting data processing pipeline...")

    # Setup
    base_dir, data_dir = setup_environment()

    # Import modules dynamically
    freshservice_module = import_module("01_data_retrieval_freshservice")
    airtable_module = import_module("02_data_retrieval_airtable")
    freshservice_processing_module = import_module("03_data_processing_freshservice")
    headcount_processing_module = import_module("04_data_processing_headcount")
    data_standardization_module = import_module("05_data_standardization")

    # Step 1: Fetch Freshservice data
    print("\nStep 1: Fetching Freshservice data...")
    freshservice_module.main()

    # Step 2: Fetch Airtable data
    print("\nStep 2: Fetching Airtable data...")
    airtable_module.main(data_dir)

    # Step 3: Process Freshservice data
    print("\nStep 3: Processing Freshservice data...")
    input_file_path = data_dir / "assets_data.csv"
    output_file_path = data_dir / "assets_data_flattened_cleaned_mapped.csv"
    departments_file_path = data_dir / "departments_data.csv"
    vendors_file_path = data_dir / "vendors_data.csv"
    requesters_file_path = data_dir / "requesters_data.csv"
    asset_types_file_path = data_dir / "asset_types_data.csv"
    filewave_file_path = data_dir / "filewave_data.csv"
    products_file_path = data_dir / "products_data.csv"

    freshservice_processing_module.process_csv(
        input_file_path,
        output_file_path,
        departments_file_path,
        vendors_file_path,
        requesters_file_path,
        asset_types_file_path,
        filewave_file_path,
        products_file_path
    )

    # Step 4: Process headcount data
    print("\nStep 4: Processing headcount data...")
    headcount_processing_module.main(data_dir)

    # Step 5: Standardize data
    print("\nStep 5: Standardizing data...")
    data_standardization_module.main()

    print("\nData processing pipeline completed successfully!")

if __name__ == "__main__":
    main()
