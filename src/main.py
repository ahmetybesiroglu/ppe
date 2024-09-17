import sys
from pathlib import Path

# Add the parent directory to the system path to access the 'ppe' module
base_dir = Path(__file__).resolve().parent.parent  # Adjust this to point to the correct directory
sys.path.append(str(base_dir))

from ppe.freshservice_data import main as freshservice_main
from ppe.freshservice_mapping import process_csv as freshservice_mapping
from ppe.airtable_data import main as airtable_main
from ppe.headcount_processing import process_headcount_data

def run_freshservice_data():
    print("\n=== Step 1: Running Freshservice Data Fetching ===\n")
    freshservice_main()

def run_freshservice_mapping():
    print("\n=== Step 2: Running Freshservice Mapping ===\n")
    freshservice_mapping()

def run_airtable_data():
    print("\n=== Step 3: Running Airtable Data Fetching ===\n")
    airtable_main()

def run_headcount_processing():
    print("\n=== Step 4: Running Headcount Data Processing ===\n")
    process_headcount_data()

def main():
    run_freshservice_data()
    run_freshservice_mapping()
    run_airtable_data()
    run_headcount_processing()

if __name__ == "__main__":
    main()
