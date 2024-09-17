import os
from pathlib import Path
import subprocess

# Define the project directories
base_dir = Path(__file__).resolve().parent  # Go to the base directory
data_dir = base_dir / "data"

# Step 1: Run Freshservice data processing
def run_freshservice_data():
    print("\n=== Step 1: Running Freshservice Data Fetching ===\n")
    subprocess.run(["python3", str(base_dir / "ppe" / "freshservice_data.py")], check=True)

# Step 2: Run Freshservice data mapping
def run_freshservice_mapping():
    print("\n=== Step 2: Running Freshservice Mapping ===\n")
    freshservice_mapping_script = base_dir / "ppe" / "freshservice_mapping.py"
    # Here, you can run the Python script as a subprocess or import and call its main function.
    subprocess.run(["python3", str(freshservice_mapping_script)], check=True)

# Step 3: Run Airtable data processing
def run_airtable_data():
    print("\n=== Step 3: Running Airtable Data Fetching ===\n")
    subprocess.run(["python3", str(base_dir / "ppe" / "airtable_data.py")], check=True)

# Step 4: Run headcount processing
def run_headcount_processing():
    print("\n=== Step 4: Running Headcount Data Processing ===\n")
    subprocess.run(["python3", str(base_dir / "ppe" / "headcount_processing.py")], check=True)

# Orchestrate the workflow
def main():
    # Step 1: Freshservice data
    run_freshservice_data()

    # Step 2: Mapping and cleaning Freshservice data
    run_freshservice_mapping()

    # Step 3: Airtable data
    run_airtable_data()

    # Step 4: Process headcount data
    run_headcount_processing()

if __name__ == "__main__":
    main()
