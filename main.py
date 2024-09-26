# main.py (Pipeline Orchestration)
import os
from pathlib import Path
from subprocess import Popen

# Define the data directory and paths to scripts
DATA_DIR = Path(__file__).resolve().parent / "data"
FLAG_FILE = DATA_DIR / "streamlit_done.flag"
STREAMLIT_APP = DATA_DIR.parent / "src" / "matching_streamlit_app.py"
LAPTOP_MATCHING_SCRIPT = DATA_DIR.parent / "src" / "laptop_matching.py"

def run_automated_steps():
    """Run the automated data retrieval, processing, and cleaning steps of the pipeline."""
    print("Starting data retrieval and processing...")

    # Call necessary scripts to retrieve and process the data from Freshservice and Airtable
    os.system(f"python src/data_retrieval_freshservice.py")
    os.system(f"python src/data_retrieval_airtable.py")
    os.system(f"python src/data_processing_freshservice.py")
    os.system(f"python src/data_processing_headcount.py")

    print("Data retrieval and processing completed.")

def prompt_user_for_matching():
    """Prompt the user to decide whether to manually match assets or proceed with automatic matching."""
    while True:
        user_input = input("Do you want to manually match assets with purchases (yes/no)? ")
        if user_input.lower() in ["yes", "no"]:
            return user_input.lower() == "yes"
        print("Invalid input, please enter 'yes' or 'no'.")

def run_manual_matching():
    """Launch the Streamlit app for manual matching."""
    # Clear the flag file if it exists
    if FLAG_FILE.exists():
        FLAG_FILE.unlink()

    # Launch the Streamlit app for manual matching
    print("Launching Streamlit app for manual matching...")
    Popen(['streamlit', 'run', str(STREAMLIT_APP)])

    # Wait for the user to complete the manual matching
    input("Press Enter when the Streamlit matching is done...")

    # Check if the Streamlit app has completed by verifying the flag file
    if FLAG_FILE.exists():
        print("Manual matching completed.")
    else:
        print("Warning: Streamlit app finished without creating the completion flag.")

def run_automatic_matching():
    """Run the automatic matching process using laptop_matching.py."""
    print("Running automatic matching...")
    os.system(f"python {LAPTOP_MATCHING_SCRIPT}")

def main():
    """Main pipeline orchestration function."""
    # Step 1: Run the automated data retrieval and processing steps
    run_automated_steps()

    # Step 2: Ask the user whether they want to perform manual matching or proceed with automatic matching
    if prompt_user_for_matching():
        run_manual_matching()
    else:
        run_automatic_matching()

    # Step 3: Proceed with headcount matching and push data to Airtable
    print("Starting headcount matching and pushing to Airtable...")
    os.system(f"python src/headcount_matching.py")
    os.system(f"python src/push_to_asset_types.py")
    os.system(f"python src/push_to_assets.py")
    os.system(f"python src/push_to_departments.py")
    os.system(f"python src/push_to_products.py")
    os.system(f"python src/push_vendors.py")
    os.system(f"python src/push_to_purchases.py")

    # Step 4: Link tables in Airtable
    print("Linking tables in Airtable...")
    os.system(f"python src/link_tables.py")

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
