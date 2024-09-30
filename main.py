import os
import sys
from pathlib import Path
from subprocess import Popen, PIPE
import logging
import argparse
import time

# Setup logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define the data directory and paths to scripts
DATA_DIR = Path(__file__).resolve().parent / "data"
FLAG_FILE = DATA_DIR / "streamlit_done.flag"
STREAMLIT_APP = DATA_DIR.parent / "src" / "matching_streamlit_app.py"
LAPTOP_MATCHING_SCRIPT = DATA_DIR.parent / "src" / "laptop_matching.py"

def run_script(script_path):
    """Helper function to run a script and handle errors."""
    try:
        result = os.system(f"python {script_path}")
        if result != 0:
            logging.error(f"Script {script_path} failed with exit code {result}.")
            return False
        return True
    except Exception as e:
        logging.error(f"Error running {script_path}: {str(e)}")
        return False

def run_automated_steps():
    """Run the automated data retrieval, processing, and cleaning steps of the pipeline."""
    logging.info("Starting data retrieval and processing...")

    scripts = [
        "src/data_retrieval_freshservice.py",
        "src/data_retrieval_airtable.py",
        "src/data_processing_freshservice.py",
        "src/data_processing_headcount.py",
        "src/data_standardization.py"
    ]

    for script in scripts:
        if not run_script(script):
            logging.error(f"Error in processing: {script}")
            return False

    logging.info("Data retrieval, processing, and standardization completed.")
    return True

def run_manual_matching():
    """Launch the Streamlit app for manual matching."""
    logging.info("Launching Streamlit app for manual matching...")

    # Clear the flag file if it exists
    if FLAG_FILE.exists():
        FLAG_FILE.unlink()

    # Launch the Streamlit app for manual matching
    process = Popen(['streamlit', 'run', str(STREAMLIT_APP)], stdout=PIPE, stderr=PIPE)
    logging.info("Streamlit app running. Please complete manual matching.")

    # Wait for the Streamlit app to create the completion flag
    while not FLAG_FILE.exists():
        time.sleep(5)  # Check every 5 seconds

    logging.info("Manual matching completed.")
    process.terminate()

def run_automatic_matching():
    """Run the automatic matching process using laptop_matching.py."""
    logging.info("Running automatic matching...")
    if run_script(LAPTOP_MATCHING_SCRIPT):
        logging.info("Automatic matching completed.")
    else:
        logging.error("Automatic matching failed.")

def run_push_scripts():
    """Run scripts to push data to Airtable."""
    push_scripts = [
        "src/push_to_asset_types.py",
        "src/push_to_assets.py",
        "src/push_to_departments.py",
        "src/push_to_products.py",
        "src/push_vendors.py",
        "src/push_to_purchases.py",
        "src/push_to_employees.py"
    ]

    for script in push_scripts:
        if not run_script(script):
            logging.error(f"Failed to push data: {script}")
            return False

    logging.info("Data pushed to Airtable successfully.")
    return True

def main():
    """Main pipeline orchestration function."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Pipeline with optional Streamlit app")
    parser.add_argument('--streamlit', action='store_true', help="Launch Streamlit app for manual matching")
    args = parser.parse_args()

    # Step 1: Run the automated data retrieval and processing steps
    if not run_automated_steps():
        logging.error("Pipeline terminated due to errors in data processing.")
        return

    # Step 2: Decide whether to run manual matching with Streamlit or automatic matching
    if args.streamlit:
        run_manual_matching()
    else:
        run_automatic_matching()

    # Step 3: Proceed with headcount matching and push data to Airtable
    logging.info("Starting headcount matching and pushing to Airtable...")
    if not run_script("src/headcount_matching.py"):
        logging.error("Headcount matching failed. Exiting pipeline.")
        return

    if not run_push_scripts():
        logging.error("Data push to Airtable failed. Exiting pipeline.")
        return

    # Step 4: Link tables in Airtable
    logging.info("Linking tables in Airtable...")
    if run_script("src/link_tables.py"):
        logging.info("Pipeline completed successfully.")
    else:
        logging.error("Failed to link tables. Exiting pipeline.")

if __name__ == "__main__":
    main()
