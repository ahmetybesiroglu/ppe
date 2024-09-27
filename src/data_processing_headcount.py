# src/data_processing_headcount.py

import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_csv(filepath):
    """Loads a CSV file into a pandas DataFrame."""
    file_path = Path(filepath)
    if file_path.exists() and file_path.suffix == '.csv':
        logging.info(f"Loading data from {file_path}...")
        return pd.read_csv(file_path)
    else:
        logging.error(f"The file {file_path} does not exist or is not a CSV file.")
        raise FileNotFoundError(f"The file {file_path} does not exist or is not a CSV file.")

def filter_employees(df):
    """
    Filters the DataFrame to include relevant columns and only active employees.
    Also, removes rows with empty first_name and last_name columns.
    """
    logging.info("Filtering employees and selecting required columns...")

    # Select relevant columns
    employee_df = df[['first_name', 'last_name', 'masterworks_email', 'status', 'employee_type', 'title',
                      'position_start_date', 'department', 'termination_date']].copy()

    # Remove rows where both 'first_name' and 'last_name' are empty
    employee_df = employee_df.dropna(subset=['first_name', 'last_name'], how='all')

    # Filter only active employees
    employee_df = employee_df[employee_df['status'] == 'Active']

    return employee_df

def clean_names(df):
    """Strips any leading/trailing whitespace from 'first_name', 'last_name', and 'masterworks_email'."""
    logging.info("Stripping whitespace from 'first_name', 'last_name', and 'masterworks_email' columns...")

    if 'masterworks_email' not in df.columns:
        logging.error("'masterworks_email' column not found in the DataFrame.")
        raise KeyError("'masterworks_email' column not found in the DataFrame.")

    # Convert the columns to string and handle NaN values before stripping whitespace
    df['first_name'] = df['first_name'].fillna('').astype(str).str.strip()
    df['last_name'] = df['last_name'].fillna('').astype(str).str.strip()
    df['masterworks_email'] = df['masterworks_email'].fillna('').astype(str).apply(lambda x: str(x).strip() if isinstance(x, str) else x)

    return df

def add_full_name(df):
    """Adds a 'full_name' column by combining 'first_name' and 'last_name'."""
    logging.info("Adding 'Full Name' column...")
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    return df

def sort_by_last_name(df):
    """Sorts the DataFrame by 'last_name'."""
    logging.info("Sorting by 'last_name'...")
    return df.sort_values(by='last_name')

def add_employee_id(df):
    """Adds an 'employee_id' column with values from 1 to n."""
    logging.info("Adding 'employee_id' column...")
    df.insert(0, 'employee_id', range(1, len(df) + 1))
    return df

def save_to_csv(df, output_filepath):
    """Saves the DataFrame to a CSV file."""
    output_path = Path(output_filepath)
    logging.info(f"Saving the modified data to {output_path}...")
    df.to_csv(output_path, index=False)
    logging.info(f"Data saved successfully to {output_path}")

def process_headcount_data(input_filepath, output_filepath):
    """Main function to load, filter, process, and save headcount data."""
    # Load the CSV file
    headcount_df = load_csv(input_filepath)

    # Filter employees and select required columns
    employees_df = filter_employees(headcount_df)

    # Clean names by stripping whitespace
    employees_df = clean_names(employees_df)

    # Add 'Full Name' column
    employees_df = add_full_name(employees_df)

    # Sort the DataFrame by 'last_name'
    employees_df = sort_by_last_name(employees_df)

    # Add 'employee_id' column
    employees_df = add_employee_id(employees_df)

    # Save the result to a new CSV file
    save_to_csv(employees_df, output_filepath)

def main(data_dir=None):
    """
    Main function to process headcount data.
    If data_dir is not provided, it uses a default path.
    """
    if data_dir is None:
        # Use a default path relative to the script location
        base_dir = Path(__file__).resolve().parent.parent
        data_dir = base_dir / "data"
    else:
        data_dir = Path(data_dir)

    # Ensure the data directory exists
    data_dir.mkdir(parents=True, exist_ok=True)

    input_file = data_dir / "headcount_data.csv"
    output_file = data_dir / "filtered_active_employees.csv"

    process_headcount_data(input_file, output_file)

if __name__ == "__main__":
    main()  # This will use the default data directory when run standalone
