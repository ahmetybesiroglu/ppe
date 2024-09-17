import pandas as pd
from pathlib import Path

def load_csv(filepath):
    """Loads a CSV file into a pandas DataFrame."""
    file_path = Path(filepath)
    if file_path.exists() and file_path.suffix == '.csv':
        print(f"Loading data from {file_path}...")
        return pd.read_csv(file_path)
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist or is not a CSV file.")

def filter_active_employees(df):
    """Filters the DataFrame to include only rows with 'Active' status."""
    print("Filtering active employees...")
    active_df = df[df['Status'] == 'Active'][['First Name', 'Last Name']].copy()
    return active_df

def add_full_name(df):
    """Adds a 'Full Name' column by combining 'First Name' and 'Last Name'."""
    print("Adding 'Full Name' column...")
    df['Full Name'] = df['First Name'] + ' ' + df['Last Name']
    return df

def save_to_csv(df, output_filepath):
    """Saves the DataFrame to a CSV file."""
    output_path = Path(output_filepath)
    print(f"Saving the modified data to {output_path}...")
    df.to_csv(output_path, index=False)
    print(f"Data saved successfully to {output_path}")

def process_headcount_data(input_filepath, output_filepath):
    """Main function to load, filter, process, and save headcount data."""
    # Load the CSV file
    headcount_df = load_csv(input_filepath)

    # Filter active employees
    active_employees_df = filter_active_employees(headcount_df)

    # Add 'Full Name' column
    active_employees_df = add_full_name(active_employees_df)

    # Save the result to a new CSV file
    save_to_csv(active_employees_df, output_filepath)

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
