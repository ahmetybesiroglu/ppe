import pandas as pd
from pathlib import Path

# Function to load the CSV file
def load_csv(filepath):
    """Loads a CSV file into a pandas DataFrame."""
    file_path = Path(filepath)
    if file_path.exists() and file_path.suffix == '.csv':
        print(f"Loading data from {file_path}...")
        return pd.read_csv(file_path)
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist or is not a CSV file.")

# Function to filter active employees and keep relevant columns
def filter_active_employees(df):
    """Filters the DataFrame to include only rows with 'Active' status."""
    print("Filtering active employees...")
    active_df = df[df['Status'] == 'Active'][['First Name', 'Last Name']].copy()
    return active_df

# Function to add full name column
def add_full_name(df):
    """Adds a 'Full Name' column by combining 'First Name' and 'Last Name'."""
    print("Adding 'Full Name' column...")
    df['Full Name'] = df['First Name'] + ' ' + df['Last Name']
    return df

# Function to save the modified DataFrame to a CSV file
def save_to_csv(df, output_filepath):
    """Saves the DataFrame to a CSV file."""
    output_path = Path(output_filepath)
    print(f"Saving the modified data to {output_path}...")
    df.to_csv(output_path, index=False)
    print(f"Data saved successfully to {output_path}")

# Main function to process the data
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

# Example usage
if __name__ == "__main__":
    # Reference the current script directory using Path(__file__)
    base_dir = Path(__file__).resolve().parent.parent  # Go two levels up from the script file (ppe/ppe/..)

    # Define input and output file paths relative to the project directory
    input_file = base_dir / "data" / "headcount_data.csv"  # Path to your input CSV file inside the data folder
    output_file = base_dir / "data" / "filtered_active_employees.csv"  # Path to save the output CSV file

    # Call the main function to process the data
    process_headcount_data(input_file, output_file)
