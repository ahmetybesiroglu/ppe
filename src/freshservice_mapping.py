import pandas as pd
import ast
import re
from pathlib import Path

# Function to load the CSV file
def load_csv(file_path):
    """Load a CSV file into a pandas DataFrame, handle file not found gracefully."""
    if not file_path.exists():
        print(f"Warning: File {file_path} does not exist. Skipping.")
        return None
    print(f"Loaded {file_path}")  # Debugging: Confirm the file is loaded
    return pd.read_csv(file_path)

# Function to flatten the type_fields column
def flatten_type_fields(df, column_name='type_fields'):
    """
    Flatten the JSON-like column in the DataFrame and return a new DataFrame.

    Args:
        df (pd.DataFrame): The original DataFrame.
        column_name (str): The name of the column to flatten. Default is 'type_fields'.

    Returns:
        pd.DataFrame: A DataFrame with the flattened type_fields data.
    """
    def flatten_row(row):
        try:
            # Convert the string dictionary to a Python dictionary
            type_fields_dict = ast.literal_eval(row[column_name])
            # Return the dictionary as a series (this will create new columns)
            return pd.Series(type_fields_dict)
        except (ValueError, SyntaxError):
            # If there's an issue with parsing, return an empty series
            return pd.Series()

    # Apply the flattening function to each row of the DataFrame
    flattened_type_fields = df.apply(flatten_row, axis=1)

    # Concatenate the original DataFrame with the flattened columns, excluding the original 'type_fields' column
    return pd.concat([df.drop(columns=[column_name]), flattened_type_fields], axis=1)

# Function to clean column names
def clean_column_names(df):
    """
    Remove trailing numbers from column names.

    Args:
        df (pd.DataFrame): The DataFrame whose column names need to be cleaned.

    Returns:
        pd.DataFrame: A DataFrame with cleaned column names.
    """
    def clean_column(col_name):
        # Use regex to remove trailing underscores followed by numbers
        return re.sub(r'_\d+$', '', col_name)

    # Apply the cleaning function to all column names
    df.columns = [clean_column(col) for col in df.columns]
    return df

# Function to map departments, vendors, requesters, and asset types
def map_assets_data(assets_df, departments_df=None, vendors_df=None, requesters_df=None, asset_types_df=None):
    """
    Map asset data with departments, vendors, requesters, and asset types. Handles missing datasets gracefully.

    Args:
        assets_df (pd.DataFrame): The assets DataFrame after flattening.
        departments_df (pd.DataFrame or None): The departments DataFrame.
        vendors_df (pd.DataFrame or None): The vendors DataFrame.
        requesters_df (pd.DataFrame or None): The requesters DataFrame.
        asset_types_df (pd.DataFrame or None): The asset types DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with the mappings applied.
    """
    # Map department names from the departments DataFrame
    if departments_df is not None and 'department_id' in assets_df.columns:
        print("Mapping departments...")  # Debugging: Adding more feedback
        assets_df = assets_df.merge(
            departments_df[['id', 'name']].rename(columns={'id': 'department_id', 'name': 'department_name'}),
            how='left', on='department_id'
        )
    else:
        print("Warning: 'departments_data' is missing or 'department_id' column not found. Skipping department mapping.")

    # Map vendor names from the vendors DataFrame
    if vendors_df is not None and 'vendor' in assets_df.columns:
        print("Mapping vendors...")  # Debugging: Adding more feedback
        assets_df = assets_df.merge(
            vendors_df[['id', 'name']].rename(columns={'id': 'vendor_id', 'name': 'vendor_name'}),
            how='left', left_on='vendor', right_on='vendor_id'
        )
    else:
        print("Warning: 'vendors_data' is missing or 'vendor' column not found. Skipping vendor mapping.")

    # Map requester names from the requesters DataFrame
    if requesters_df is not None and 'user_id' in assets_df.columns:
        print("Mapping requesters...")  # Debugging: Adding more feedback
        assets_df = assets_df.merge(
            requesters_df[['id', 'name']].rename(columns={'id': 'user_id', 'name': 'requester_name'}),
            how='left', on='user_id'
        )
    else:
        print("Warning: 'requesters_data' is missing or 'user_id' column not found. Skipping requester mapping.")

    # Map asset type names from the asset types DataFrame
    if asset_types_df is not None and 'asset_type_id' in assets_df.columns:
        print("Mapping asset types...")  # Debugging: Adding more feedback
        assets_df = assets_df.merge(
            asset_types_df[['id', 'name']].rename(columns={'id': 'asset_type_id', 'name': 'asset_type_name'}),
            how='left', on='asset_type_id'
        )
    else:
        print("Warning: 'asset_types_data' is missing or 'asset_type_id' column not found. Skipping asset type mapping.")

    return assets_df

# Function to save the DataFrame to a CSV file
def save_csv(df, output_file_path):
    """Save a DataFrame to a CSV file."""
    df.to_csv(output_file_path, index=False)
    print(f'Cleaned and flattened CSV saved to {output_file_path}')

def process_csv(input_file_path, output_file_path, departments_file_path=None, vendors_file_path=None, requesters_file_path=None, asset_types_file_path=None, column_name='type_fields'):
    """
    Load a CSV, flatten the type_fields column, clean column names, apply mappings, and save the result.
    Handles missing datasets gracefully.

    Args:
        input_file_path (Path): Path to the input CSV file.
        output_file_path (Path): Path to save the processed CSV file.
        departments_file_path (Path or None): Path to the departments CSV file.
        vendors_file_path (Path or None): Path to the vendors CSV file.
        requesters_file_path (Path or None): Path to the requesters CSV file.
        asset_types_file_path (Path or None): Path to the asset types CSV file.
        column_name (str): The name of the column to flatten. Default is 'type_fields'.
    """
    # Step 1: Load the assets CSV file
    df = load_csv(input_file_path)

    # Step 2: Flatten the type_fields column
    df_flattened = flatten_type_fields(df, column_name)

    # Step 3: Clean the column names
    df_cleaned = clean_column_names(df_flattened)

    # Step 4: Load the other CSV files if they exist
    departments_df = load_csv(departments_file_path) if departments_file_path else None
    vendors_df = load_csv(vendors_file_path) if vendors_file_path else None
    requesters_df = load_csv(requesters_file_path) if requesters_file_path else None
    asset_types_df = load_csv(asset_types_file_path) if asset_types_file_path else None

    # Step 5: Map departments, vendors, requesters, and asset types
    df_mapped = map_assets_data(df_cleaned, departments_df, vendors_df, requesters_df, asset_types_df)

    # Step 6: Save the cleaned, flattened, and mapped DataFrame to a CSV file
    save_csv(df_mapped, output_file_path)

if __name__ == "__main__":
    # This block allows the script to run standalone
    base_dir = Path(__file__).resolve().parent.parent  # Go two levels up from the script file
    data_dir = base_dir / "data"

    input_file_path = data_dir / "assets_data.csv"
    output_file_path = data_dir / "assets_data_flattened_cleaned.csv"
    departments_file_path = data_dir / "departments_data.csv"
    vendors_file_path = data_dir / "vendors_data.csv"
    requesters_file_path = data_dir / "requesters_data.csv"
    asset_types_file_path = data_dir / "asset_types_data.csv"

    process_csv(
        input_file_path,
        output_file_path,
        departments_file_path,
        vendors_file_path,
        requesters_file_path,
        asset_types_file_path
    )
