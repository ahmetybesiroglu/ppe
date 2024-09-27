# src/laptop_matching.py

import pandas as pd
from rapidfuzz import fuzz
from pathlib import Path
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PURCHASES_FILE = DATA_DIR / "netsuite_data_cleaned.csv"
ASSETS_FILE = DATA_DIR / "assets_data_cleaned.csv"
OUTPUT_FILE = DATA_DIR / "assets_data_with_assignments.csv"
ASSIGNMENTS_FILE = DATA_DIR / "asset_purchase_assignments.json"

def enforce_data_types(df):
    """
    Ensure that ID columns, asset tags, and other relevant fields are integers (or strings if needed),
    while preserving empty values as NaN (and not filling them with 0).
    """
    int_columns = ['purchase_id', 'asset_type_id', 'asset_id', 'vendor_id', 'vendor', 'product_id', 'display_id', 'count']
    str_columns = ['asset_tag', 'serial_number', 'uuid', 'vendor_name', 'product_name']

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    for col in str_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df

# Load data
def load_data():
    purchases_df = pd.read_csv(PURCHASES_FILE)
    assets_df = pd.read_csv(ASSETS_FILE)

    if 'vendor' not in purchases_df.columns:
        if 'vendor_name' in purchases_df.columns:
            purchases_df['vendor'] = purchases_df['vendor_name']
        else:
            raise KeyError(f"Column 'vendor' not found in purchases_df. Available columns: {', '.join(purchases_df.columns)}")

    if 'item' not in purchases_df.columns:
        if 'product_name' in purchases_df.columns:
            purchases_df['item'] = purchases_df['product_name']
        else:
            raise KeyError(f"Column 'item' not found in purchases_df. Available columns: {', '.join(purchases_df.columns)}")

    if 'description' not in purchases_df.columns:
        raise KeyError(f"Column 'description' not found in purchases_df. Available columns: {', '.join(purchases_df.columns)}")

    purchases_df['vendor_lower'] = purchases_df['vendor'].fillna('').str.lower()
    purchases_df['item_lower'] = purchases_df['item'].fillna('').str.lower()
    purchases_df['description_lower'] = purchases_df['description'].fillna('').str.lower()
    purchases_df['composite_index'] = purchases_df['item_lower'] + ' ' + purchases_df['vendor_lower'] + ' ' + purchases_df['description_lower']

    assets_df['vendor_name_lower'] = assets_df['vendor_name'].fillna('').str.lower()
    assets_df['product_name_lower'] = assets_df['product_name'].fillna('').str.lower()
    assets_df['description_lower'] = assets_df['description'].fillna('').str.lower()
    assets_df['composite_index'] = assets_df['product_name_lower'] + ' ' + assets_df['vendor_name_lower'] + ' ' + assets_df['description_lower']

    if 'count' in purchases_df.columns:
        purchases_df['remaining_count'] = purchases_df['count']
    else:
        raise KeyError(f"Column 'count' not found in purchases_df. Available columns: {', '.join(purchases_df.columns)}")

    return purchases_df, assets_df

# Find exact and fuzzy matches
def match_asset(asset, purchases_df):
    valid_purchases = purchases_df[purchases_df['remaining_count'] > 0].copy()

    exact_matches = valid_purchases[
        (valid_purchases['vendor_lower'] == asset['vendor_name_lower']) &
        (valid_purchases['item_lower'] == asset['product_name_lower']) &
        (valid_purchases['remaining_count'] > 0)
    ]

    if not exact_matches.empty:
        return exact_matches.iloc[0]

    valid_purchases.loc[:, 'fuzzy_score'] = valid_purchases['composite_index'].apply(
        lambda x: fuzz.ratio(x, asset['composite_index'])
    )
    fuzzy_matches = valid_purchases[valid_purchases['fuzzy_score'] > 50].sort_values(by='fuzzy_score', ascending=False)

    if not fuzzy_matches.empty:
        return fuzzy_matches.iloc[0]

    return None

# Automatic matching function with summary and JSON generation
def auto_match_assets(purchases_df, assets_df):
    total_assets = len(assets_df)
    total_purchase_items = int(purchases_df['count'].sum()) if 'count' in purchases_df.columns else len(purchases_df)

    matched_count = 0
    exact_matches_count = 0
    fuzzy_matches_count = 0

    asset_purchase_assignments = {}

    for i, asset in assets_df.iterrows():
        match = match_asset(asset, purchases_df)

        if match is not None:
            assets_df.at[i, 'purchase_assignment'] = match['purchase_id']
            matched_count += 1

            if match.get('fuzzy_score', None) is not None:
                fuzzy_matches_count += 1
            else:
                exact_matches_count += 1

            purchases_df.loc[purchases_df['purchase_id'] == match['purchase_id'], 'remaining_count'] -= 1

            asset_purchase_assignments[asset['asset_id']] = int(match['purchase_id'])
        else:
            assets_df.at[i, 'purchase_assignment'] = None

    logging.info(f"Total assets to match: {total_assets}")
    logging.info(f"Total purchase items to match against (based on count): {total_purchase_items}")
    logging.info(f"Total matched assets: {matched_count}")
    logging.info(f"Exact matches: {exact_matches_count}")
    logging.info(f"Fuzzy matches: {fuzzy_matches_count}")
    logging.info(f"Unmatched assets: {total_assets - matched_count}")

    return assets_df, asset_purchase_assignments

# Save asset-purchase assignments to JSON
def save_assignments_to_json(assignments, output_file):
    with open(output_file, 'w') as json_file:
        json.dump(assignments, json_file, indent=4)
    logging.info(f"Asset-purchase assignments saved to {output_file}")

# Main function to load data, match assets, and save output
def main():
    purchases_df, assets_df = load_data()

    logging.info("Starting automatic matching...")

    assets_df, asset_purchase_assignments = auto_match_assets(purchases_df, assets_df)

    assets_df = enforce_data_types(assets_df)
    assets_df['purchase_assignment'] = assets_df['purchase_assignment'].astype('Int64')

    # Save the matched data to CSV
    assets_df.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Automatic matching completed. Results saved to {OUTPUT_FILE}")

    # Save asset-purchase assignments to JSON
    save_assignments_to_json(asset_purchase_assignments, ASSIGNMENTS_FILE)

if __name__ == "__main__":
    main()
