# src/laptop_matching.py

import streamlit as st
import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Paths to data files
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PURCHASES_FILE = DATA_DIR / "netsuite_data_cleaned.csv"
ASSETS_FILE = DATA_DIR / "assets_data_cleaned.csv"
ASSIGNMENTS_FILE = DATA_DIR / "asset_purchase_assignments.json"
UPDATED_ASSETS_FILE = DATA_DIR / "assets_data_with_assignments.csv"  # New file to save updated assets data
FLAG_FILE = DATA_DIR / "streamlit_done.flag"  # Flag file to indicate when Streamlit is done

# Load data
@st.cache_data
def load_data():
    purchases = pd.read_csv(PURCHASES_FILE)
    assets = pd.read_csv(ASSETS_FILE)

    # Strip whitespace from column names
    purchases.columns = purchases.columns.str.strip()
    assets.columns = assets.columns.str.strip()

    # Ensure 'count' is numeric and handle missing values
    purchases['count'] = pd.to_numeric(purchases['count'], errors='coerce').fillna(0).astype(int)

    # Parse dates after stripping column names
    purchases['date'] = pd.to_datetime(purchases['date'], errors='coerce')
    assets['created_at'] = pd.to_datetime(assets['created_at'], errors='coerce')

    # Preprocess data
    purchases['vendor_lower'] = purchases['vendor'].fillna('').str.lower()
    purchases['item_lower'] = purchases['item'].fillna('').str.lower()
    purchases['description_lower'] = purchases['description'].fillna('').str.lower()
    purchases['composite_index'] = purchases['item_lower'] + ' ' + purchases['vendor_lower'] + ' ' + purchases['description_lower']

    assets['vendor_name_lower'] = assets['vendor_name'].fillna('').str.lower()
    assets['product_name_lower'] = assets['product_name'].fillna('').str.lower()
    assets['description_lower'] = assets['description'].fillna('').str.lower()
    assets['composite_index'] = assets['product_name_lower'] + ' ' + assets['vendor_name_lower'] + ' ' + assets['description_lower']

    # Filter to only Laptops (case-insensitive)
    purchases_laptops = purchases[purchases['asset_class'].str.lower() == 'laptop']
    assets_laptops = assets[assets['asset_type_name'].str.lower() == 'laptop']

    return purchases_laptops.reset_index(drop=True), assets_laptops.reset_index(drop=True)

# Initialize session state
if 'purchases_df' not in st.session_state:
    st.session_state.purchases_df, st.session_state.assets_df = load_data()
    st.session_state.assignments = {}
    st.session_state.asset_order = []
    st.session_state.current_asset_index = 0

# Function to save assignments
def save_assignments():
    with open(ASSIGNMENTS_FILE, 'w') as f:
        json.dump(st.session_state.assignments, f, default=str)
    st.sidebar.success("Assignments saved successfully!")

# Function to load assignments and apply them to the assets data
def load_assignments():
    if ASSIGNMENTS_FILE.exists():
        with open(ASSIGNMENTS_FILE, 'r') as f:
            st.session_state.assignments = json.load(f)
    else:
        st.session_state.assignments = {}

    # Initialize remaining counts
    st.session_state.purchases_df['remaining_count'] = st.session_state.purchases_df['count']

    # Reset any existing assignments in the assets DataFrame
    st.session_state.assets_df['purchase_assignment'] = None

    # Apply the assignments to the assets DataFrame and update remaining counts
    for asset_id_str, purchase_id in st.session_state.assignments.items():
        asset_id = int(asset_id_str)
        st.session_state.assets_df.loc[
            st.session_state.assets_df['asset_id'] == asset_id, 'purchase_assignment'
        ] = purchase_id

        # Decrease remaining count for the assigned purchase
        st.session_state.purchases_df.loc[
            st.session_state.purchases_df['purchase_id'] == purchase_id, 'remaining_count'
        ] -= 1

load_assignments()

# Ensure 'date' column is datetime in purchases_df
st.session_state.purchases_df['date'] = pd.to_datetime(st.session_state.purchases_df['date'], errors='coerce')

# Function to update sidebar information
def update_sidebar_info():
    st.sidebar.subheader("Assignment Summary")
    st.sidebar.write(f"Total Assets: {len(st.session_state.assets_df)}")
    st.sidebar.write(f"Assigned: {len(st.session_state.assignments)}")
    st.sidebar.write(f"Remaining: {len(st.session_state.assets_df) - len(st.session_state.assignments)}")

    if st.sidebar.checkbox("Show All Assignments"):
        assignments_list = []
        for asset_id, purchase_id in st.session_state.assignments.items():
            asset_df = st.session_state.assets_df[st.session_state.assets_df['asset_id'] == int(asset_id)]
            purchase_df = st.session_state.purchases_df[st.session_state.purchases_df['purchase_id'] == int(purchase_id)]

            if not asset_df.empty and not purchase_df.empty:
                asset = asset_df.iloc[0]
                purchase = purchase_df.iloc[0]
                assignments_list.append({
                    "Asset ID": str(asset_id),
                    "Asset Name": str(asset['name']),
                    "Purchase ID": str(purchase_id),
                    "Purchase Date": purchase['date'].strftime('%Y-%m-%d') if pd.notnull(purchase['date']) else '',
                    "Vendor": str(purchase['vendor']),
                    "Item": str(purchase['item']),
                    "Remaining Count": str(purchase['remaining_count'])
                })
            else:
                st.sidebar.warning(f"Missing data for Asset ID {asset_id} or Purchase ID {purchase_id}")

        if assignments_list:
            assignments_df = pd.DataFrame(assignments_list)
            assignments_df = assignments_df.astype(str)
            st.sidebar.dataframe(assignments_df)
        else:
            st.sidebar.write("No valid assignments to display.")

update_sidebar_info()

# Helper functions
def get_matching_purchases(asset, purchases):
    # Ensure 'date' column is datetime
    purchases['date'] = pd.to_datetime(purchases['date'], errors='coerce')

    # Exact matches
    exact_matches = purchases[
        (purchases['vendor_lower'] == asset['vendor_name_lower']) &
        (purchases['item_lower'] == asset['product_name_lower']) &
        (purchases['date'] <= asset['created_at']) &
        (purchases['remaining_count'] > 0)
    ].copy()
    exact_matches['date_discrepancy'] = (asset['created_at'] - exact_matches['date']).dt.days
    exact_matches = exact_matches.sort_values('date_discrepancy')

    # Remaining purchases
    remaining_purchases = purchases[~purchases.index.isin(exact_matches.index)].copy()

    # Fuzzy matching on remaining purchases
    asset_composite = asset['composite_index']
    remaining_purchases['fuzzy_score'] = remaining_purchases['composite_index'].apply(
        lambda x: round(fuzz.ratio(x, asset_composite), 2)  # Round to 2 decimal places
    )

    remaining_purchases['date_discrepancy'] = (asset['created_at'] - remaining_purchases['date']).dt.days
    fuzzy_matches = remaining_purchases[
        (remaining_purchases['fuzzy_score'] > 45) &
        (remaining_purchases['date_discrepancy'] >= 0) &
        (remaining_purchases['remaining_count'] > 0)
    ].sort_values(['fuzzy_score', 'date_discrepancy'], ascending=[False, True])

    return exact_matches, fuzzy_matches

def display_asset(asset):
    st.header(f"Asset ID: {asset['asset_id']}")
    st.write(f"**Name:** {asset['name']}")
    st.write(f"**Asset Type:** {asset['asset_type_name']}")
    st.write(f"**Vendor:** {asset['vendor_name']}")
    st.write(f"**Product Name:** {asset['product_name']}")
    st.write(f"**Created At:** {asset['created_at'].strftime('%Y-%m-%d')}")
    st.write(f"**Cost:** ${asset['cost']}")
    st.write(f"**Description:** {asset['description']}")
    st.write(f"**Asset State:** {asset['asset_state']}")
    st.write(f"**Requester Name:** {asset['requester_name']}")
    st.write(f"**Last Logged Username:** {asset['last_logged_username']}")
    st.write(f"**Purchase Assignment:** {asset['purchase_assignment']}")

def display_potential_purchases(exact_matches, fuzzy_matches):
    st.subheader("Exact Matches")
    if not exact_matches.empty:
        exact_matches_display = exact_matches.copy()
        exact_matches_display['date'] = pd.to_datetime(exact_matches_display['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        exact_matches_display = exact_matches_display[['purchase_id', 'date', 'vendor', 'item', 'cost', 'remaining_count', 'date_discrepancy']]
        exact_matches_display = exact_matches_display.astype(str)
        st.dataframe(exact_matches_display, height=200)
    else:
        st.write("No exact matches found.")

    st.subheader("Fuzzy Matches")
    if not fuzzy_matches.empty:
        fuzzy_matches_display = fuzzy_matches.copy()
        fuzzy_matches_display['date'] = pd.to_datetime(fuzzy_matches_display['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        fuzzy_matches_display = fuzzy_matches_display[['purchase_id', 'date', 'vendor', 'item', 'cost', 'remaining_count', 'fuzzy_score', 'date_discrepancy']]
        fuzzy_matches_display = fuzzy_matches_display.astype(str)
        st.dataframe(fuzzy_matches_display, height=200)
    else:
        st.write("No fuzzy matches found.")

    potential_purchases = pd.concat([exact_matches, fuzzy_matches])
    return potential_purchases

# Function to assign a purchase to an asset
def assign_purchase(asset, purchase):
    asset_id = asset['asset_id']
    purchase_id = purchase['purchase_id']

    # Get remaining count for the purchase
    remaining_count = st.session_state.purchases_df.loc[
        st.session_state.purchases_df['purchase_id'] == purchase_id, 'remaining_count'
    ].values[0]

    if str(asset_id) in st.session_state.assignments:
        prev_purchase_id = st.session_state.assignments[str(asset_id)]
        if prev_purchase_id == purchase_id:
            st.info(f"Asset ID {asset_id} is already assigned to Purchase ID {purchase_id}.")
            return
        else:
            # Reassigning to a different purchase
            # Increase remaining count of previous purchase
            st.session_state.purchases_df.loc[
                st.session_state.purchases_df['purchase_id'] == prev_purchase_id, 'remaining_count'
            ] += 1

    # Check if new purchase has remaining count
    if remaining_count < 1:
        st.error("Selected purchase has no remaining count.")
        return

    # Decrease remaining count of new purchase
    st.session_state.purchases_df.loc[
        st.session_state.purchases_df['purchase_id'] == purchase_id, 'remaining_count'
    ] -= 1

    # Update assignment
    st.session_state.assignments[str(asset_id)] = purchase_id

    # Update purchase_assignment in assets_df
    st.session_state.assets_df.loc[
        st.session_state.assets_df['asset_id'] == asset_id, 'purchase_assignment'
    ] = purchase_id

    st.success(f"Assigned Asset ID {asset_id} to Purchase ID {purchase_id}.")
    save_assignments()  # Save after assignment
    st.rerun()

# Function to unassign a purchase from an asset
def unassign_purchase(asset):
    asset_id = asset['asset_id']
    if str(asset_id) in st.session_state.assignments:
        purchase_id = int(st.session_state.assignments[str(asset_id)])
        # Increase remaining count of the purchase
        st.session_state.purchases_df.loc[
            st.session_state.purchases_df['purchase_id'] == purchase_id, 'remaining_count'
        ] += 1
        del st.session_state.assignments[str(asset_id)]
        # Reset purchase_assignment in assets_df
        st.session_state.assets_df.loc[
            st.session_state.assets_df['asset_id'] == asset_id, 'purchase_assignment'
        ] = None
        st.success(f"Unassigned Purchase ID {purchase_id} from Asset ID {asset_id}.")
        save_assignments()  # Save after unassignment
        st.rerun()
    else:
        st.warning("No purchase is assigned to this asset.")

# Prioritize assets with non-empty exact matches
if not st.session_state.asset_order:
    assets_with_matches = []
    assets_without_matches = []
    for _, asset_row in st.session_state.assets_df.iterrows():
        exact_matches, _ = get_matching_purchases(asset_row, st.session_state.purchases_df)
        if not exact_matches.empty:
            assets_with_matches.append(asset_row['asset_id'])
        else:
            assets_without_matches.append(asset_row['asset_id'])
    st.session_state.asset_order = assets_with_matches + assets_without_matches

# Main Interface
st.title("Asset to Purchase Assignment")

if st.session_state.current_asset_index < len(st.session_state.asset_order):
    asset_id = st.session_state.asset_order[st.session_state.current_asset_index]
    asset = st.session_state.assets_df[st.session_state.assets_df['asset_id'] == asset_id].iloc[0].to_dict()
    display_asset(asset)

    # Check if asset has an existing assignment
    if str(asset_id) in st.session_state.assignments:
        assigned_purchase_id = int(st.session_state.assignments[str(asset_id)])
        assigned_purchase = st.session_state.purchases_df[
            st.session_state.purchases_df['purchase_id'] == assigned_purchase_id
        ].iloc[0]

        st.write("### This asset is already assigned to the following purchase:")
        assigned_purchase_display = assigned_purchase[['purchase_id', 'date', 'vendor', 'item', 'cost', 'remaining_count']].to_frame().T.copy()
        # Ensure 'date' is datetime
        assigned_purchase_display['date'] = pd.to_datetime(assigned_purchase_display['date'], errors='coerce')
        assigned_purchase_display['date'] = assigned_purchase_display['date'].dt.strftime('%Y-%m-%d')
        # Convert object columns to strings
        for col in assigned_purchase_display.columns:
            if assigned_purchase_display[col].dtype == 'object':
                assigned_purchase_display[col] = assigned_purchase_display[col].astype(str)
        st.write(assigned_purchase_display)

        # Navigation and action buttons in a fixed container
        button_container = st.container()
        with button_container:
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                if st.session_state.current_asset_index > 0:
                    if st.button("Previous Asset", key="previous_asset"):
                        st.session_state.current_asset_index -= 1
                        st.rerun()
            with col2:
                if st.button("Unassign Purchase", key="unassign_purchase"):
                    unassign_purchase(asset)
            with col3:
                if st.session_state.current_asset_index < len(st.session_state.asset_order) - 1:
                    if st.button("Next Asset", key="next_asset"):
                        st.session_state.current_asset_index += 1
                        st.rerun()
    else:
        # Find matching purchases
        exact_matches, fuzzy_matches = get_matching_purchases(asset, st.session_state.purchases_df)
        potential_purchases = display_potential_purchases(exact_matches, fuzzy_matches)

        if not potential_purchases.empty:
            selected_purchase_id = st.selectbox(
                "Select a Purchase to Assign",
                options=potential_purchases['purchase_id'].astype(int),
                format_func=lambda x: f"Purchase ID {x}"
            )

            # Navigation and action buttons in a fixed container
            button_container = st.container()
            with button_container:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col1:
                    if st.session_state.current_asset_index > 0:
                        if st.button("Previous Asset", key="previous_asset"):
                            st.session_state.current_asset_index -= 1
                            st.rerun()
                with col2:
                    if st.button("Assign Purchase", key="assign_purchase"):
                        selected_purchase = st.session_state.purchases_df[
                            st.session_state.purchases_df['purchase_id'] == selected_purchase_id
                        ].iloc[0].to_dict()
                        assign_purchase(asset, selected_purchase)
                with col3:
                    if st.session_state.current_asset_index < len(st.session_state.asset_order) - 1:
                        if st.button("Next Asset", key="next_asset"):
                            st.session_state.current_asset_index += 1
                            st.rerun()
        else:
            st.write("No purchases available to assign for this asset.")

            # Navigation buttons in a fixed container
            button_container = st.container()
            with button_container:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col1:
                    if st.session_state.current_asset_index > 0:
                        if st.button("Previous Asset", key="previous_asset"):
                            st.session_state.current_asset_index -= 1
                            st.rerun()
                with col2:
                    st.write("")  # Empty placeholder
                with col3:
                    if st.button("Next Asset", key="next_asset"):
                        st.session_state.current_asset_index += 1
                        st.rerun()
else:
    st.write("All assets have been processed.")
    st.write("Assignments have been saved automatically.")

st.write("Navigate between assets using the buttons above or the sidebar.")

# Optionally, at the end, save the updated assets data with assignments
if st.button("Save Updated Assets Data"):
    st.session_state.assets_df.to_csv(UPDATED_ASSETS_FILE, index=False)
    st.success(f"Updated assets with assignments saved to {UPDATED_ASSETS_FILE}")

# Add a "Finish" button that will end the Streamlit session
if st.button("Finish"):
    # Create a flag file that indicates the Streamlit session is done
    with open(FLAG_FILE, 'w') as flag_file:
        flag_file.write("done")
    st.success("Process completed. You can now exit Streamlit and resume the pipeline.")
