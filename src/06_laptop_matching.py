# src/06_laptop_matching.py

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

# Load data
@st.cache_data
def load_data():
    purchases = pd.read_csv(PURCHASES_FILE, parse_dates=['date'])
    assets = pd.read_csv(ASSETS_FILE, parse_dates=['created_at'])

    # Ensure 'count' is numeric and handle missing values
    purchases['count'] = pd.to_numeric(purchases['count'], errors='coerce').fillna(0).astype(int)

    # Preprocess data
    purchases['vendor'] = purchases['vendor'].fillna('').str.lower()
    purchases['item'] = purchases['item'].fillna('').str.lower()
    purchases['composite_index'] = purchases['item'] + ' ' + purchases['vendor']

    assets['vendor_name'] = assets['vendor_name'].fillna('').str.lower()
    assets['product_name'] = assets['product_name'].fillna('').str.lower()
    assets['composite_index'] = assets['product_name'] + ' ' + assets['vendor_name']

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

# Function to load assignments
def load_assignments():
    if ASSIGNMENTS_FILE.exists():
        with open(ASSIGNMENTS_FILE, 'r') as f:
            st.session_state.assignments = json.load(f)
    else:
        st.session_state.assignments = {}
    # Initialize remaining counts
    st.session_state.purchases_df['remaining_count'] = st.session_state.purchases_df['count']
    # Update remaining counts based on existing assignments
    if st.session_state.assignments:
        purchase_assignments = pd.Series(st.session_state.assignments).astype(int)
        assigned_counts = purchase_assignments.value_counts()
        for purchase_id, assigned_count in assigned_counts.items():
            st.session_state.purchases_df.loc[
                st.session_state.purchases_df['purchase_id'] == purchase_id, 'remaining_count'
            ] -= assigned_count

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
            asset = st.session_state.assets_df[st.session_state.assets_df['asset_id'] == int(asset_id)].iloc[0]
            purchase = st.session_state.purchases_df[st.session_state.purchases_df['purchase_id'] == int(purchase_id)].iloc[0]
            assignments_list.append({
                "Asset ID": str(asset_id),
                "Asset Name": str(asset['name']),
                "Purchase ID": str(purchase_id),
                "Purchase Date": purchase['date'].strftime('%Y-%m-%d') if pd.notnull(purchase['date']) else '',
                "Vendor": str(purchase['vendor']).capitalize(),
                "Item": str(purchase['item']),
            })
        if assignments_list:
            assignments_df = pd.DataFrame(assignments_list)
            # Convert object columns to strings
            for col in assignments_df.columns:
                if assignments_df[col].dtype == 'object':
                    assignments_df[col] = assignments_df[col].astype(str)
            st.sidebar.dataframe(assignments_df)
        else:
            st.sidebar.write("No assignments yet.")

# Sidebar navigation
st.sidebar.title("Asset-Purchase Assignment Tool")
if st.sidebar.button("Save Assignments"):
    save_assignments()

update_sidebar_info()

# Helper functions
def get_matching_purchases(asset, purchases):
    # Ensure 'date' column is datetime
    purchases['date'] = pd.to_datetime(purchases['date'], errors='coerce')

    # Exact matches
    exact_matches = purchases[
        (purchases['vendor'] == asset['vendor_name']) &
        (purchases['item'] == asset['product_name']) &
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
        lambda x: fuzz.ratio(x, asset_composite)
    )
    remaining_purchases['date_discrepancy'] = (asset['created_at'] - remaining_purchases['date']).dt.days
    fuzzy_matches = remaining_purchases[
        (remaining_purchases['fuzzy_score'] > 70) &
        (remaining_purchases['date_discrepancy'] >= 0) &
        (remaining_purchases['remaining_count'] > 0)
    ].sort_values(['fuzzy_score', 'date_discrepancy'], ascending=[False, True])

    return exact_matches, fuzzy_matches

def display_asset(asset):
    st.header(f"Asset ID: {asset['asset_id']}")
    st.write(f"**Name:** {asset['name']}")
    st.write(f"**Asset Type:** {asset['asset_type_name']}")
    st.write(f"**Vendor:** {asset['vendor_name'].capitalize()}")
    st.write(f"**Product Name:** {asset['product_name']}")
    st.write(f"**Created At:** {asset['created_at'].strftime('%Y-%m-%d')}")
    st.write(f"**Cost:** ${asset['cost']}")
    st.write(f"**Description:** {asset['description']}")
    st.write(f"**Asset State:** {asset['asset_state']}")
    st.write(f"**Requester Name:** {asset['requester_name']}")
    st.write(f"**Last Logged Username:** {asset['last_logged_username']}")

def display_potential_purchases(exact_matches, fuzzy_matches):
    st.subheader("Exact Matches")
    if not exact_matches.empty:
        exact_matches_display = exact_matches.copy()
        # Ensure 'date' is datetime
        exact_matches_display['date'] = pd.to_datetime(exact_matches_display['date'], errors='coerce')
        exact_matches_display['date'] = exact_matches_display['date'].dt.strftime('%Y-%m-%d')
        # Convert object columns to strings
        for col in exact_matches_display.columns:
            if exact_matches_display[col].dtype == 'object':
                exact_matches_display[col] = exact_matches_display[col].astype(str)
        st.dataframe(exact_matches_display[['purchase_id', 'date', 'vendor', 'item', 'cost', 'remaining_count', 'date_discrepancy']], height=200)
    else:
        st.write("No exact matches found.")

    st.subheader("Fuzzy Matches")
    if not fuzzy_matches.empty:
        fuzzy_matches_display = fuzzy_matches.copy()
        # Ensure 'date' is datetime
        fuzzy_matches_display['date'] = pd.to_datetime(fuzzy_matches_display['date'], errors='coerce')
        fuzzy_matches_display['date'] = fuzzy_matches_display['date'].dt.strftime('%Y-%m-%d')
        # Convert object columns to strings
        for col in fuzzy_matches_display.columns:
            if fuzzy_matches_display[col].dtype == 'object':
                fuzzy_matches_display[col] = fuzzy_matches_display[col].astype(str)
        st.dataframe(fuzzy_matches_display[['purchase_id', 'date', 'vendor', 'item', 'cost', 'remaining_count', 'fuzzy_score', 'date_discrepancy']].head(5), height=200)
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
        prev_purchase_id = int(st.session_state.assignments[str(asset_id)])
        if prev_purchase_id == purchase_id:
            # Asset is already assigned to this purchase
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
            st.success(f"Reassigned Asset ID {asset_id} from Purchase ID {prev_purchase_id} to Purchase ID {purchase_id}.")
    else:
        # New assignment
        if remaining_count < 1:
            st.error("Selected purchase has no remaining count.")
            return
        st.session_state.assignments[str(asset_id)] = purchase_id
        st.session_state.purchases_df.loc[
            st.session_state.purchases_df['purchase_id'] == purchase_id, 'remaining_count'
        ] -= 1
        st.success(f"Assigned Purchase ID {purchase_id} to Asset ID {asset_id}.")

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
