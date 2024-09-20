# tests/test_laptop_matching.py

import pytest
import pandas as pd
from src import laptop_matching as lm

def test_get_matching_purchases():
    # Sample asset and purchases data
    asset = {
        'vendor_name': 'vendor_a',
        'product_name': 'product_x',
        'created_at': pd.Timestamp('2021-01-10'),
        'composite_index': 'product_x vendor_a'
    }
    purchases = pd.DataFrame({
        'purchase_id': [1, 2],
        'vendor': ['vendor_a', 'vendor_b'],
        'item': ['product_x', 'product_y'],
        'date': [pd.Timestamp('2021-01-05'), pd.Timestamp('2021-01-08')],
        'remaining_count': [1, 1],
        'composite_index': ['product_x vendor_a', 'product_y vendor_b']
    })
    exact_matches, fuzzy_matches = lm.get_matching_purchases(asset, purchases)
    assert len(exact_matches) == 1
    assert exact_matches.iloc[0]['purchase_id'] == 1
    assert len(fuzzy_matches) == 0
