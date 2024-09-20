# tests/test_processing_headcount.py

import pytest
import pandas as pd
from src import processing_headcount as ph

def test_filter_active_employees():
    df = pd.DataFrame({'status': ['Active', 'Inactive'], 'first_name': ['John', 'Jane'], 'last_name': ['Doe', 'Smith']})
    active_df = ph.filter_active_employees(df)
    assert len(active_df) == 1
    assert active_df.iloc[0]['first_name'] == 'John'

def test_clean_names():
    df = pd.DataFrame({'first_name': [' John '], 'last_name': [' Doe ']})
    cleaned_df = ph.clean_names(df)
    assert cleaned_df.iloc[0]['first_name'] == 'John'
    assert cleaned_df.iloc[0]['last_name'] == 'Doe'

def test_add_full_name():
    df = pd.DataFrame({'first_name': ['John'], 'last_name': ['Doe']})
    df_with_full_name = ph.add_full_name(df)
    assert 'full_name' in df_with_full_name.columns
    assert df_with_full_name.iloc[0]['full_name'] == 'John Doe'
