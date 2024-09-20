# tests/test_data_standardization.py

import pytest
import pandas as pd
from src import data_standardization as ds
from unittest.mock import MagicMock, patch
import tempfile

def test_load_mapping():
    mapping_str = "{'Old Value': 'New Value'}"
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
        tmp.write(mapping_str)
        tmp_path = tmp.name

    mapping = ds.load_mapping(tmp_path)
    assert mapping['Old Value'] == 'New Value'

def test_apply_mapping_to_dataset():
    df = pd.DataFrame({'column': ['Old Value', 'Another Value']})
    mapping = {'Old Value': 'New Value'}
    mapped_df = ds.apply_mapping_to_dataset(df, 'column', mapping)
    assert mapped_df.loc[0, 'column'] == 'New Value'
    assert mapped_df.loc[1, 'column'] == 'Another Value'

def test_format_dates():
    df = pd.DataFrame({'date_column': ['2021/01/01', '01-02-2021', 'invalid']})
    formatted_df = ds.format_dates(df, ['date_column'])
    assert formatted_df.loc[0, 'date_column'] == '2021-01-01'
    assert formatted_df.loc[1, 'date_column'] == '2021-01-02'
    assert pd.isnull(formatted_df.loc[2, 'date_column'])
