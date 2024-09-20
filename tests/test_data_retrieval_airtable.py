# tests/test_data_retrieval_airtable.py

import pytest
from unittest.mock import patch, MagicMock
from src import data_retrieval_airtable as dra
from pathlib import Path
import pandas as pd
import os

def test_load_env_variables(monkeypatch):
    monkeypatch.setenv('AIRTABLE_API_KEY', 'test_api_key')
    monkeypatch.setenv('SANDBOX_BASE_ID', 'test_base_id')
    monkeypatch.setenv('NETSUITE_TABLE_ID', 'test_table_id')
    monkeypatch.setenv('HEADCOUNT_TABLE_ID', 'test_headcount_table_id')
    monkeypatch.setenv('FILEWAVE_TABLE_ID', 'test_filewave_table_id')
    env_vars = dra.load_env_variables()
    assert env_vars['AIRTABLE_API_KEY'] == 'test_api_key'
    assert env_vars['SANDBOX_BASE_ID'] == 'test_base_id'
    assert env_vars['NETSUITE_TABLE_ID'] == 'test_table_id'

@patch('src.data_retrieval_airtable.Api')
def test_fetch_and_save_airtable_data(mock_api, tmp_path):
    mock_table = MagicMock()
    mock_table.all.return_value = [{'fields': {'Name': 'Test', 'Value': 123}}]
    mock_api.return_value.table.return_value = mock_table

    api = dra.init_airtable_api('test_key')
    data_dir = tmp_path
    dra.fetch_and_save_airtable_data(api, 'base_id', 'table_id', data_dir, 'test.csv')

    assert (data_dir / 'test.csv').exists()
    df = pd.read_csv(data_dir / 'test.csv')
    assert 'name' in df.columns
    assert 'value' in df.columns
