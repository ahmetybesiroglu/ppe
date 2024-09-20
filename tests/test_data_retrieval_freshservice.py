# tests/test_data_retrieval_freshservice.py

import pytest
from unittest.mock import patch, MagicMock
from src import data_retrieval_freshservice as drf
from pathlib import Path
import pandas as pd
import os

def test_load_env_variables(monkeypatch):
    # Set environment variables using monkeypatch
    monkeypatch.setenv('FRESHSERVICE_DOMAIN', 'test_domain')
    monkeypatch.setenv('FRESHSERVICE_API_KEY', 'test_api_key')

    env_vars = drf.load_env_variables()
    assert env_vars['FRESHSERVICE_DOMAIN'] == 'test_domain'
    assert env_vars['FRESHSERVICE_API_KEY'] == 'test_api_key'

def test_ensure_data_dir(tmp_path):
    # Use a temporary directory for testing
    with patch('src.data_retrieval_freshservice.Path') as mock_path:
        mock_path.return_value = tmp_path
        data_dir = drf.ensure_data_dir()
        assert data_dir.exists()

def test_fetch_paginated_data():
    # Mock the session.get method
    with patch('requests.Session.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {'assets': [{'id': 1}, {'id': 2}]}
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        session = drf.configure_retry_session()
        headers = {'Authorization': 'test'}
        data = drf.fetch_paginated_data('http://test_url', headers, session)
        assert data == [{'id': 1}, {'id': 2}]

def test_save_to_csv(tmp_path):
    data = [{'col1': 'value1', 'col2': 'value2'}]
    data_dir = tmp_path
    df = drf.save_to_csv(data, 'test.csv', data_dir)
    assert (data_dir / 'test.csv').exists()
    saved_df = pd.read_csv(data_dir / 'test.csv')
    assert list(saved_df.columns) == ['col1', 'col2']
    assert saved_df.iloc[0]['col1'] == 'value1'
    assert saved_df.iloc[0]['col2'] == 'value2'
