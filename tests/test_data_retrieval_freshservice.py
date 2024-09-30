import sys
from pathlib import Path

# Use pathlib to handle path manipulations
current_dir = Path(__file__).resolve().parent
src_dir = current_dir.parent / 'src'

# Insert the src directory into sys.path
sys.path.insert(0, str(src_dir))

import unittest
from unittest.mock import patch, mock_open, MagicMock
import data_retrieval_freshservice as drf
import os
import pandas as pd
from pathlib import Path

class TestDataRetrievalFreshservice(unittest.TestCase):

    ### 1. Test: `load_env_variables` ###
    @patch('data_retrieval_freshservice.load_dotenv')
    @patch('data_retrieval_freshservice.os.getenv')
    def test_load_env_variables(self, mock_getenv, mock_load_dotenv):
        # Mock environment variables
        mock_getenv.side_effect = lambda key: {'FRESHSERVICE_DOMAIN': 'domain', 'FRESHSERVICE_API_KEY': 'api_key'}.get(key)

        # Call the function
        env_vars = drf.load_env_variables()

        # Assertions to check if variables are loaded correctly
        self.assertEqual(env_vars['FRESHSERVICE_DOMAIN'], 'domain')
        self.assertEqual(env_vars['FRESHSERVICE_API_KEY'], 'api_key')

        # Test when variables are missing
        mock_getenv.side_effect = lambda key: None
        with self.assertRaises(ValueError):
            drf.load_env_variables()

    ### 2. Test: `ensure_data_dir` ###
    @patch('data_retrieval_freshservice.Path.mkdir')
    def test_ensure_data_dir(self, mock_mkdir):
        # Call the function
        data_dir = drf.ensure_data_dir()

        # Assertions
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        self.assertEqual(data_dir, Path("data"))

    ### 3. Test: `configure_retry_session` ###
    def test_configure_retry_session(self):
        # Call the function
        session = drf.configure_retry_session()

        # Assertions to ensure the session is properly configured
        self.assertIsNotNone(session)
        self.assertEqual(session.adapters['http://'].max_retries.total, 5)
        self.assertEqual(session.adapters['https://'].max_retries.total, 5)

    ### 4. Test: `create_headers` ###
    def test_create_headers(self):
        # Call the function with a mock API key
        headers = drf.create_headers('fake_api_key')

        # Assertions to check if the header is correctly encoded
        self.assertIn('Authorization', headers)
        self.assertEqual(headers['Content-Type'], 'application/json')

    ### 5. Test: `fetch_paginated_data` ###
    @patch('data_retrieval_freshservice.requests.Session.get')
    def test_fetch_paginated_data(self, mock_get):
        # Setup the mock to return paginated data
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {'records': [{'id': 1}]}

        # Call the function
        session = drf.configure_retry_session()
        data = drf.fetch_paginated_data('http://fake-url', {}, session)

        # Assertions
        self.assertEqual(len(data), 1)
        mock_get.assert_called()

    ### 6. Test: `save_to_csv` ###
    @patch('pandas.DataFrame.to_csv')
    @patch('pandas.read_csv')
    @patch('data_retrieval_freshservice.Path.exists', return_value=True)
    def test_save_to_csv_with_existing_file(self, mock_exists, mock_read_csv, mock_to_csv):
        # Mock existing and new data
        mock_read_csv.return_value = pd.DataFrame({'id': [1]})
        new_data = [{'id': 2}]

        # Call the function
        result = drf.save_to_csv(new_data, 'test.csv', Path("data"))

        # Assertions: DataFrame should be saved, and mock should be called
        mock_to_csv.assert_called_once()
        self.assertEqual(result.shape[0], 2)

    ### 7. Test: `download_data` ###
    @patch('data_retrieval_freshservice.fetch_paginated_data')
    @patch('pandas.read_csv')
    @patch('data_retrieval_freshservice.Path.exists', return_value=True)
    def test_download_data_existing(self, mock_exists, mock_read_csv, mock_fetch_paginated_data):
        # Mock the case when the file already exists
        mock_read_csv.return_value = pd.DataFrame({'id': [1]})

        # Call the function
        data = drf.download_data('endpoint', 'file.csv', 'base_url', {}, Path("data"), MagicMock())

        # Assertions: The function should return the already existing data without calling fetch
        self.assertEqual(len(data), 1)
        mock_fetch_paginated_data.assert_not_called()

    @patch('data_retrieval_freshservice.fetch_paginated_data', return_value=[{'id': 1}])
    @patch('data_retrieval_freshservice.Path.exists', return_value=False)
    @patch('pandas.DataFrame.to_csv')
    def test_download_data_new(self, mock_to_csv, mock_exists, mock_fetch_paginated_data):
        # Test for downloading new data
        session = MagicMock()

        # Call the function
        data = drf.download_data('endpoint', 'file.csv', 'base_url', {}, Path("data"), session)

        # Assertions: Data should be fetched and saved
        self.assertEqual(len(data), 1)
        mock_to_csv.assert_called_once()

if __name__ == '__main__':
    unittest.main()
