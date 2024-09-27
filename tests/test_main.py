import unittest
from unittest.mock import patch, MagicMock
from main import run_script, run_automated_steps, prompt_user_for_matching, run_push_scripts

class TestMainPipeline(unittest.TestCase):

    @patch('main.run_script')
    def test_run_automated_steps(self, mock_run_script):
        mock_run_script.return_value = True  # Simulate successful script execution
        self.assertTrue(run_automated_steps())
        self.assertEqual(mock_run_script.call_count, 5)

    @patch('builtins.input', return_value='yes')
    def test_prompt_user_for_matching_yes(self, mock_input):
        self.assertTrue(prompt_user_for_matching())

    @patch('builtins.input', return_value='no')
    def test_prompt_user_for_matching_no(self, mock_input):
        self.assertFalse(prompt_user_for_matching())

    @patch('main.run_script')
    def test_run_push_scripts(self, mock_run_script):
        mock_run_script.return_value = True
        self.assertTrue(run_push_scripts())
        self.assertEqual(mock_run_script.call_count, 7)

if __name__ == '__main__':
    unittest.main()
