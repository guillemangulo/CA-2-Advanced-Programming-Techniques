#using class method with mock
import unittest
import sys
import os
from unittest.mock import patch

#backend
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.main_backend import main

class TestMainBackend(unittest.TestCase):

    @patch('backend.main_backend.calculate_season_stats')
    @patch('backend.main_backend.calculate_metrics')
    @patch('backend.main_backend.upload_to_mongo')
    @patch('backend.main_backend.data_extraction')
    @patch('backend.main_backend.os.path.exists') #mocking don't need real files
    def test1_full_update_flow(self, mock_exists, mock_extract, mock_upload, mock_metrics, mock_season): #order of arguments inverted
        
        print("\nTesting main workflow...")
        
        fake_file = "raw_data/gameweek_10.json"
        mock_extract.return_value = [fake_file]
        
        #force to say exists
        mock_exists.return_value = True
        main()
        mock_upload.assert_called_with(fake_file)
        mock_metrics.assert_called_with(10)
        
        #Did it update season stats at the end?
        mock_season.assert_called_once()
        
        print("The main script coordinate functions correctly")

    @patch('backend.main_backend.calculate_season_stats')
    @patch('backend.main_backend.data_extraction')
    def test2_no_new_files(self, mock_extract, mock_season):
        
        print("\nTesting main workflow when everything is up to date...")
        
        #simulating no new gameweeks
        mock_extract.return_value = []
        main()
        mock_season.assert_not_called()
        
        print("The main script stopped as expected.")

if __name__ == "__main__":
    unittest.main()