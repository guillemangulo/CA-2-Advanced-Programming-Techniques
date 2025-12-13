import unittest
import sys
import os
from unittest.mock import patch, MagicMock

#to resolve backend imports
PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(PROJECT_ROOT)

from backend.data_layer.api_ingestion import get_bootstrap, last_gameweek, data_extraction

class TestApiIngestion(unittest.TestCase):
    #checking that dont crash
    @patch('backend.data_layer.api_ingestion.requests.get')
    def test_get_simulated_bootstrap_fail(self, mock_get):
        mock_get.side_effect = Exception("e.g No internet")
        result = get_bootstrap()
        #none instead of crashing
        self.assertIsNone(result)


    @patch('backend.data_layer.api_ingestion.requests.get')
    @patch('backend.data_layer.api_ingestion.os.path.exists')
    @patch('builtins.open')
    def test_data_extraction_flow(self, mock_open, mock_exists, mock_get):
        simulated_bootstrap = {
            "events": [
                {"id": 1, "finished": True, "data_checked": True, "name": "Gameweek 1"}, #should get it
                {"id": 2, "finished": False, "data_checked": False, "name": "Gameweek 2"} #should ignore it
            ],
            "elements": [{"id": 100, "first_name": "Guillem", "second_name": "Angulo", "team": 1, "element_type": 1}],
            "teams": [{"id": 1, "name": "FC Barcelona"}]
        }

        gameweek = {
            "elements": [
                {"id": 100, "stats": {"minutes": 90, "goals_scored": 1}}
            ]
        }

        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = simulated_bootstrap
        
        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = gameweek
        #order
        mock_get.side_effect = [mock_response_1, mock_response_2]
        #directory exists, gameweek no
        mock_exists.side_effect = [True, False] 
        new_files = data_extraction()
        #just creates the gw that is finished and checked
        self.assertEqual(len(new_files), 1)
        self.assertTrue("gameweek_1.json" in new_files[0])
        
        mock_open.assert_called()

    @patch('backend.data_layer.api_ingestion.requests.get')
    def test_last_gameweek(self, mock_get):
        last_gameweek(5)
        #correct url for this gw
        mock_get.assert_called_with("https://fantasy.premierleague.com/api/event/5/live/")

if __name__ == "__main__":
    unittest.main()