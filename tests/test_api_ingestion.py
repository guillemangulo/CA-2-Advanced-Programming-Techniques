import unittest
import sys
import os

PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(PROJECT_ROOT)

from backend.data_layer.api_ingestion import get_bootstrap, last_gameweek, data_extraction

class TestApiIngestionReal(unittest.TestCase):
    
    def setUp(self):
        print("\nConnecting to real API...")

    def test_connection_bootstrap(self):
        data = get_bootstrap()
        
        self.assertIsNotNone(data)
        
        self.assertIn('events', data) #gameweeks
        self.assertIn('elements', data) #players
        self.assertIn('teams', data) #teams
        self.assertTrue(len(data['teams']) > 0)

    def test_connection_specific_gameweek(self):
        gw_data = last_gameweek(1)
        self.assertIsNotNone(gw_data)
        self.assertIn('elements', gw_data)
        
        if len(gw_data['elements']) > 0:
            player = gw_data['elements'][0]
            self.assertIn('stats', player)
            self.assertIn('minutes', player['stats'])

    def test_handle_invalid_gameweek(self):
        result = last_gameweek(999) #imposible id
        self.assertIsNone(result)

    def test_data_extraction_integration(self):

        new_files = data_extraction()
        
        self.assertIsInstance(new_files, list)
        
        if len(new_files) > 0:
            first_file = new_files[0]
            self.assertTrue(os.path.exists(first_file))

if __name__ == "__main__":
    unittest.main()