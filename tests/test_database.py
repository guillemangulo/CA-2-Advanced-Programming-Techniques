#Using classh method to just connect with database one time
import json
import unittest
import sys
import os
from pymongo import MongoClient

#to resolve backend imports
PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(PROJECT_ROOT)

from backend.data_layer.database import (clean_metrics, upload_to_mongo, fetch_gameweek_data, save_gameweek_data,
                                          fetch_mpv_data, fetch_all_raw_data, MONGO_URI, DB_NAME, COLLECTION, ANALYTICS_COLL)

class TestDatabase(unittest.TestCase):      #inheriting Testcase

    @classmethod
    def setUpClass(cls):
        try:
            cls.client = MongoClient(MONGO_URI)
            cls.db = cls.client[DB_NAME]
            cls.players_coll = cls.db[COLLECTION]
            cls.gw_coll = cls.db[ANALYTICS_COLL]
            print("Connected with MongoDB")
        except Exception as e:
            print(f"Error: {e}")
             
    def setUp(self):
        self.test_gw_id = 9999  #to do not overwrite the right ones
        self.test_player_id = "test_p1"

        self.unique_id = f"gw{self.test_gw_id}_p{self.test_player_id}"
        
        self.valid_stats = {
            'influence': "35.2",
            'expected_goals': "0.45"
        }
        self.not_valid_stats = {
            'influence': "",        
            'creativity': "None",   
            'threat': "12.5"
        }
        self.empty_stats = {}

        self.test_json_filename = "test_gameweek_9999.json"
        self.test_json_data = [
            {
                "player_id": self.test_player_id,
                "name": "Integration Tester",
                "team": "Test FC",
                "position_code": 2,
                "gameweek_index": self.test_gw_id,
                "statistics": {
                    "minutes": 90,
                    "influence": "50.5", #should be converted to float
                    "goals_scored": 1
                }
            }
        ]
        with open(self.test_json_filename, 'w') as f:
            json.dump(self.test_json_data, f)

    def tearDown(self):
        self.players_coll.delete_one({"_id": self.unique_id})
        self.gw_coll.delete_many({"gameweek": self.test_gw_id})
        self.gw_coll.delete_one({"_id": "test_gw_summary"})

        if os.path.exists(self.test_json_filename):
            os.remove(self.test_json_filename)

    #unit test
    def test_float_conversion(self):
        cleaned = clean_metrics(self.valid_stats)
        self.assertEqual(cleaned['influence'], 35.2)
        self.assertIsInstance(cleaned['expected_goals'], float)

    def test_error_handling(self):
        #checks if empty strings become 0.0
        cleaned = clean_metrics(self.not_valid_stats)
        self.assertEqual(cleaned['influence'], 0.0)
        self.assertEqual(cleaned['creativity'], 0.0)
        self.assertEqual(cleaned['threat'], 12.5)

    def test_empty_input(self):
        cleaned = clean_metrics(self.empty_stats)
        self.assertEqual(cleaned, {})

    #integration tests
    def test_upload(self):
        print(f"Testing upload flow...")

        upload_to_mongo(self.test_json_filename)
        doc = self.players_coll.find_one({"_id": self.unique_id})
        self.assertIsNotNone(doc)
        self.assertEqual(doc['name'], "Integration Tester")
        #checking clean metrics worked properly
        self.assertEqual(doc['statistics']['influence'], 50.5)
    
    def test_save_and_read(self):
        gw_summary = {
            "_id": "test_gw_summary",
            "gameweek": self.test_gw_id,
            "mvp": {"name": "Test MVP Player", "points": 100},
            "top_scorers": []
        }

        save_gameweek_data(gw_summary)
        all_mvps = fetch_mpv_data()
        
        found = False
        for item in all_mvps:
            if item.get('mvp', {}).get('name') == "Test MVP Player":
                found = True
                break
        
        self.assertTrue(found)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
    
    

if __name__ == "__main__":
    unittest.main()