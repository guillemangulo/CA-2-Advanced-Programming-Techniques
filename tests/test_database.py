#Using classh method because we dont want to reset each time,
# is better to do it once for expensive operations. Cheking the clean_metrics function
import unittest
import sys
import os

#to resolve backend imports
PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(PROJECT_ROOT)

from backend.data_layer.database import clean_metrics

class TestDatabase(unittest.TestCase):      #inheriting Testcase

    @classmethod
    def setUpClass(cls):        
        cls.valid_stats = {
            'influence': "35.2",
            'creativity': "10.0",
            'threat': "55.5",
            'ict_index': "9.5",
            'expected_goals': "0.45"
        }

        cls.not_valid_stats = {
            'influence': "",        
            'creativity': "None",   
            'threat': "12.5"
        }

        cls.empty_stats = {}

        return super().setUpClass()
    
    @classmethod
    def tearDownClass(cls):
        return super().tearDownClass()
    
    def test1_float_conversion(self):
        cleaned = clean_metrics(self.valid_stats)
        self.assertEqual(cleaned['influence'], 35.2)
        self.assertIsInstance(cleaned['expected_goals'], float)

    def test2_error_handling(self):
        #checks if empty strings become 0.0
        cleaned = clean_metrics(self.not_valid_stats)
        self.assertEqual(cleaned['influence'], 0.0)
        self.assertEqual(cleaned['creativity'], 0.0)
        self.assertEqual(cleaned['threat'], 12.5)

    def test3_empty_input(self):
        cleaned = clean_metrics(self.empty_stats)
        self.assertEqual(cleaned, {})

if __name__ == "__main__":
    unittest.main()