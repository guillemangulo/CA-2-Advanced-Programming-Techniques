import unittest
import sys
import os
import pandas as pd
from unittest.mock import patch, MagicMock

#to resolve backend imports
PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(PROJECT_ROOT)

from backend.algorithm.season_evaluator import find_winners, get_top_3_ranking, calculate_season_stats

class TestSeasonEvaluation(unittest.TestCase):

    def setUp(self):
       self.players = pd.DataFrame([
            {"name": "Player A", "team": "Team 1", "goals": 10, "matches_played": 5},
            {"name": "Player B", "team": "Team 1", "goals": 20, "matches_played": 10},
            {"name": "Player C", "team": "Team 2", "goals": 5,  "matches_played": 2},
            {"name": "Player D", "team": "Team 2", "goals": 20, "matches_played": 5},
        ])

    def tearDown(self):
        pass

    def test_find_winners_returns_all_players_with_max_goals(self):
        results = find_winners(self.df, 'goals', 'max')
        self.assertEqual(len(results), 2)
        
        names_found = []
        for r in results:
            names_found.append(r['name'])
        self.assertIn('Player B', names_found)
        self.assertIn('Player D', names_found)
        
    def test_find_winners_calculates_avg(self):
        sample_df = pd.DataFrame([
            {'name': 'Player A', 'team': 'Team B', 'goals': 10, 'matches_played': 2}
        ])
        results = find_winners(sample_df, 'goals', 'max')
        #created the new field average and correctly
        self.assertIn('average_per_match', results[0])
        self.assertEqual(results[0]['average_per_match'], 5.0)

    def test_find_winners_returns_min_goals(self):
        results = find_winners(self.df, 'goals', 'min')
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'Player C')

    def test_get_top_3_ranking_orders_descending(self):
        results = get_top_3_ranking(self.df, 'goals')
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]['value'], 20) 
        self.assertEqual(results[2]['value'], 10)
        
if __name__ == "__main__":
    unittest.main()