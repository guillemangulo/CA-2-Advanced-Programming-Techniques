#using fixtures method
import unittest
import sys
import os
import pandas as pd

#backend
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.algorithm.player_evaluator import add_best_players, top_teams

class TestPlayerEvaluation(unittest.TestCase):

    def setUp(self):        
        self.sample = [
            #gk
            {'player_id': 1, 'name': 'Keeper A', 'team': 'Team A', 'position_id': 1, 'total_points': 10},
            {'player_id': 2, 'name': 'Keeper B', 'team': 'Team B', 'position_id': 1, 'total_points': 5},
            #def
            {'player_id': 3, 'name': 'Defender Top', 'team': 'Team A', 'position_id': 2, 'total_points': 50},
            {'player_id': 4, 'name': 'Defender Bad', 'team': 'Team B', 'position_id': 2, 'total_points': 2},
            #fwd
            {'player_id': 5, 'name': 'Striker', 'team': 'Team B', 'position_id': 4, 'total_points': 100},
        ]
        
        self.df = pd.DataFrame(self.sample)
        self.dream_team = [] 

    def tearDown(self):
        return super().tearDown()

    def test_add_best_players(self):
        #top1 defender
        add_best_players(2, 1, self.df, self.dream_team)
        self.assertEqual(len(self.dream_team), 1)
        self.assertEqual(self.dream_team[0]['name'], 'Defender Top')
        self.assertEqual(self.dream_team[0]['total_points'], 50)

    def test_add_best_players_limit(self):
        #there are 2 goalkeepers in data, but we ask for 5
        add_best_players(1, 5, self.df, self.dream_team)
        self.assertEqual(len(self.dream_team), 2)

    def test_top_teams_max(self):
        
        teams = [
            {'team': 'Team A', 'goals': 10, 'cards': 2},
            {'team': 'Team B', 'goals': 30, 'cards': 5},
            {'team': 'Team C', 'goals': 30, 'cards': 1} 
        ]
        df_teams = pd.DataFrame(teams)
        
        result = top_teams(df_teams, 'goals', 'max')
        self.assertEqual(len(result), 2)
        
        teams_found = []
        for team in result:
            teams_found.append(team['team'])
        self.assertIn('Team B', teams_found)
        self.assertIn('Team C', teams_found)

    def test_top_teams_min(self):
        teams = [
            {'team': 'Team A', 'goals': 10, 'cards': 2},
            {'team': 'Team B', 'goals': 30, 'cards': 5},
            {'team': 'Team C', 'goals': 30, 'cards': 1}
        ]
        df_teams = pd.DataFrame(teams)

        result = top_teams(df_teams, 'cards', 'min')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['team'], 'Team C')

if __name__ == "__main__":
    unittest.main()