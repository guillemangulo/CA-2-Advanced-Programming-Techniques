import os
import json
import pandas as pd

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION = os.getenv("COLLECTION")
ANALYTICS_COLL = os.getenv("ANALYTICS_COLL")

file_path = os.path.abspath(__file__) # TODO: Document it
current_folder = os.path.dirname(file_path)
# cd ..
parent_folder = os.path.dirname(current_folder)
# datalayer/analytics
ANALYTICS_FOLDER = os.path.join(parent_folder, "data_layer", "analytics")

def add_best_players(pos_id, count, df, dream_team):
            position_df = df[df['position_id'] == pos_id]
            sorted_pos = position_df.sort_values(by='total_points', ascending=False)
            best = sorted_pos.head(count)
            
            for index, p in best.iterrows():
                dream_team.append({
                    "name": p['name'],
                    "team": p['team'],
                    "total_points": int(p['total_points'])
                })

#function to take team lists that are draw in a metric
def top_teams(team_stats, column, method='max'):
    if method == 'max':
        target_value = team_stats[column].max()
    else:
        target_value = team_stats[column].min()
        
    filtered = team_stats[team_stats[column] == target_value]
    result_list = []
    for index, row in filtered.iterrows():
        result_list.append({
            "team": row['team'],
            "value": int(row[column])
        })
    return result_list

def calculate_metrics(gameweek_id):
    print(f"Metrics for gameweek {gameweek_id} ...")
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION]

        cursor = collection.find({"gameweek": gameweek_id})
        players_list = []

        for doc in cursor:
            stats = doc.get('statistics', {})
            
            row = {
                'player_id': doc['player_id'],
                'name': doc['name'],
                'team': doc['team'],
                'position_id': doc['position_id'],
                'minutes': stats.get('minutes', 0),
                'total_points': stats.get('total_points', 0),
                'goals_scored': stats.get('goals_scored', 0),
                'assists': stats.get('assists', 0),
                'influence': stats.get('influence', 0.0),
                'yellow_cards': stats.get('yellow_cards', 0),
                'in_dreamteam': stats.get('in_dreamteam', False),
                'red_cards': stats.get('red_cards', 0),
                'goals_conceded': stats.get('goals_conceded', 0)
            }
            players_list.append(row)

        if len(players_list) == 0:
            print("There is not available data for this gameweek.")
            client.close()
            return

        df = pd.DataFrame(players_list) # TODO: document it
    
    
        #MVP with no draw option
        df_sorted = df.sort_values(
            by=['total_points', 'goals_scored', 'in_dreamteam', 'influence', 'yellow_cards'],
            ascending=[False, False, False, False, True]
        )
        mvp_row = df_sorted.iloc[0] #TODO: document it
    
        mvp_dict = {
            "name": mvp_row['name'],
            "team": mvp_row['team'],
            "points": int(mvp_row['total_points']),
            "goals": int(mvp_row['goals_scored'])
        }

        #top scorers
        max_goals = df['goals_scored'].max()
        top_scorers = []

        for index, player in df.iterrows(): # TODO: document it
            if player['goals_scored'] == max_goals:
                top_scorers.append({
                    "name": player['name'],
                    "team": player['team'],
                    "goals": int(player['goals_scored'])
                })

        #dreamteam
        dream_team = []    

        add_best_players(1, 1, df, dream_team) #gk
        add_best_players(2, 3, df, dream_team) #def
        add_best_players(3, 4, df, dream_team) #mid
        add_best_players(4, 3, df, dream_team) #fwd

        #team stats
        team_stats = df.groupby('team').agg({
            'goals_scored': 'sum',      #sum of all goals of all players in a team
            'goals_conceded': 'max',    
            'yellow_cards': 'sum',
            'red_cards': 'sum'
        }).reset_index()

        #total of targets in a team
        team_stats['total_cards'] = team_stats['yellow_cards'] + team_stats['red_cards']

        #tops of teams
        most_attacking = top_teams(team_stats, 'goals_scored', 'max')
        best_defense = top_teams(team_stats, 'goals_conceded', 'min')
        most_cards = top_teams(team_stats, 'total_cards', 'max')
        least_cards = top_teams(team_stats, 'total_cards', 'min')

        #prepare json file
        summary = {
            "_id": "summary_gw_" + str(gameweek_id),
            "gameweek": gameweek_id,
            "mvp": mvp_dict,
            "top_scorers": top_scorers,
            "dream_team": dream_team,
            "team_stats": {
                "most_goals_scored": most_attacking,
                "least_goals_conceded": best_defense,
                "most_cards_conceded": most_cards,
                "least_cards_conceded": least_cards
            }
        }

        #save to file
        if not os.path.exists(ANALYTICS_FOLDER):
            os.makedirs(ANALYTICS_FOLDER)

        file_name = "analytics_gw_" + str(gameweek_id) + ".json"
        full_path = os.path.join(ANALYTICS_FOLDER, file_name)

        with open(full_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"File saved at: {full_path}")

        #save it to mongo
        analytics_db = db[ANALYTICS_COLL]
        analytics_db.replace_one(
            {"_id": summary["_id"]}, summary, upsert=True
        )
        
        print("Uploaded to MongoDB.")
    except Exception as e:
        print(f"Something happened: {e}")
        
    finally:
        client.close()
