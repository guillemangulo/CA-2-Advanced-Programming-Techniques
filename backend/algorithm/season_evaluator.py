# with the info of all gameweeks, calculates seasons metrics
import os
import json
import pandas as pd
import sys
from datetime import datetime
from collections import Counter

PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(PROJECT_ROOT)

from data_layer.database import fetch_all_raw_data, save_season_data, fetch_mvp_data

file_path = os.path.abspath(__file__)
current_folder = os.path.dirname(file_path)
#cd ..
parent_folder = os.path.dirname(current_folder)

ANALYTICS_FOLDER = os.path.join(parent_folder, "data_layer", "analytics")


def get_top_3_ranking(df_source, col_name):
    #top 3
    sorted_df = df_source.sort_values(by=col_name, ascending=False).head(3)
    res = []
    for index, row in sorted_df.iterrows():
        matches = row['matches_played']
        val = row[col_name]
        if matches > 0:
            avg = round(val / matches, 2)
        else:
            avg = 0
            
        res.append({
            "name": row['name'],
            "team": row['team'],
            "value": val,
            "matches": int(matches),
            "average_per_match": avg
        })
    return res

def find_winners(dataframe, column_name, method):
    if dataframe.empty:
        return []
    
    if method == 'max':
        target_val = dataframe[column_name].max()
        if target_val == 0:
            return []
    else:
        target_val = dataframe[column_name].min()
        
    #filter rows that match the target value
    filtered_rows = dataframe[dataframe[column_name] == target_val]
    
    results = []
    for index, row in filtered_rows.iterrows():
        matches = row['matches_played']
        val = row[column_name]
        
        #calc average per match
        if matches > 0:
            avg = round(val / matches, 2)
        else:
            avg = 0
        
        results.append({
            "name": row['name'],
            "team": row['team'],
            "value": val,
            "matches": int(matches),
            "average_per_match": avg
        })
    return results


def calculate_season_stats():
    print("Recalculating season stats")

    raw_data = fetch_all_raw_data()     #get the info of all players in each gameweek (Players table in MongoDB)
    players_list = []
    
    for doc in raw_data:
        stats = doc.get('statistics', {})
        
        row = {
            'player_id': doc['player_id'],
            'name': doc['name'],
            'team': doc['team'],
            'position_id': doc['position_id'],
            'gameweek': doc['gameweek'],
            'minutes': float(stats.get('minutes', 0)),
            'total_points': float(stats.get('total_points', 0)),
            'goals_scored': float(stats.get('goals_scored', 0)),
            'assists': float(stats.get('assists', 0)),
            'goals_conceded': float(stats.get('goals_conceded', 0)),
            'clean_sheets': float(stats.get('clean_sheets', 0)),
            'penalties_saved': float(stats.get('penalties_saved', 0)),
            'yellow_cards': float(stats.get('yellow_cards', 0)),
            'red_cards': float(stats.get('red_cards', 0)),
            'expected_goals': float(stats.get('expected_goals', 0)),
            'in_dreamteam': bool(stats.get('in_dreamteam', False))
        }
        players_list.append(row)

    if len(players_list) == 0:
        print("No raw data found in database.")
        return

    df = pd.DataFrame(players_list)

    #group by players to later sum stats across al gameweeks
    p_totals = df.groupby(['player_id', 'name', 'team', 'position_id']).agg({
        'goals_scored': 'sum',
        'assists': 'sum',
        'minutes': 'sum',
        'goals_conceded': 'sum',
        'clean_sheets': 'sum',
        'penalties_saved': 'sum',
        'yellow_cards': 'sum',
        'red_cards': 'sum',
        'expected_goals': 'sum',
        'in_dreamteam': 'sum', 
        'total_points': 'sum',
        'gameweek': 'count' # matches played by this player
    }).reset_index()

    p_totals.rename(columns={'gameweek': 'matches_played'}, inplace=True)
    
    p_totals['total_cards'] = p_totals['yellow_cards'] + p_totals['red_cards']

    #Scorers/Asistants
    pichichis = find_winners(p_totals, 'goals_scored', 'max')
    top_assisters = find_winners(p_totals, 'assists', 'max')
    #cards
    most_yellows = find_winners(p_totals, 'yellow_cards', 'max')
    most_reds = find_winners(p_totals, 'red_cards', 'max')

    current_max_gw = df['gameweek'].max()
    min_played = current_max_gw * 90 * 0.60
    
    #goalkeaper must have played atleast 60% of minutes
    gk_df = p_totals[
        (p_totals['position_id'] == 1) & 
        (p_totals['minutes'] >= min_played)
    ]
    
    if not gk_df.empty:
        zamoras = find_winners(gk_df, 'goals_conceded', 'min')
    else:
        zamoras = [{"name": "Insufficient Data", "team": "-", "value": 0, "average_per_match": 0}]

    best_penalty_stoppers = find_winners(
        p_totals[p_totals['position_id'] == 1], 
        'penalties_saved', 'max'
    )
    
    outfield_players = p_totals[p_totals['position_id'] != 1]
    most_min_played = find_winners(outfield_players, 'minutes', 'max')

    #last 3 games stats
    start_gameweek = current_max_gw - 2 
    recent_df = df[df['gameweek'] >= start_gameweek]
    
    #group the recent data available
    recent_stats = recent_df.groupby(['name', 'team']).agg({
        'goals_scored': 'sum',
        'assists': 'sum',
        'gameweek': 'count'
    }).reset_index()
    recent_stats.rename(columns={'gameweek': 'matches_played'}, inplace=True)

    top_3_recent_scorers = get_top_3_ranking(recent_stats, 'goals_scored')
    top_3_recent_assisters = get_top_3_ranking(recent_stats, 'assists')

    #team most scored goals
    team_goals = df.groupby('team')['goals_scored'].sum().reset_index()
    max_team_goals = team_goals['goals_scored'].max()
    most_scoring_teams = team_goals[team_goals['goals_scored'] == max_team_goals].to_dict('records')

    #least conceded goals
    goals_conceded_per_gw = df.groupby(['team', 'gameweek'])['goals_conceded'].max().reset_index()
    team_conceded = goals_conceded_per_gw.groupby('team')['goals_conceded'].sum().reset_index()
    min_team_conceded = team_conceded['goals_conceded'].min()
    least_conceded_teams = team_conceded[team_conceded['goals_conceded'] == min_team_conceded].to_dict('records')

    #top 3 expected and dream team
    top_3_xg = get_top_3_ranking(p_totals, 'expected_goals')
    top_3_dream_team = get_top_3_ranking(p_totals, 'in_dreamteam')
    
    
    #defenders ranking
    defenders_df = p_totals[p_totals['position_id'] == 2]
    top_3_defenders = get_top_3_ranking(defenders_df, 'total_points')

    #most MVPs
    analytics_data = fetch_mvp_data()      #mvp of each gameweek (Gameweeks table)
    mvp_names = []    
    for doc in analytics_data:
        mvp_data = doc.get('mvp', {})

        if 'name' in mvp_data:
            mvp_names.append(mvp_data['name'])
            
    most_mvps_result = {"names": [], "count": 0}
    
    if len(mvp_names) > 0:
        #counter to find the most frequent name
        counts = Counter(mvp_names)
        max_mvps = max(counts.values())
        #players who have that max count
        most_mvps_list = []
        for name, count in counts.items():
            if count == max_mvps:
                most_mvps_list.append(name)
                
        most_mvps_result = {"names": most_mvps_list, "count": max_mvps}

    #calculate season id
    today = datetime.now()
    if today.month > 6:
        start_year = today.year
    else:
        start_year = today.year - 1
    
    end_year = start_year + 1
    season_id = "season_" + str(start_year) + "_" + str(end_year)
    
    #preapare json file
    season_summary = {
        "_id": season_id,
        "last_updated_gw": int(current_max_gw),
        "leaders": {
            "top_scorers": pichichis,
            "top_assists": top_assisters,
            "yellow_cards": most_yellows,
            "red_cards": most_reds,
            "least_conceded_gk": zamoras,
            "penalties_saver": best_penalty_stoppers,
            "most_minutes": most_min_played,
            "most_mvps": most_mvps_result
        },
        "teams": {
            "most_goals_scored": most_scoring_teams,
            "least_goals_conceded": least_conceded_teams
        },
        "top_lists": {
            "top_3_scorers_last_3_gw": top_3_recent_scorers,
            "top_3_assisters_last_3_gw": top_3_recent_assisters,
            "top_3_expected_generators": top_3_xg,
            "top_3_best_defenders": top_3_defenders,
            "top_3_dream_team_appearances": top_3_dream_team
        }
    }

    #save to file
    if not os.path.exists(ANALYTICS_FOLDER):
        os.makedirs(ANALYTICS_FOLDER)
    
    json_path = os.path.join(ANALYTICS_FOLDER, "season_overview.json")
    
    with open(json_path, 'w') as f:
        json.dump(season_summary, f, indent=2)

    save_season_data(season_summary)
 
 
