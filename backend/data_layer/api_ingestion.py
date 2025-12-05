import requests
import json
from pathlib import Path

# Config
FPL_BASE = "https://fantasy.premierleague.com/api"
OUT_PATH = Path("raw/")

def get_bootstrap():
    #we first get the entire json that later needs to be filtered
    try:
        res = requests.get(f"{FPL_BASE}/bootstrap-static/")
        return res.json()
    except Exception as e:
        print(f"CRITICAL: Failed to get bootstrap. {e}")
        return None

def get_last_gw(event_id=1):
    url = f"{FPL_BASE}/event/{event_id}/live/"
    try:
        res = requests.get(url)
        return res.json()
    except Exception as e:
        print(f"Error getting stats for Game Week - {event_id}: {e}")
        return None

def run_data_extraction():
    print("Initializing FPL API...")
    
    #entire json with all the info
    base_data = get_bootstrap()
    if not base_data:
        return

    #find finished gameweeks (if verified)
    completed_gameweek = [
        event for event in base_data['events'] 
        if event['finished'] and event['data_checked']
    ]
    print(completed_gameweek)
    
    if not completed_gameweek:
        print("No completed gameweeks found.")
        return
    
    #pick the last of the list
    current_gw = completed_gameweek[-1]
    gameweek_id = current_gw['id']
    print(f"Processing Gameweek: {current_gw['name']} (ID: {gameweek_id})")

    #get the stats of that specific gameweek
    last_gw_played = get_last_gw(gameweek_id)
    if not last_gw_played:
        return

    #map id players with name, same for teams
    player_map = {}
    for player in base_data['elements']: #elements is the list of players in bootstrap-static json
        player_map[player['id']] = player

    team_map = {}
    for team in base_data['teams']:
        team_map[team['id']] = team['name']


    final_output = []

    for live_statistics_player in last_gw_played['elements']: 
        player_id = live_statistics_player['id']
        stats = live_statistics_player['stats']
        
        #we dont save players with 0 mins played
        if stats['minutes'] == 0:
            continue
            
        player_info = player_map.get(player_id)
        if not player_info: 
            continue
        
        row = {
            "player_id": player_id,
            "name": f"{player_info['first_name']} {player_info['second_name']}",
            "team": team_map.get(player_info['team'], "Unknown"),
            "position_code": player_info['element_type'],
            "gameweek_index": gameweek_id,
            "stadistics": stats
        }
        final_output.append(row)

    #save it in to store it later on database
    OUT_PATH.mkdir(exist_ok=True)
    file_name = OUT_PATH / f"gameweek_{gameweek_id}.json"
    
    with open(file_name, 'w') as f:
        json.dump(final_output, f, indent=2)
        
    print("Saving to:", file_name.resolve())

    print(f"Done. Records have been saved to {file_name}")

if __name__ == "__main__":
    run_data_extraction()