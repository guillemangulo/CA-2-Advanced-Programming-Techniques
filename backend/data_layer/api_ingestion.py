import requests
import json
import os

FPL_BASE = "https://fantasy.premierleague.com/api"

#current file and directory
file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)
OUT_PATH = os.path.join(current_dir, "raw")

def get_bootstrap():
    url = f"{FPL_BASE}/bootstrap-static/"
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"Error: Failed to get bootstrap. {e}")
        return None

def last_gameweek(event_id):
    url = f"{FPL_BASE}/event/{event_id}/live/"
    try:
        res = requests.get(url)
        res.raise_for_status()   #found thanks to testing
        return res.json()
    except Exception as e:
        print(f"Error getting stats for Gameweek {event_id}: {e}")
        return None

def data_extraction():
    print("Getting Bootstrap...")
    
    #make sure that output path exists
    if not os.path.exists(OUT_PATH):
        os.makedirs(OUT_PATH)

    basic_data = get_bootstrap()
    if not basic_data: return []

    events_list = basic_data['events']
    completed_gameweeks = []

    for event in events_list:
        is_finished = event['finished']
        is_verified = event['data_checked']

        if is_finished and is_verified:
            completed_gameweeks.append(event)

    if not completed_gameweeks:
        print("There are not gameweek finished")
        return []

    new_files_created = [] 

    for gw in completed_gameweeks:
        gameweek_id = gw['id']
        json_filename = "gameweek_" + str(gameweek_id) + ".json"
        file_path = os.path.join(OUT_PATH, json_filename)

        if os.path.exists(file_path):
            continue

        print(f"Downloading new data: {gw['name']} (ID: {gameweek_id})")

        last_gw_played = last_gameweek(gameweek_id)
        if not last_gw_played: continue

        #mapping
        player_map = {}
        for p in basic_data['elements']:
            player_map[p['id']] = p

        team_map = {}
        for t in basic_data['teams']:
            team_map[t['id']] = t['name']

        final_output = []

        for live_player in last_gw_played['elements']: 
            stats = live_player['stats']

            if stats['minutes'] == 0: 
                continue
            
            player_id = live_player['id']
            player_info = player_map.get(player_id)

            if not player_info: 
                continue
            
            row = {
                "player_id": live_player['id'],
                "name": f"{player_info['first_name']} {player_info['second_name']}",
                "team": team_map.get(player_info['team'], "Unknown"),
                "position_code": player_info['element_type'],
                "gameweek_index": gameweek_id,
                "statistics": stats
            }
            final_output.append(row)

        #save the new gameweek file
        with open(file_path, 'w') as f:
            json.dump(final_output, f, indent=2)
            
        print(f"JSON created and saved in: {file_path}")
        new_files_created.append(file_path) 

    return new_files_created