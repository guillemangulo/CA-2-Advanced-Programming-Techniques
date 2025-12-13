#file that uploads each gameweek json file into mongodb 

import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION = os.getenv("COLLECTION")

#convert some string fields into float
def clean_metrics(stats):
    
    if not stats:
        return {}
    
    cleaned = stats.copy()

    #should be floats
    fields_to_convert = [
        'influence', 'creativity', 'threat', 'ict_index',
        'expected_goals', 'expected_assists',
        'expected_goal_involvements', 'expected_goals_conceded'
    ]

    for field in fields_to_convert:
        if field in cleaned:
            try:
                cleaned[field] = float(cleaned[field])
            except:
                #e.g empty string
                cleaned[field] = 0.0
    
    return cleaned

def upload_to_mongo(json_file_path):
    print(f"Reading file: {json_file_path}...")

    #connection
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION]
    except Exception as e:
        print(f"Database connection error: {e}")
        return

    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        count = 0
        
        #each player id for specific gameweek
        for player in data:
            gw_index = str(player['gameweek_index'])
            player_id = str(player['player_id'])
            unique_id = "gw" + gw_index + "_p" + player_id
            
            statistics = player.get('statistics')
            clean_stats = clean_metrics(statistics)

            #prepare to upload in mongo
            doc = {
                "_id": unique_id,
                "player_id": player['player_id'],
                "name": player['name'],
                "team": player['team'],
                "position_id": player['position_code'],
                "gameweek": player['gameweek_index'],
                "statistics": clean_stats,
            }

            # TODO: Document replace_one
            col.replace_one({"_id": unique_id}, doc, upsert=True)
            count += 1

        print("Processed " + str(count) + " players")
           
    except FileNotFoundError:
        print(f"File {json_file_path} not found")
    except Exception as e:
        print(e)
    finally:
        client.close()

if __name__ == "__main__":

    folder_path = "raw/"    
    # os.listdir to give the files inside                           TODO: document it
    if os.path.exists(folder_path):
        files = os.listdir(folder_path)
        for filename in files:
            if filename.endswith(".json"):
                full_path = os.path.join(folder_path, filename)
                upload_to_mongo(full_path)
    else:
        print(f"No folder found: {folder_path}")