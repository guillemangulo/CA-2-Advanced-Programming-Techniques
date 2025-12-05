import json
import os
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION = os.getenv("COLLECTION")

if not MONGO_URI:
    print("Error: No MONGO_URI found in .env file")
    exit(1)

#convert some string fields into float
def clean_metrics(stats):
    
    cleaned = stats.copy()

    try:
        cleaned['influence'] = float(stats.get('influence', 0))
        cleaned['creativity'] = float(stats.get('creativity', 0))
        cleaned['threat'] = float(stats.get('threat', 0))
        cleaned['ict_index'] = float(stats.get('ict_index', 0))     
        cleaned['expected_goals'] = float(stats.get('expected_goals', 0))
        cleaned['expected_assists'] = float(stats.get('expected_assists', 0))
        cleaned['expected_goal_involvements'] = float(stats.get('expected_goal_involvements', 0))
        cleaned['expected_goals_conceded'] = float(stats.get('expected_goals_conceded', 0))
        
    except Exception as e:
        print(e)
    return cleaned

def upload_mongo(json_file_path):
    print(f"Processing file: {json_file_path}...")

    #connection
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION]
    except Exception as e:
        print(f"Error: Failed while connecting to MongoDB Atlas: {e}")
        return

    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)

        operations = []
        
        #each player id for each gameweek
        for player in data:
    
            unique_id = f"gw{player['gameweek_index']}_p{player['player_id']}"
            
            raw_stats = player.get('stadistics')
            clean_stats = clean_metrics(raw_stats)

            doc = {
                "_id": unique_id,
                "player_id": player['player_id'],
                "name": player['name'],
                "team": player['team'],
                "position_id": player['position_code'],
                "gameweek": player['gameweek_index'],
                "stats": clean_stats,
            }

            #if exist update, otherwise create it                   TODO: document it
            operations.append(
                UpdateOne({"_id": unique_id}, {"$set": doc}, upsert=True)
            )

        if operations:
            result = collection.bulk_write(operations)
            print(f"Inserted: {result.upserted_count}, Uploaded: {result.modified_count}")
        else:
            print("Empty JSON or without valid data")

    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
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
                upload_mongo(full_path)
    else:
        print(f"No folder found: {folder_path}")