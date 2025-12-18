#file that communicates with MongoDB

import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION = os.getenv("COLLECTION")
ANALYTICS_COLL = os.getenv("ANALYTICS_COLL")
SEASON_COLL = os.getenv("SEASON_COLL")


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
    print(f"\nUploading to Mongo: {json_file_path}")

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

            col.replace_one({"_id": unique_id}, doc, upsert=True)
            count += 1

        print("Processed/Inserted " + str(count) + " players")
           
    except FileNotFoundError:
        print(f"File {json_file_path} not found")
    except Exception as e:
        print(e)
    finally:
        client.close()


def fetch_all_raw_data():
    client = MongoClient(MONGO_URI)
    try:
        db = client[DB_NAME]
        collection = db[COLLECTION]
        return list(collection.find({}))
    
    except Exception as e:
        print(f"Error fetching raw data: {e}")
        return []
    finally:
        client.close()

def save_season_data(season_summary):
    client = MongoClient(MONGO_URI)
    try:
        db = client[DB_NAME]
        season_collection = db[SEASON_COLL] 
        
        season_collection.replace_one(
            {"_id": season_summary["_id"]}, 
            season_summary, 
            upsert=True
        )
        print(f"Season stats for {season_summary['_id']} saved successfully to MongoDB")
    except Exception as e:
        print(f"Error saving analytics: {e}")
    finally:
        client.close()


def fetch_mvp_data():
    client = MongoClient(MONGO_URI)
    try:
        db = client[DB_NAME]
        analytics_collection = db[ANALYTICS_COLL]
        cursor = analytics_collection.find({}, {"mvp": 1})

        mvp_results = []
        for doc in cursor:
            mvp_results.append(doc)
        return mvp_results
    
    except Exception as e:
        print(f"Error fetching analytics MVPs: {e}")
        return []
    
    finally:
        client.close()

#data for specific gameweek
def fetch_gameweek_data(gameweek_id):
    client = MongoClient(MONGO_URI)
    try:
        db = client[DB_NAME]
        collection = db[COLLECTION]
        
        cursor = collection.find({"gameweek": gameweek_id})
        gameweek_data = []
        for doc in cursor:
            gameweek_data.append(doc)
            
        return gameweek_data
    
    except Exception as e:
        print(f"Error fetching gameweek {gameweek_id}: {e}")
        return []
    
    finally:
        client.close()

#save data for specific gameweek
def save_gameweek_data(summary_data):
    client = MongoClient(MONGO_URI)
    try:
        db = client[DB_NAME]
        analytics_db = db[ANALYTICS_COLL]
        
        analytics_db.replace_one(
            {"_id": summary_data["_id"]}, 
            summary_data, 
            upsert=True
        )

    except Exception as e:
        print(f"Error saving gameweek analytics: {e}")

    finally:
        client.close()

if __name__ == "__main__":

    folder_path = "raw/"    
    if os.path.exists(folder_path):
        files = os.listdir(folder_path)
        for filename in files:
            if filename.endswith(".json"):
                full_path = os.path.join(folder_path, filename)
                upload_to_mongo(full_path)
    else:
        print(f"No folder found: {folder_path}")