import os
#from pathlib import Path
from data_layer.api_ingestion import data_extraction
from data_layer.database import upload_to_mongo
from algorithm.player_evaluator import calculate_metrics 
from algorithm.season_evaluator import calculate_season_stats 

def main():
    #update just if there are new gameweeks
    new_files_list = data_extraction()
    
    if len(new_files_list) == 0:
        print("There are no new gameweeks to update.")
        return

    #to know if seasons stats must be update later
    updates_made = False

    for file_path in new_files_list:
        #path to string
        path_str = str(file_path)
        
        if os.path.exists(path_str):
            
            #upload the entire json of this gameweek
            upload_to_mongo(path_str)
            
            try:
                filename = os.path.basename(path_str)
                #from gameweek_14.json to [gameweek, 14.json]
                parts = filename.split('_')
                number_with_extension = parts[1]
                number_string = number_with_extension.replace('.json', '')
                gw_id = int(number_string)
                
                #specific metrics of this gameweek
                calculate_metrics(gw_id)
                updates_made = True 
                
            except Exception as e:
                print(f"Error calculating metrics for {filename}: {e}")
    #seasons stats uploaded only if a new gameweek is detected/analysed
    if updates_made:
        print("Recalculating season history...")
        calculate_season_stats()

if __name__ == "__main__":
    main()