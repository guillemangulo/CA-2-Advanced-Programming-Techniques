from flask import Flask, render_template
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# get settings from .env
uri = os.getenv("MONGO_URI")
db_name = os.getenv("DB_NAME", "premier_league")
coll_name = os.getenv("COLLECTION", "gameweek_stats")

client = MongoClient(uri)
db = client[db_name]
players_collection = db[coll_name]

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/players")
def players():
    all_players = players_collection.find().limit(200)

    players_list = []

    for p in all_players:
        # Atlas uses "statistics", older uploads used "stats"
        stats = p.get("statistics") or p.get("stats") or {}

        player_info = {
            "name": p.get("name", "N/A"),
            "team": p.get("team", "N/A"),
            "stats": stats
        }

        players_list.append(player_info)

    return render_template("players.html", players=players_list)


@app.route("/gameweeks")
def gameweeks():
    # Get unique gameweeks from database
    weeks = players_collection.distinct("gameweek_index")
    weeks = sorted(weeks)
    return render_template("gameweeks.html", weeks=weeks)


@app.route("/gameweek/<int:gw>")
def gameweek_detail(gw):
    # Get players for a specific gameweek
    all_players = players_collection.find({"gameweek_index": gw})

    players_list = []
    for p in all_players:
        stats = p.get("stadistics", {})
        player_info = {
            "name": p.get("name"),
            "team": p.get("team"),
            "stats": stats
        }
        players_list.append(player_info)

    return render_template("gameweek_detail.html", players=players_list, gw=gw)

@app.route("/best-player")
def best_player():
    # placeholder — your teammate will add the algorithm later
    return render_template("best_player.html")

if __name__ == "__main__":
    app.run(debug=True)
