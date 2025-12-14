# app.py
import os
from flask import Flask, render_template, jsonify, request, abort
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
RAW_COLL = os.getenv("COLLECTION")          # raw player-by-gw collection
ANALYTICS_COLL = os.getenv("ANALYTICS_COLL")# analytics per-gameweek collection
SEASON_COLL = os.getenv("SEASON_COLL")      # season summary collection

if not MONGO_URI:
    raise RuntimeError("Set MONGO_URI in .env")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

app = Flask(__name__, static_folder="static", template_folder="templates")


# ---------- Utility helpers ----------
def safe_find_one(coll_name, query):
    try:
        coll = db[coll_name]
        doc = coll.find_one(query, projection={"_id": False})
        return doc
    except Exception as e:
        print("DB error:", e)
        return None

def safe_find_many(coll_name, query={}, limit=0):
    try:
        coll = db[coll_name]
        cursor = coll.find(query, projection={"_id": False})
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)
    except Exception as e:
        print("DB error:", e)
        return []


# ---------- Frontend pages ----------
# in app.py - update the dashboard route
@app.route("/")
def dashboard():
    # for loading season summary
    season = safe_find_one(SEASON_COLL, {})
    # this is to get list of analytics (gameweeks) for summary cards
    analytics = safe_find_many(ANALYTICS_COLL, {}, limit=50)
    # sort by gameweek if present
    analytics_sorted = sorted(
    analytics,
    key=lambda x: x.get("gameweek", 0),
    reverse=True
    )[:6]

    # prepare the "recent" list (last 6 gameweeks, most recent first)
    recent_gws = analytics_sorted[-6:] if len(analytics_sorted) > 0 else []
    # reverse so the UI shows newest first
    recent_gws = list(reversed(recent_gws))

    return render_template("dashboard.html",
                           season=season,
                           analytics=analytics_sorted,
                           recent=recent_gws)


@app.route("/gameweek/<int:gw>")
def gameweek_page(gw):
    summary = safe_find_one(ANALYTICS_COLL, {"gameweek": gw})
    if not summary:
        abort(404, f"No summary for gameweek {gw}")
    # for convenience, also load top players raw for that gw from raw collection
    players = safe_find_many(RAW_COLL, {"gameweek": gw})
    return render_template("gameweek.html", gw=gw, summary=summary, players=players)


@app.route("/player/<player_id>")
def player_page(player_id):
    # player_id may be numeric or string; search by player_id or name
    try:
        pid = int(player_id)
        query = {"player_id": pid}
    except:
        query = {"name": {"$regex": player_id, "$options": "i"}}
    # fetch all gw records for player
    records = safe_find_many(RAW_COLL, query)
    if not records:
        abort(404, "Player not found")
    # sort records by gameweek
    records_sorted = sorted(records, key=lambda x: x.get("gameweek", 0))
    # build simple series for chart (points per gw)
    series = []
    for r in records_sorted:
        stats = r.get("statistics", {})
        series.append({
            "gw": r.get("gameweek"),
            "name": r.get("name"),
            "points": stats.get("total_points", 0),
            "minutes": stats.get("minutes", 0)
        })
    return render_template("player.html", records=records_sorted, series=series)


@app.route("/season")
def season_page():
    season = safe_find_one(SEASON_COLL, {})
    if not season:
        abort(404, "Season summary not found")
    return render_template("season.html", season=season)


# ---------- JSON API endpoints (optional for AJAX) ----------
@app.route("/api/gameweeks")
def api_gameweeks():
    analytics = safe_find_many(ANALYTICS_COLL)
    return jsonify(analytics)


@app.route("/api/gameweek/<int:gw>")
def api_gameweek(gw):
    summary = safe_find_one(ANALYTICS_COLL, {"gameweek": gw})
    if not summary:
        return jsonify({"error": "not found"}), 404
    return jsonify(summary)


@app.route("/api/player/<player_id>")
def api_player(player_id):
    try:
        pid = int(player_id)
        query = {"player_id": pid}
    except:
        query = {"name": {"$regex": player_id, "$options": "i"}}
    records = safe_find_many(RAW_COLL, query)
    return jsonify(records)


@app.route("/api/season")
def api_season():
    season = safe_find_one(SEASON_COLL, {})
    return jsonify(season or {})

@app.route("/gameweeks")
def gameweeks():
    analytics = safe_find_many(ANALYTICS_COLL)
    analytics_sorted = sorted(
        analytics,
        key=lambda x: x.get("gameweek", 0),
        reverse=True
    )
    return render_template("gameweeks.html", analytics=analytics_sorted)

@app.route("/players")
def players_page():
    # get one record per player 
    players = db[RAW_COLL].aggregate([
        {"$group": {
            "_id": "$player_id",
            "name": {"$first": "$name"},
            "team": {"$first": "$team"}
        }},
        {"$sort": {"name": 1}}
    ])

    return render_template("players.html", players=list(players))

if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))