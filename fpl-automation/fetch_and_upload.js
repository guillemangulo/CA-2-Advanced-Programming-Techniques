require("dotenv").config();
const axios = require("axios");
const { MongoClient } = require("mongodb");

// Change the gameweek here
const GAMEWEEK = 13;

async function uploadGameweek(gameweek) {
  const url = `https://fantasy.premierleague.com/api/event/${gameweek}/live/`;

  try {
    // 1. Fetch data from FPL API
    const response = await axios.get(url);
    const players = response.data.elements;

    console.log(`Fetched ${players.length} players from API.`);

    // 2. Connect to MongoDB Atlas
    const client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    console.log("Connected to MongoDB Atlas.");

    const db = client.db("premier_league");
    const collection = db.collection("gameweek_stats");

    // 3. Add gameweek number to each document
    const documents = players.map(player => ({
      ...player,
      gameweek_index: gameweek
    }));

    // 4. Insert into database
    await collection.insertMany(documents);

    console.log(`Gameweek ${gameweek} uploaded successfully!`);
    client.close();
  } catch (error) {
    console.error("Error uploading gameweek:", error.message);
  }
}

// Run the script
uploadGameweek(GAMEWEEK);
