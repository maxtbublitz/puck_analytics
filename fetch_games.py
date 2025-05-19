from dotenv import load_dotenv
from datetime import datetime
import os
import requests
import psycopg2

load_dotenv()

base_url = os.getenv("NHL_API_URL")
game_endpoint = "v1/gamecenter/2023020205/play-by-play"
# url = f"{base_url}/{game_endpoint}"
url = "https://api-web.nhle.com/v1/gamecenter/2023020205/play-by-play"

# fetch game data from a teams schedule to insert data for game scoring, results and player stats
response = requests.get(url)
if response.status_code == 200:
    print("connected to game at url ", url)
    data = response.json()
    
    # Data to store in games table
    game_id = data["id"]
    season_id = data["season"]
    date = data["gameDate"]
    home_team_id = data["homeTeam"]["id"]
    home_team_name = data["homeTeam"]["commonName"]["default"]
    home_team_score = data["homeTeam"]["score"]
    away_team_id = data["awayTeam"]["id"]
    away_team_name = data["awayTeam"]["commonName"]["default"]
    away_team_score = data["awayTeam"]["commonName"]["default"]

    game_data = data["plays"] # objects about game events

    for event in game_data:
        play = event["typeDescKey"]
        if play == "goal": 
            print(event["details"]["eventOwnerTeamId"], " GOAL SCORED")
        if play == "hit":
            print(event["details"]["eventOwnerTeamId"], " throws the body")


else:
    print("failed to connect to game ", url)



# get the schedule
# get games from the schedule
# check to see if the game ID already exists, if not create a new game in games table

# go through game to track goals, hits and penalties
# add hits and penalties to play stats
