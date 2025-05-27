import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

base_url = os.getenv("NHL_API_URL")

# go through all players in the players table
# add all stats but hits to player stats

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()
print("Connected to database")

ignore_leagues = ["WC-A", "WJC-A", "Olympics", "ECHL", "M-Cup", "International", "WCup", "4 Nations"]

cur.execute("""
    SELECT id FROM players
""")
players = [row[0] for row in cur.fetchall()]  # flattening to just IDs

cur.execute("""
    SELECT id FROM seasons;
""")
seasons = [row[0] for row in cur.fetchall()]

for player in players:
    url = f"{base_url}/v1/player/{str(player)}/landing"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed request for {url}") 
        continue  
    else:
        data = response.json()
        season_stats = data.get("seasonTotals", [])

        previous_team = "N/A"
        for season in season_stats:
            # handle ameture league
            if season["leagueAbbrev"] == "NHL" or season["leagueAbbrev"] == "AHL":
                print(player, previous_team)
                cur.execute("""
                    UPDATE players
                    SET ameture_league = %s
                    WHERE id = %s
                """, (previous_team, player))
                break
            else:
                if season["leagueAbbrev"] in ignore_leagues:
                    continue
                else:
                    previous_team = season["leagueAbbrev"]



    
# Commit changes to database
# conn.commit()

# Cleanup
cur.close()
conn.close()
print("Data insertion complete and connection closed.")
            

    