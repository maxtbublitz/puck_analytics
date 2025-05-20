# Fetches roster information from the api
# Team seasons are created from the data
# Rosters are created from the data with a corresponding team season

from dotenv import load_dotenv
from datetime import datetime
import os
import requests
import psycopg2

load_dotenv()

base_url = os.getenv("NHL_API_URL")

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()
print("Connected to database")

cur.execute("""
    SELECT seasons.id, teams.abbreviation, teams.id
    FROM seasons, teams;
""")

season_team_pairs = cur.fetchall()

for season_id, abbreviation, team_id in season_team_pairs:
    url = f"{base_url}/v1/roster/{abbreviation}/{season_id}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed request for {url}") # Some requests should fail, for EX UTA ans SEA
        continue  # Skip invalid responses

    # Insert into team_seasons table
    cur.execute("""
        INSERT INTO team_seasons (team_id, season_id)
        VALUES (%s, %s)
        ON CONFLICT (team_id, season_id) DO NOTHING;
    """, (team_id, season_id))

    # Get the id of the inserted or existing team_season row
    cur.execute("""
        SELECT id FROM team_seasons
        WHERE team_id = %s AND season_id = %s;
    """, (team_id, season_id))
    team_season_id = cur.fetchone()[0]

    # Combine all player types into one list
    data = response.json()
    roster_data = data.get("forwards", []) + data.get("defensemen", []) + data.get("goalies", [])

    for player in roster_data:
        player_id = player["id"]
        first_name = player["firstName"]["default"]
        last_name = player["lastName"]["default"]
        birthdate = player.get("birthDate")
        country = player.get("birthCountry")
        shoots_catches = player.get("shootsCatches")

        jersey_number = player.get("sweaterNumber")
        position = player.get("positionCode")
        player_height_inches = player.get("heightInInches")
        player_weight_pounds = player.get("weightInPounds")

        # Insert into players table if not exists
        cur.execute("""
            INSERT INTO players (id, first_name, last_name, birthdate, country, shoots_catches)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (player_id, first_name, last_name, birthdate, country, shoots_catches))

        # Insert into rosters table
        cur.execute("""
            INSERT INTO rosters (
                player_id, team_season_id, jersey_number,
                position, player_weight_pounds, player_height_inches
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (player_id, team_season_id, jersey_number, position, player_weight_pounds, player_height_inches))

# Commit changes to database
conn.commit()

# Cleanup
cur.close()
conn.close()
print("Data insertion complete and connection closed.")