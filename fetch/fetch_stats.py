from dotenv import load_dotenv
from datetime import datetime
import os
import requests
import psycopg2
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db.connection import get_connection

conn = get_connection()
cur = conn.cursor()
print("Connected to database!")
base_url = os.getenv("NHL_API_URL")

# find all the teams that played for each season
cur.execute("SELECT * FROM seasons")
seasons = [row[0] for row in cur.fetchall()]

for season in seasons:
    cur.execute("""
        SELECT ts.id, ts.team_id, ts.season_id, t.abbreviation, t.id
        FROM team_seasons ts
        JOIN teams t ON ts.team_id = t.id
    """)
    rows = cur.fetchall()
    teams = [row[3] for row in rows]
    for team in teams:
        team_url = f"{base_url}/v1/club-stats/{team}/{season}/2"
        response = requests.get(team_url)
        if(response.status_code == 200):
            # team existed this year
            print("connected to ", team_url)
        else:
            print("failed to connect to ", team_url)
