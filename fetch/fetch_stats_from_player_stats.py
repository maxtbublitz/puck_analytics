from datetime import datetime
from datetime import timedelta
import requests
import psycopg2
import sys
import os

from db.connection import get_connection

conn = get_connection()
cur = conn.cursor()
print("Connected to database!")
base_url = os.getenv("NHL_API_URL")

# define cutoff limit for seasons
cur.execute("SELECT MIN(id) FROM seasons")
season_limit = cur.fetchone()[0]  # Extract the value from the result tuple
print(f"Season limit: {season_limit}")

# find all players
cur.execute("SELECT id FROM players")
players = [row[0] for row in cur.fetchall()]

player_count = 0

def get_team_season_id_from_team_name(team_name, season_id):
    cur.execute("""
        SELECT ts.id, ts.team_id, ts.season_id, t.name
        FROM team_seasons ts
        JOIN teams t ON ts.team_id = t.id
        WHERE t.name = %s AND ts.season_id = %s
    """,(team_name, season_id))
    return cur.fetchall()

def add_player_stats_for_season(player_id, team_season_id, goals, assists, points, plus_minus, average_toi, pim, games_played, season_type):
    if season_type == 2:
        table = "player_stats"
    else:
        table = "player_stats_playoffs"
    cur.execute(f"""
    INSERT INTO {table} (
        player_id, team_season_id, goals, assists, points, plus_minus, average_toi, pim, games_played
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (player_id, team_season_id) DO UPDATE SET
    goals = EXCLUDED.goals,
    assists = EXCLUDED.assists,
    points = EXCLUDED.points,
    plus_minus = EXCLUDED.plus_minus,
    average_toi = EXCLUDED.average_toi,
    pim = EXCLUDED.pim,
    games_played = EXCLUDED.games_played;
    """, (player_id, team_id, goals, assists, points, plus_minus, average_toi, pim, games_played))


for player in players:
    url = f"{base_url}/v1/player/{player}/landing"
    response = requests.get(url)
    player_count += 1

    if(response.status_code == 200):
        data = response.json()
        season_stats = data.get("seasonTotals", [])
        position = data.get("position", [])

        for season in season_stats:
            season_type = int(season["gameTypeId"])

            if season["leagueAbbrev"] == "NHL" and season["season"] >= season_limit and season_type != 1 and position != "G":
                team_name = season["teamName"]["default"]
                season_id = season["season"]
                team_seasons = get_team_season_id_from_team_name(team_name, season_id)

                goals = season["goals"]
                assists = season["assists"]
                points = season["points"]
                plus_minus = season["plusMinus"]
                pim = season["pim"]
                games_played = season["gamesPlayed"]

                avg_toi_str = season["avgToi"]
                minutes, seconds = map(int, avg_toi_str.split(":"))
                average_toi = timedelta(minutes=minutes, seconds=seconds)

                if team_seasons:
                    team_id = team_seasons[0][0]
                    print("Adding ", player, " to team ", team_name, " team season id ", team_id, " type ", season_type, " player number ", player_count)
                    add_player_stats_for_season(player, team_id, goals, assists, points, plus_minus, average_toi, pim, games_played, season_type)
                else:
                    print(f"No team season found for {team_name}")

    else:
        print("failed to connect to ", url)


conn.commit()
cur.close()
conn.close()
