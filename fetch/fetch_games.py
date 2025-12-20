from datetime import datetime
import requests
import psycopg2
import sys
import os

from db.connection import get_connection

conn = get_connection()
cur = conn.cursor()
print("Connected to database!")

base_url = os.getenv("NHL_API_URL")
game_endpoint = "v1/gamecenter/2023020205/play-by-play"
# url = f"{base_url}/{game_endpoint}"
url = "https://api-web.nhle.com/v1/gamecenter/2023020205/play-by-play"

def get_team_season_id(cur, team_id, season_id):
    cur.execute(
        """
        SELECT team_seasons.id, teams.name
        FROM team_seasons
        JOIN teams ON team_seasons.team_id = teams.id
        WHERE team_seasons.team_id = %s AND team_seasons.season_id = %s
        """,
        (team_id, season_id)
    )
    return cur.fetchone()

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
    away_team_score = data["awayTeam"]["score"]


    # Resolve team_season_ids
    home_result = get_team_season_id(cur, home_team_id, season_id)
    if not home_result:
        print(f"No team_season found for home team {home_team_id} in season {season_id}")
        sys.exit()
    home_team_season_id, home_team_name = home_result
    print("HOME: ", home_team_name, home_team_season_id)

    away_result = get_team_season_id(cur, away_team_id, season_id)
    if not away_result:
        print(f"No team_season found for away team {away_team_id} in season {season_id}")
        sys.exit()
    away_team_season_id, away_team_name = away_result
    print("AWAY: ", away_team_name, away_team_season_id)
    

    # only update if game is not yet in the table
    cur.execute(
        """
        INSERT INTO games (id, season_id, date, home_team_id, away_team_id, home_score, away_score)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(id) DO NOTHING;
        """,(game_id, season_id, date, home_team_season_id, away_team_season_id, home_team_score, away_team_score)
    )

    if cur.rowcount == 0:
        print(f"Game {game_id} already exists. Skipping")

    else:

        game_data = data["plays"] # objects about game events

        goals = 0 # used to track the amount of goals and the order scored in the game
        for event in game_data:
            play = event["typeDescKey"]
            if play == "goal": 
                # update game goals table
                goal_order = goals
                period = event["periodDescriptor"]["number"]
                time_in_period = event["timeInPeriod"]
                situation_code = event["situationCode"]
                home_score = event["details"]["homeScore"]
                away_score = event["details"]["awayScore"]
                # find what team scored
                scoring_team_id = event["details"]["eventOwnerTeamId"]
                if scoring_team_id == home_team_id:
                    team_season_id = home_team_season_id
                    team_season_name = home_team_name
                else:
                    team_season_id = away_team_season_id
                    team_season_name = away_team_name
                print(team_season_id, team_season_name)

                cur.execute("""
                    INSERT INTO game_goals(game_id, team_season_id, goal_order, period, time_in_period, situation_code, home_score, away_score)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(game_id, goal_order) DO NOTHING;
                """,(game_id, team_season_id, goal_order, period, time_in_period, situation_code, home_score, away_score))

                goals += 1
            if play == "hit":
                # add hit to player stats
                hitting_player_id = event["details"]["hittingPlayerId"]
                hitting_team_id = event["details"]["eventOwnerTeamId"]
                if hitting_team_id == home_team_id:
                    team_season_id = home_team_season_id
                    team_season_name = home_team_name
                else:
                    team_season_id = away_team_season_id
                    team_season_name = away_team_name
                print(team_season_id, team_season_name)
                cur.execute("""
                    UPDATE player_stats
                    SET hits = hits + 1
                    WHERE team_season_id = %s AND player_id = %s
                """, (team_season_id, hitting_player_id))


else:
    print("failed to connect to game ", url)

# conn.commit()

cur.close()
conn.close()
print("Data insertion complete and connection closed.")