import requests
import json
import time
import psycopg2
from datetime import datetime

from .db_helpers import get_or_create_conference, get_or_create_division, get_team_id
from .http_utils import get_with_retry

# database helper functions (moved to database/db_helpers.py):
# - get_or_create_conference
# - get_or_create_division
# - get_team_id
#
# HTTP helper `get_with_retry` moved to database/http_utils.py


# fetch seasons data from NHL API
def get_seasons_from_api(base_url, season_endpoint="stats/rest/en/season", season_threshold=20052006):
    """Fetch seasons data from the NHL API."""
    url = f"{base_url}/{season_endpoint}"

    try:
        response = get_with_retry(url)
        response.raise_for_status()
        seasons_data = response.json().get("data", [])
    except requests.RequestException as e:
        print(f"Error fetching seasons data: {e}")
        return []
    
    processed_seasons = []
    for season in seasons_data:
        id = season["id"]
        if id >= season_threshold:
            processed_seasons.append({
                "id": id,
                "season_start_year": int(str(id)[:4]),
                "season_end_year": int(str(id)[4:]),
                "wild_card_in_use": bool(season["wildcardInUse"]),
                "ties_in_use": bool(season["tiesInUse"]),
                "point_for_ot_loss": bool(season["pointForOTLossInUse"]),
                "regular_season_end_date": datetime.fromisoformat(str(season["regularSeasonEndDate"])).date(),
                "playoff_end_date": datetime.fromisoformat(str(season["endDate"])).date(),
            })
    return processed_seasons

# insert seasons data into the database
def insert_seasons_into_db(conn, seasons):
    if not seasons:
        print("No seasons to insert.")
        return
    cur = conn.cursor()
    print(f"Attempting to add {len(seasons)} seasons to the database...")

    try:
        for season in seasons:
            cur.execute("""
                INSERT INTO seasons (
                    id, season_start_year, season_end_year,
                    wild_card_in_use, ties_in_use, point_for_ot_loss,
                    regular_season_end_date, playoff_end_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    season_start_year = EXCLUDED.season_start_year,
                    season_end_year = EXCLUDED.season_end_year,
                    wild_card_in_use = EXCLUDED.wild_card_in_use,
                    ties_in_use = EXCLUDED.ties_in_use,
                    point_for_ot_loss = EXCLUDED.point_for_ot_loss,
                    regular_season_end_date = EXCLUDED.regular_season_end_date,
                    playoff_end_date = EXCLUDED.playoff_end_date;
            """, (
                season["id"], season["season_start_year"], season["season_end_year"],
                season["wild_card_in_use"], season["ties_in_use"], season["point_for_ot_loss"],
                season["regular_season_end_date"], season["playoff_end_date"]
            ))
            print(f"Inserted/Updated season {season['id']}")

            conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error: {e}")
    finally:
        cur.close()

# fetch teams data from NHL API
def get_teams_from_api(base_url, teams_endpoint="stats/rest/en/team"):
    """
    Fetches teams data from the NHL API, processes it, and returns a list of dictionaries.
    """
    url = f"{base_url}/{teams_endpoint}"
    
    try:
        # reuse helper defined in get_seasons_from_api scope
        response = get_with_retry(url)
        response.raise_for_status() # Raise exception for bad status codes
        teams_data = response.json().get("data", [])
    except requests.RequestException as e:
        print(f"Error fetching teams data: {e}")
        return []

    processed_teams = []
    for team in teams_data:
        processed_teams.append({
            "id": team["id"],
            "franchise_id": team["franchiseId"],
            "name": team["fullName"],
            "abbreviation": team["triCode"],
        })
            
    return processed_teams

def insert_teams_into_db(conn, teams):
    """
    Inserts a list of team records into the 'teams' table.
    Use ON CONFLICT (id) DO NOTHING to skip duplicates.
    """
    if not teams:
        print("No teams data to insert.")
        return

    cur = conn.cursor()
    print(f"Attempting to insert {len(teams)} team records...")

    try:
        for team in teams:
            cur.execute("""
                INSERT INTO teams (id, name, abbreviation, franchise_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                team["id"], 
                team["name"], 
                team["abbreviation"], 
                team["franchise_id"]
            ))

        # Commit the transaction once after all inserts/updates
        conn.commit()
        print("Team insertion complete and committed.")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during team insertion: {e}")
    finally:
        cur.close()
        
def get_team_seasons_from_api(conn, base_url):
    """Fetch team-season combinations by querying the DB and validating via API.
    """
    cur = conn.cursor()

    # Get all season and team combinations
    cur.execute("""
    SELECT seasons.id, teams.abbreviation, teams.id
    FROM seasons, teams;
    """)
    season_team_pairs = cur.fetchall()
    cur.close()

    processed_team_seasons = []

    # use a session for connection reuse and to enable retries
    with requests.Session() as session:
        for season_id, abbreviation, team_id in season_team_pairs:
            # skip Utah until 2025, as there is an entry here but no data in the API
            if season_id < 20242025 and abbreviation == "UTA":
                print("Skipping UTA prior to 2025")
                continue

            url = f"{base_url}/v1/roster/{abbreviation}/{season_id}"
            try:
                response = get_with_retry(url, session=session)
            except requests.RequestException as e:
                print(f"Failed request for {abbreviation} in season {season_id}: {e}")
                continue
            if response.status_code != 200:
                print(f"Failed request for {abbreviation} in season {season_id} (status {response.status_code})")
                continue  # Skip invalid responses

            processed_team_seasons.append({
                "team_id": team_id,
                "season_id": season_id
            })

    return processed_team_seasons
    
def insert_team_seasons_into_db(conn, team_seasons):
    """Inserts a list of team season records into the 'team_seasons' table."""
    if not team_seasons:
        print("No team seasons data to insert.")
        return

    cur = conn.cursor()
    print(f"Attempting to insert {len(team_seasons)} team season records...")

    try:
        for ts in team_seasons:
            cur.execute("""
                INSERT INTO team_seasons (team_id, season_id)
                VALUES (%s, %s)
                ON CONFLICT (team_id, season_id) DO NOTHING;
            """, (
                ts["team_id"], 
                ts["season_id"]
            ))

        # Commit the transaction once after all inserts
        conn.commit()
        print("Team seasons insertion complete and committed.")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during team seasons insertion: {e}")
    finally:
        cur.close()

def get_players_from_api(conn, base_url, include_roster_info=False):
    """
    Fetches players (and optional roster fields) from the external API by
    querying team-season combinations from the database.

    If `include_roster_info` is True, each returned player dict will include
    roster-related fields: `team_season_id`, `jersey_number`, `position`,
    `player_height_inches`, and `player_weight_pounds`.
    """
    print("Querying team-season combinations from the database.")

    with conn.cursor() as cur:
        cur.execute("""
        SELECT teams.abbreviation, team_seasons.season_id, team_seasons.id
        FROM teams JOIN team_seasons on teams.id = team_seasons.team_id;
        """)
        season_team_pairs = cur.fetchall()

    print(f"Processing {len(season_team_pairs)} team-season pairs to fetch players.")

    processed_players = []

    with requests.Session() as session:
        for abbreviation, season_id, team_season_id in season_team_pairs:
            url = f"{base_url}/v1/roster/{abbreviation}/{season_id}"

            try:
                response = get_with_retry(url, session=session, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error fetching roster for {abbreviation} in season {season_id}: {e}")
                continue

            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"Invalid JSON response for {abbreviation} in season {season_id}")
                continue

            player_data = (
                data.get("forwards", []) + 
                data.get("defensemen", []) + 
                data.get("goalies", [])
            )

            print(f"Found {len(player_data)} players for {abbreviation} in season {season_id}.")

            for player in player_data:
                first_name = player.get("firstName")
                if isinstance(first_name, dict):
                    first_name = first_name.get("default")
                last_name = player.get("lastName")
                if isinstance(last_name, dict):
                    last_name = last_name.get("default")

                entry = {
                    "player_id": player.get("id"),
                    "first_name": first_name,
                    "last_name": last_name,
                    "birthdate": player.get("birthDate"),
                    "country": player.get("birthCountry"),
                    "shoots_catches": player.get("shootsCatches"),
                }

                if include_roster_info:
                    entry.update({
                        "team_season_id": team_season_id,
                        "jersey_number": player.get("sweaterNumber"),
                        "position": player.get("positionCode"),
                        "player_height_inches": player.get("heightInInches"),
                        "player_weight_pounds": player.get("weightInPounds"),
                    })

                processed_players.append(entry)

    return processed_players

def insert_players_into_db(conn, players):
    """Inserts a list of player records into the 'players' table."""
    if not players:
        print("No player data to insert.")
        return

    cur = conn.cursor()
    print(f"Attempting to insert {len(players)} player records...")

    try:
        for player in players:
            cur.execute("""
                INSERT INTO players (
                    id, first_name, last_name, birthdate, country, shoots_catches
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    birthdate = EXCLUDED.birthdate,
                    country = EXCLUDED.country,
                        shoots_catches = EXCLUDED.shoots_catches
            """, (
                player["player_id"], 
                player["first_name"], 
                player["last_name"], 
                player["birthdate"], 
                player["country"], 
                player["shoots_catches"]
            ))

        # Commit the transaction once after all inserts/updates
        conn.commit()
        print("Player insertion complete and committed.")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during player insertion: {e}")
    finally:
        cur.close()
        
# get_rosters_from_api was merged into get_players_from_api with
# `include_roster_info=True`. Use that to obtain roster-related fields.
    
def insert_rosters_into_db(conn, rosters):
    if not rosters:
        print("No roster data to insert.")
        return  
    cur = conn.cursor()
    
    print(f"Attempting to insert {len(rosters)} roster records...")
    
    try:
        for roster in rosters:
            cur.execute("""
                INSERT INTO rosters (
                    team_season_id, player_id, jersey_number, position,
                    player_height_inches, player_weight_pounds
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_season_id, player_id) DO UPDATE SET
                    jersey_number = EXCLUDED.jersey_number,
                    position = EXCLUDED.position,
                    player_height_inches = EXCLUDED.player_height_inches,
                    player_weight_pounds = EXCLUDED.player_weight_pounds;
            """, (
                roster["team_season_id"], 
                roster["player_id"], 
                roster["jersey_number"], 
                roster["position"], 
                roster["player_height_inches"], 
                roster["player_weight_pounds"]
            ))
    
        # Commit the transaction once after all inserts/updates
        conn.commit()
        print("Roster insertion complete and committed.")
    
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during roster insertion: {e}")
    finally:
        cur.close()
        
def get_standings_from_api(conn, base_url, standings_endpoint="v1/standings/"):
    """Fetch standings data from the NHL API."""
    url = f"{base_url}/{standings_endpoint}"
    
    standings = []
    
    cur = conn.cursor()
    cur.execute("SELECT regular_season_end_date, id AS season_id FROM seasons;")
    # Fetch all rows
    rows = cur.fetchall()

    current_date = datetime.now().date()
    
    # Prepare regular season end dates (adjust logic to use 'now' if end date is after today)
    regular_season_end_dates = []
    
    for row in rows:
        season_end_date = row[0]  # regular_season_end_date is already a date object
        season_id = row[1]
        
        # Use current date if season end date is after today
        if season_end_date > current_date:
            season_end_date = current_date
        
        regular_season_end_dates.append((season_end_date.strftime('%Y-%m-%d'), season_id))
        
    print(regular_season_end_dates)

    for date, season_id in regular_season_end_dates:
        seasonUrl = f"{url}{date}"
        response = requests.get(seasonUrl)

        if response.status_code == 200:
            data = response.json()
            standingsData = data.get("standings", [])

            for team in standingsData:
                wins = team["wins"]
                losses = team["losses"]
                ot = team["otLosses"]
                points = team["points"]
                division_name = team["divisionName"]
                conference_name = team.get("conferenceName")
                abbreviation = team["teamAbbrev"]["default"]

                print(f"Season {season_id}: Conference = {conference_name}")
                
                standings.append({
                    "season_id": season_id,
                    "conference_name": conference_name,
                    "division_name": division_name,
                    "team_abbreviation": abbreviation,
                    "wins": wins,
                    "losses": losses,
                    "ot": ot,
                    "points": points
                })

    return standings

        