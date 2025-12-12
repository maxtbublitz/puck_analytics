import requests
import json
import time
import psycopg2
from datetime import datetime

def get_with_retry(url, session=None, max_retries=5, backoff_factor=1, timeout=10):
        """Simple GET with exponential backoff and Retry-After handling.

        Returns the final `requests.Response` (may be non-200 if all retries exhausted).
        """
        sess = session or requests
        for attempt in range(1, max_retries + 1):
            print(f"get_with_retry: attempt {attempt}/{max_retries} GET {url}")
            try:
                resp = sess.get(url, timeout=timeout)
            except requests.RequestException as e:
                print(f"get_with_retry: request exception on attempt {attempt}: {e}")
                if attempt == max_retries:
                    print("get_with_retry: max retries reached, raising")
                    raise
                sleep = backoff_factor * (2 ** (attempt - 1))
                print(f"get_with_retry: sleeping {sleep}s before retry")
                time.sleep(sleep)
                continue

            # Handle rate limit explicitly
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = int(retry_after) if retry_after is not None else backoff_factor * (2 ** (attempt - 1))
                except Exception:
                    wait = backoff_factor * (2 ** (attempt - 1))
                print(f"get_with_retry: 429 received, Retry-After={retry_after}, waiting {wait}s")
                if attempt == max_retries:
                    print("get_with_retry: max retries reached after 429, returning response")
                    return resp
                time.sleep(wait)
                continue

            # Retry on server errors
            if 500 <= resp.status_code < 600 and attempt < max_retries:
                sleep = backoff_factor * (2 ** (attempt - 1))
                print(f"get_with_retry: server error {resp.status_code}, sleeping {sleep}s and retrying")
                time.sleep(sleep)
                continue

            print(f"get_with_retry: success/terminal response status={resp.status_code}")
            return resp

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

def get_players_from_api(conn, base_url):
    """
    Fetches all eligible players from an external API by first querying 
    team-season combinations from the database.
    """
    print("Querying team-season combinations from the database.")
    
    season_team_pairs = []
    # Use 'with' for automatic cursor management
    with conn.cursor() as cur:
        # Correct SQL SELECT order to match unpacking order
        cur.execute("""
        SELECT teams.abbreviation, team_seasons.season_id, team_seasons.id
        FROM teams JOIN team_seasons on teams.id = team_seasons.team_id;
        """)
        season_team_pairs = cur.fetchall()
        
    print(f"Processing {len(season_team_pairs)} team-season pairs to fetch players.")

    processed_players = []
    
    # Unpack variables in the correct order: abbreviation, season_id, team_season_id
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

            # Safely combine player data, defaulting to empty list if keys are missing
            player_data = (
                data.get("forwards", []) + 
                data.get("defensemen", []) + 
                data.get("goalies", [])
            )

            print(f"Found {len(player_data)} players for {abbreviation} in season {season_id}.")

            for player in player_data:
                # Use .get() for all fields to prevent KeyError if data is inconsistent
                # Safely extract nested name fields which may be dicts or simple strings
                first_name = player.get("firstName")
                if isinstance(first_name, dict):
                    first_name = first_name.get("default")
                last_name = player.get("lastName")
                if isinstance(last_name, dict):
                    last_name = last_name.get("default")

                processed_players.append({
                    "player_id": player.get("id"),
                    "first_name": first_name,
                    "last_name": last_name,
                    "birthdate": player.get("birthDate"),
                    "country": player.get("birthCountry"),
                    "shoots_catches": player.get("shootsCatches"),
                    "jersey_number": player.get("sweaterNumber"),
                    "position": player.get("positionCode"),
                    "player_height_inches": player.get("heightInInches"),
                    "player_weight_pounds": player.get("weightInPounds"),
                })
            
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
        
def get_rosters_from_api(conn, base_url):
    """Fetches rosters for all team-season combinations from the NHL API."""
    print("Querying team-season combinations from the database.")
    
    print("Querying team-season combinations from the database.")
    
    season_team_pairs = []
    # Use 'with' for automatic cursor management
    with conn.cursor() as cur:
        # Correct SQL SELECT order to match unpacking order
        cur.execute("""
        SELECT teams.abbreviation, team_seasons.season_id, team_seasons.id
        FROM teams JOIN team_seasons on teams.id = team_seasons.team_id;
        """)
        season_team_pairs = cur.fetchall()
        
    print(f"Processing {len(season_team_pairs)} team-season pairs to fetch players.")
    
    processed_rosters = []
    
    # Unpack variables in the correct order: abbreviation, season_id, team_season_id
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

            # Safely combine player data, defaulting to empty list if keys are missing
            player_data = (
                data.get("forwards", []) + 
                data.get("defensemen", []) + 
                data.get("goalies", [])
            )

            print(f"Found {len(player_data)} players for {abbreviation} in season {season_id}.")

            for player in player_data:
                processed_rosters.append({
                    "team_season_id": team_season_id,
                    "player_id": player.get("id"),
                    "jersey_number": player.get("sweaterNumber"),
                    "position": player.get("positionCode"),
                    "player_height_inches": player.get("heightInInches"),
                    "player_weight_pounds": player.get("weightInPounds"),
                })

    return processed_rosters
    
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
    
    
    
    
        
        