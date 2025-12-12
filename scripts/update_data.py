# scripts/update_data.py (CORRECTED)

from dotenv import load_dotenv
import os
import sys

# Import helper functions
from database.db_utils import get_db_connection
from database.crud import (
    get_seasons_from_api, 
    insert_seasons_into_db,
    get_teams_from_api, 
    insert_teams_into_db,
    get_team_seasons_from_api,
    insert_team_seasons_into_db,
    get_players_from_api,
    insert_players_into_db,
    get_rosters_from_api,
    insert_rosters_into_db
)

def update_seasons(conn, base_url):
    """Handles the entire season data update cycle: fetch and insert."""
    try:
        print("\n--- Starting Seasons Update ---")
        seasons_data = get_seasons_from_api(base_url)
        print(f"API fetched {len(seasons_data)} eligible season records.")
        insert_seasons_into_db(conn, seasons_data)
        return True
    except Exception as e:
        print(f"❌ Error updating seasons: {e}")
        # Note: Rollback is handled inside insert_seasons_into_db
        return False

def update_teams(conn, base_url):
    """Handles the entire team data update cycle: fetch and insert."""
    try:
        print("\n--- Starting Teams Update ---")
        teams_data = get_teams_from_api(base_url)
        print(f"API fetched {len(teams_data)} team records.")
        insert_teams_into_db(conn, teams_data)
        return True
    except Exception as e:
        print(f"❌ Error updating teams: {e}")
        # Note: Rollback is handled inside insert_teams_into_db
        return False
    
def update_team_seasons(conn, base_url):
    """Inserts team seasons based on existing teams and seasons in the database."""
    try:
        print("\n--- Starting Team Season Update ---")
        team_seasons_data = get_team_seasons_from_api(conn, base_url)
        print(f"Processing {len(team_seasons_data)} team-season records.")
        insert_team_seasons_into_db(conn, team_seasons_data)
        return True
    except Exception as e:
        print(f"❌ Error updating team seasons: {e}")
        return False

def update_players(conn, base_url):
    """Fetches and processes player data from the API."""
    try:
        print("\n--- Starting Players Update ---")
        players_data = get_players_from_api(conn, base_url)
        print(f"API fetched {len(players_data)} eligible player records.")
        insert_players_into_db(conn, players_data)
        return True
    except Exception as e:
        print(f"❌ Error updating players: {e}")
        return False

def update_rosters(conn, base_url):
    """Fetches and processes roster data from the API."""
    try:
        print("\n--- Starting Rosters Update ---")
        rosters_data = get_rosters_from_api(conn, base_url)
        print(f"API fetched {len(rosters_data)} eligible roster records.")
        insert_rosters_into_db(conn, rosters_data)
        return True
    except Exception as e:
        print(f"❌ Error updating rosters: {e}")
        return False
        
def run_update_sequence(target=None):
    """
    Manages connection/cleanup and runs selected data updates.
    ... (rest of the docstring) ...
    """
    
    print(f"Starting update process. Target: {target if target else 'ALL'}")
    load_dotenv()
    base_url = os.getenv("NHL_API_URL_2")
    base_url_2 = os.getenv("NHL_API_URL")
    
    conn = get_db_connection()
    if conn is None:
        return

    # A mapping of possible command arguments to their respective functions
    # These names are now defined above.
    update_map = {
        'seasons': update_seasons,
        'teams': update_teams,
        'team_seasons': update_team_seasons,
        'players': update_players,
        'rosters': update_rosters
        # Add future functions here: 'games': update_games,
    }
    
    # ... (rest of the function logic remains the same) ...

    try:
        if target and target in update_map:
            # Run only the specified function
            update_map[target](conn, base_url_2)
        elif target is None:
            # Run ALL functions sequentially (default behavior)
            print("No specific target provided. Running full update sequence.")
            update_seasons(conn, base_url)
            update_teams(conn, base_url)
            update_team_seasons(conn, base_url_2)
            update_players(conn, base_url_2)
            update_rosters(conn, base_url_2)
        else:
            print(f"🛑 Error: Unknown update target '{target}'. Must be one of: {list(update_map.keys())} or left blank.")

    except Exception as e:
        print(f"\n❌ A critical, unexpected error occurred: {e}")
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()
            print("\n✅ Database connection closed. Update process finished.")

# --- ENTRY POINT ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_name = sys.argv[1].lower()
        run_update_sequence(target_name)
    else:
        run_update_sequence()