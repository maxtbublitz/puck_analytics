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
    insert_rosters_into_db,
    get_standings_from_api,
    insert_standings_into_db,
    get_player_stats_from_api,
    insert_player_stats_into_db,
    get_playoff_data_from_api,
    insert_playoff_data_into_db,
    get_amateur_league_from_api,
    insert_amateur_league_into_db
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
        # Reuse consolidated get_players_from_api with roster info included
        players_with_rosters = get_players_from_api(conn, base_url, include_roster_info=True)
        # Extract roster-specific fields expected by insert_rosters_into_db
        rosters_data = []
        for p in players_with_rosters:
            # Only include entries that have team_season_id and player_id
            if p.get("team_season_id") and p.get("player_id"):
                rosters_data.append({
                    "team_season_id": p.get("team_season_id"),
                    "player_id": p.get("player_id"),
                    "jersey_number": p.get("jersey_number"),
                    "position": p.get("position"),
                    "player_height_inches": p.get("player_height_inches"),
                    "player_weight_pounds": p.get("player_weight_pounds"),
                })

        print(f"API fetched {len(rosters_data)} eligible roster records.")
        insert_rosters_into_db(conn, rosters_data)
        return True
    except Exception as e:
        print(f"❌ Error updating rosters: {e}")
        return False
    
def update_standings(conn, base_url):
    """Fetches and processes standing data from the API."""
    try:
        standings_data = get_standings_from_api(conn, base_url)
        print(f"API fetched {len(standings_data)} eligible standing records.")
        insert_standings_into_db(conn, standings_data)
        return True
    except Exception as e:
        print(f"❌ Error updating standings: {e}")
        return False
    
def update_player_stats(conn, base_url):
    """Fetches and processes player stats data from the API."""
    try:
        player_stats_data = get_player_stats_from_api(conn, base_url)
        print(f"API fetched stats for {len(player_stats_data)} players.")
        insert_player_stats_into_db(conn, player_stats_data)
        return True
    except Exception as e:
        print(f"❌ Error updating player stats: {e}")
        return False
    
def update_playoff_data(conn, base_url, season_range=(20242025, 20252025)):
    """Fetches and processes playoff data from the API."""
    try:
        print("\n--- Starting Playoff Data Update ---")
        playoff_data = get_playoff_data_from_api(conn, base_url, season_range)
        print(f"API fetched playoff data for {len(playoff_data)} seasons.")
        insert_playoff_data_into_db(conn, playoff_data)
        return True
    except Exception as e:
        print(f"❌ Error updating playoff data: {e}")
        return False

def update_amateur_league(conn, base_url):
    """Fetches each player's amateur league from the API and updates the players table."""
    try:
        print("\n--- Starting Amateur League Update ---")
        data = get_amateur_league_from_api(conn, base_url)
        print(f"API found amateur league data for {len(data)} players.")
        insert_amateur_league_into_db(conn, data)
        return True
    except Exception as e:
        print(f"❌ Error updating amateur leagues: {e}")
        return False

def run_update_sequence(target=None):
    """
    Manages connection/cleanup and runs selected data updates.
    ... (rest of the docstring) ...
    """
    
    print(f"Starting update process. Target: {target if target else 'ALL'}")
    load_dotenv()
    stats_url = os.getenv("NHL_API_URL_2")  # https://api.nhle.com  — stats/rest endpoints
    web_url = os.getenv("NHL_API_URL")      # https://api-web.nhle.com — v1 endpoints

    conn = get_db_connection()
    if conn is None:
        return

    # Each entry maps a target name to (function, url) so targeted runs use the right base URL.
    update_map = {
        'seasons':      (update_seasons,      stats_url),
        'teams':        (update_teams,        stats_url),
        'team_seasons': (update_team_seasons, web_url),
        'players':      (update_players,      web_url),
        'rosters':      (update_rosters,      web_url),
        'standings':    (update_standings,    web_url),
        'player_stats': (update_player_stats, web_url),
        'playoff_data':    (update_playoff_data,    web_url),
        'amateur_league':  (update_amateur_league,  web_url),
    }

    try:
        if target and target in update_map:
            fn, url = update_map[target]
            fn(conn, url)
        elif target is None:
            # Run ALL functions sequentially (default behavior)
            print("No specific target provided. Running full update sequence.")
            update_seasons(conn, stats_url)
            update_teams(conn, stats_url)
            update_team_seasons(conn, web_url)
            update_players(conn, web_url)
            update_rosters(conn, web_url)
            update_standings(conn, web_url)
            update_player_stats(conn, web_url)
            update_playoff_data(conn, web_url)
            update_amateur_league(conn, web_url)
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