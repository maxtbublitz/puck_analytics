# Populate divisions and conferences table
# Add record (wins, losses ect) to team seasons table

from dotenv import load_dotenv
from datetime import datetime
import os
import requests
import psycopg2

load_dotenv()

# functions
def get_or_create_conference(cur, name, season_id):
    cur.execute("""
        SELECT id FROM conferences WHERE name = %s AND season_id = %s
    """, (name, season_id))
    result = cur.fetchone()
    if result:
        return result[0]
    cur.execute("""
        INSERT INTO conferences (name, season_id)
        VALUES (%s, %s)
        RETURNING id
    """, (name, season_id))
    return cur.fetchone()[0]

def get_or_create_division(cur, name, conference_id, season_id):
    if conference_id:
        cur.execute("""
            SELECT id FROM divisions WHERE name = %s AND conference_id = %s
        """, (name, conference_id))
    else:
        cur.execute("""
            SELECT id FROM divisions WHERE name = %s AND conference_id IS NULL
        """, (name,))
    
    result = cur.fetchone()
    if result:
        return result[0]

    if conference_id:
        cur.execute("""
            INSERT INTO divisions (name, conference_id, season_id)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (name, conference_id, season_id))
    else:
        cur.execute("""
            INSERT INTO divisions (name, conference_id, season_id)
            VALUES (%s, NULL)
            RETURNING id
        """, (name,))
    
    return cur.fetchone()[0]

def get_team_id(cur, abbreviation):
    cur.execute("SELECT id FROM teams WHERE abbreviation = %s", (abbreviation,))
    result = cur.fetchone()
    return result[0] if result else None


# API path
base_url = os.getenv("NHL_API_URL")
standingsEndpoint = "v1/standings/"
url = f"{base_url}/{standingsEndpoint}"

print(url)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname= os.getenv("DB_NAME"),
    user= os.getenv("DB_USER"),
    password= os.getenv("DB_PASSWORD"),
    host= os.getenv("DB_HOST"),
    port= os.getenv("DB_PORT")
)
cur = conn.cursor()

# Get the seasons table
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

            # add the conference in to the conferences table if it does not yet exist
            # add the division to the divisions table if it does not yet exist
            # add wins, losses, points, ot, and division to the correct team in the team_seasons table
            if conference_name:
                conference_id = get_or_create_conference(cur, conference_name, season_id)
            else:
                conference_id = None  # For seasons like 2020–21
            division_id = get_or_create_division(cur, division_name, conference_id, season_id)
            team_id = get_team_id(cur, abbreviation)

            if team_id:
                cur.execute("""
                    INSERT INTO team_seasons (team_id, season_id, wins, losses, ot, points, division_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (team_id, season_id) DO UPDATE
                    SET wins = EXCLUDED.wins,
                        losses = EXCLUDED.losses,
                        ot = EXCLUDED.ot,
                        points = EXCLUDED.points,
                        division_id = EXCLUDED.division_id
                """, (team_id, season_id, wins, losses, ot, points, division_id))
            else:
                print(f"Team not found for abbreviation: {abbreviation}")

    else:
        print(f"Failed to fetch data for {date} on {seasonUrl}: {response.status_code}")

conn.commit()
cur.close()
conn.close()