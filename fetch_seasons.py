from dotenv import load_dotenv
from datetime import datetime
import os
import requests
import psycopg2

load_dotenv()

# API path
base_url = os.getenv("NHL_API_URL_2")
seasonEndpoint = "stats/rest/en/season"
url = f"{base_url}/{seasonEndpoint}"

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

# Fetch standings data from NHL API
response = requests.get(url)
seasons_data = response.json()["data"]

for season in seasons_data:
    id =  season["id"]
    if(id >= 20092010):
        print("Adding columns to year ", {id})
        season_start_year = int(str(id)[:4])
        season_end_year = int(str(id)[4:])
        wild_card_in_use = bool(season["wildcardInUse"])
        ties_in_use = bool(season["tiesInUse"])
        point_for_ot_loss = bool(season["pointForOTLossInUse"])
        regular_season_end_date = datetime.fromisoformat(str(season["regularSeasonEndDate"])).date()
        playoff_end_date = datetime.fromisoformat(str(season["endDate"])).date()

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
        id, season_start_year, season_end_year,
        wild_card_in_use, ties_in_use, point_for_ot_loss,
        regular_season_end_date, playoff_end_date
    ))

conn.commit()
cur.close()
conn.close()