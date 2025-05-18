from dotenv import load_dotenv
import os
import requests
import psycopg2

load_dotenv()

# API path
base_url = os.getenv("NHL_API_URL_2")
endpoint = "stats/rest/en/team"
url = f"{base_url}/{endpoint}"

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
teams_data = response.json()["data"]

for team in teams_data:
    id =  team["id"]
    franchise_id = team["franchiseId"]
    name = team["fullName"]
    abbreviation = team["triCode"]

    # Insert into DB (skip duplicates using api_id)
    cur.execute("""
        INSERT INTO teams (id, name, abbreviation, franchise_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, (id, name, abbreviation, franchise_id))

# Commit and close
conn.commit()
cur.close()
conn.close()

print("Team data inserted.")
