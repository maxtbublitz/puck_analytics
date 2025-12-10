# database/db_utils.py (CORRECTED)

import os
import psycopg2
from dotenv import load_dotenv

# load environment variables
load_dotenv()

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    
    try: 
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
        
    except psycopg2.Error as e: 
        print(f"Error connecting to the database: {e}")
        return None 