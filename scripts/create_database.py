from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()


def create_database():
    db_name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    # Must connect to the postgres maintenance DB to create/drop another database
    conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    try:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        print(f"Dropped existing database '{db_name}' (if it existed).")

        cur.execute(f"""
            CREATE DATABASE "{db_name}"
            ENCODING 'UTF8'
            LC_COLLATE 'C'
            LC_CTYPE 'C'
            TEMPLATE template0
        """)
        print(f"Created database '{db_name}' with UTF-8 encoding.")
    except psycopg2.Error as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    create_database()
