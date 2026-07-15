import psycopg2
from .db_utils import get_db_connection


def create_schema(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seasons (
                id                      INTEGER PRIMARY KEY,
                season_start_year       INTEGER NOT NULL,
                season_end_year         INTEGER NOT NULL,
                wild_card_in_use        BOOLEAN NOT NULL,
                ties_in_use             BOOLEAN NOT NULL,
                point_for_ot_loss       BOOLEAN NOT NULL,
                regular_season_end_date DATE    NOT NULL,
                playoff_end_date        DATE    NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                id           INTEGER PRIMARY KEY,
                name         TEXT    NOT NULL,
                abbreviation TEXT    NOT NULL,
                franchise_id INTEGER NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS conferences (
                id        SERIAL  PRIMARY KEY,
                name      TEXT    NOT NULL,
                season_id INTEGER NOT NULL REFERENCES seasons(id),
                UNIQUE (name, season_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS divisions (
                id             SERIAL  PRIMARY KEY,
                name           TEXT    NOT NULL,
                conference_id  INTEGER REFERENCES conferences(id),
                season_id      INTEGER NOT NULL REFERENCES seasons(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS team_seasons (
                id          SERIAL  PRIMARY KEY,
                team_id     INTEGER NOT NULL REFERENCES teams(id),
                season_id   INTEGER NOT NULL REFERENCES seasons(id),
                wins        INTEGER,
                losses      INTEGER,
                ot          INTEGER,
                points      INTEGER,
                division_id INTEGER REFERENCES divisions(id),
                UNIQUE (team_id, season_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id             INTEGER PRIMARY KEY,
                first_name     TEXT,
                last_name      TEXT,
                birthdate      DATE,
                country        TEXT,
                shoots_catches TEXT,
                amateur_league TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS rosters (
                team_season_id       INTEGER NOT NULL REFERENCES team_seasons(id),
                player_id            INTEGER NOT NULL REFERENCES players(id),
                jersey_number        INTEGER,
                position             TEXT,
                player_height_inches INTEGER,
                player_weight_pounds INTEGER,
                PRIMARY KEY (team_season_id, player_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                player_id      INTEGER  NOT NULL REFERENCES players(id),
                team_season_id INTEGER  NOT NULL REFERENCES team_seasons(id),
                goals          INTEGER,
                assists        INTEGER,
                points         INTEGER,
                plus_minus     INTEGER,
                average_toi    INTERVAL,
                pim            INTEGER,
                games_played   INTEGER,
                UNIQUE (player_id, team_season_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_stats_playoffs (
                player_id      INTEGER  NOT NULL REFERENCES players(id),
                team_season_id INTEGER  NOT NULL REFERENCES team_seasons(id),
                goals          INTEGER,
                assists        INTEGER,
                points         INTEGER,
                plus_minus     INTEGER,
                average_toi    INTERVAL,
                pim            INTEGER,
                games_played   INTEGER,
                UNIQUE (player_id, team_season_id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS playoffs (
                id        SERIAL  PRIMARY KEY,
                season_id INTEGER NOT NULL UNIQUE REFERENCES seasons(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS playoff_series (
                id                   SERIAL  PRIMARY KEY,
                playoff_id           INTEGER REFERENCES playoffs(id),
                season_id            INTEGER NOT NULL REFERENCES seasons(id),
                round                INTEGER NOT NULL,
                home_team_season_id  INTEGER NOT NULL REFERENCES team_seasons(id),
                away_team_season_id  INTEGER NOT NULL REFERENCES team_seasons(id),
                home_team_games_won  INTEGER,
                away_team_games_won  INTEGER,
                series_letter        TEXT    NOT NULL,
                UNIQUE (series_letter, season_id)
            )
        """)

        cur.execute("""
            ALTER TABLE playoff_series
            ADD COLUMN IF NOT EXISTS playoff_id INTEGER REFERENCES playoffs(id)
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS playoff_games (
                id                  SERIAL  PRIMARY KEY,
                playoff_series_id   INTEGER NOT NULL REFERENCES playoff_series(id),
                game_number         INTEGER NOT NULL,
                home_team_season_id INTEGER NOT NULL REFERENCES team_seasons(id),
                away_team_season_id INTEGER NOT NULL REFERENCES team_seasons(id),
                home_score          INTEGER,
                away_score          INTEGER,
                UNIQUE (playoff_series_id, game_number)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS analysis_team_season (
                team_season_id      INTEGER PRIMARY KEY REFERENCES team_seasons(id),
                season_id           INTEGER NOT NULL REFERENCES seasons(id),
                avg_age             NUMERIC(5, 2),
                avg_height_inches   NUMERIC(5, 2),
                avg_height_inches_f NUMERIC(5, 2),
                avg_height_inches_d NUMERIC(5, 2),
                avg_weight_lbs      NUMERIC(6, 2),
                avg_weight_lbs_f    NUMERIC(6, 2),
                avg_weight_lbs_d    NUMERIC(6, 2),
                pct_canadian        NUMERIC(6, 4),
                pct_american        NUMERIC(6, 4),
                pct_european        NUMERIC(6, 4),
                pct_chl             NUMERIC(6, 4),
                pct_qmjhl           NUMERIC(6, 4),
                pct_whl             NUMERIC(6, 4),
                pct_ohl             NUMERIC(6, 4),
                pct_defense         NUMERIC(6, 4),
                playoff_score       INTEGER NOT NULL DEFAULT 0,
                made_playoffs       BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)

        conn.commit()
        print("Schema created successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
    finally:
        cur.close()


if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        create_schema(conn)
        conn.close()
