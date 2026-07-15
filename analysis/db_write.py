import psycopg2

_COLS = [
    "team_season_id", "season_id",
    "avg_age",
    "avg_height_inches", "avg_height_inches_f", "avg_height_inches_d",
    "avg_weight_lbs",    "avg_weight_lbs_f",    "avg_weight_lbs_d",
    "pct_canadian", "pct_american", "pct_european",
    "pct_chl", "pct_qmjhl", "pct_whl", "pct_ohl",
    "pct_defense", "playoff_score", "made_playoffs",
]


def insert_analysis_into_db(conn, profiles_df):
    """Upsert the full analysis_team_season table from the profiles DataFrame."""
    cur = conn.cursor()
    rows = profiles_df[_COLS].itertuples(index=False, name=None)

    try:
        cur.executemany("""
            INSERT INTO analysis_team_season (
                team_season_id, season_id,
                avg_age,
                avg_height_inches, avg_height_inches_f, avg_height_inches_d,
                avg_weight_lbs,    avg_weight_lbs_f,    avg_weight_lbs_d,
                pct_canadian, pct_american, pct_european,
                pct_chl, pct_qmjhl, pct_whl, pct_ohl,
                pct_defense, playoff_score, made_playoffs
            )
            VALUES (%s,%s, %s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s)
            ON CONFLICT (team_season_id) DO UPDATE SET
                season_id           = EXCLUDED.season_id,
                avg_age             = EXCLUDED.avg_age,
                avg_height_inches   = EXCLUDED.avg_height_inches,
                avg_height_inches_f = EXCLUDED.avg_height_inches_f,
                avg_height_inches_d = EXCLUDED.avg_height_inches_d,
                avg_weight_lbs      = EXCLUDED.avg_weight_lbs,
                avg_weight_lbs_f    = EXCLUDED.avg_weight_lbs_f,
                avg_weight_lbs_d    = EXCLUDED.avg_weight_lbs_d,
                pct_canadian        = EXCLUDED.pct_canadian,
                pct_american        = EXCLUDED.pct_american,
                pct_european        = EXCLUDED.pct_european,
                pct_chl             = EXCLUDED.pct_chl,
                pct_qmjhl           = EXCLUDED.pct_qmjhl,
                pct_whl             = EXCLUDED.pct_whl,
                pct_ohl             = EXCLUDED.pct_ohl,
                pct_defense         = EXCLUDED.pct_defense,
                playoff_score       = EXCLUDED.playoff_score,
                made_playoffs       = EXCLUDED.made_playoffs
        """, rows)
        conn.commit()
        print(f"Inserted/updated {len(profiles_df)} rows in analysis_team_season.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during analysis insert: {e}")
    finally:
        cur.close()
