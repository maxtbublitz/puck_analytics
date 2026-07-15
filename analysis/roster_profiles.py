import pandas as pd

_QUERY = """
    SELECT
        r.team_season_id,
        ts.season_id,
        AVG(
            CASE WHEN p.birthdate IS NOT NULL
            THEN DATE_PART('year', AGE(MAKE_DATE(s.season_start_year, 10, 1), p.birthdate))
            END
        )                                                                                   AS avg_age,
        AVG(r.player_height_inches)                                                                    AS avg_height_inches,
        AVG(CASE WHEN r.position IN ('C', 'L', 'R') THEN r.player_height_inches END)              AS avg_height_inches_f,
        AVG(CASE WHEN r.position = 'D'              THEN r.player_height_inches END)              AS avg_height_inches_d,
        AVG(r.player_weight_pounds)                                                                AS avg_weight_lbs,
        AVG(CASE WHEN r.position IN ('C', 'L', 'R') THEN r.player_weight_pounds END)              AS avg_weight_lbs_f,
        AVG(CASE WHEN r.position = 'D'              THEN r.player_weight_pounds END)              AS avg_weight_lbs_d,
        COUNT(CASE WHEN p.country = 'CAN'  THEN 1 END)::float / COUNT(*)                   AS pct_canadian,
        COUNT(CASE WHEN p.country = 'USA'  THEN 1 END)::float / COUNT(*)                   AS pct_american,
        COUNT(CASE WHEN p.country NOT IN ('CAN', 'USA') AND p.country IS NOT NULL
                   THEN 1 END)::float / COUNT(*)                                            AS pct_european,
        COUNT(CASE WHEN p.amateur_league IN ('QMJHL', 'WHL', 'OHL') THEN 1 END)::float
            / COUNT(*)                                                                      AS pct_chl,
        COUNT(CASE WHEN p.amateur_league = 'QMJHL' THEN 1 END)::float / COUNT(*)           AS pct_qmjhl,
        COUNT(CASE WHEN p.amateur_league = 'WHL'   THEN 1 END)::float / COUNT(*)           AS pct_whl,
        COUNT(CASE WHEN p.amateur_league = 'OHL'   THEN 1 END)::float / COUNT(*)           AS pct_ohl,
        COUNT(CASE WHEN r.position = 'D' THEN 1 END)::float / COUNT(*)                     AS pct_defense
    FROM rosters r
    JOIN players      p  ON r.player_id      = p.id
    JOIN team_seasons ts ON r.team_season_id  = ts.id
    JOIN seasons      s  ON ts.season_id      = s.id
    GROUP BY r.team_season_id, ts.season_id, s.season_start_year
"""

_COLUMNS = [
    "team_season_id", "season_id",
    "avg_age",
    "avg_height_inches", "avg_height_inches_f", "avg_height_inches_d",
    "avg_weight_lbs",    "avg_weight_lbs_f",    "avg_weight_lbs_d",
    "pct_canadian", "pct_american", "pct_european",
    "pct_chl", "pct_qmjhl", "pct_whl", "pct_ohl",
    "pct_defense",
]


def get_roster_profiles(conn):
    cur = conn.cursor()
    try:
        cur.execute(_QUERY)
        rows = cur.fetchall()
    finally:
        cur.close()
    df = pd.DataFrame(rows, columns=_COLUMNS)
    numeric_cols = _COLUMNS[2:]  # everything after team_season_id and season_id
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df
