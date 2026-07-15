def get_playoff_scores(conn):
    """
    Scoring: 0 = missed playoffs, 1 = qualified, +2 per series win (round-agnostic).
    Max score: 9 (qualified + 4 wins = Cup champion).

    Returns:
        scores        dict  team_season_id -> playoff score
        playoff_teams set   all team_season_ids that appeared in any series
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT home_team_season_id, away_team_season_id,
                   home_team_games_won, away_team_games_won
            FROM playoff_series
            WHERE home_team_games_won IS NOT NULL
              AND away_team_games_won IS NOT NULL
        """)
        rows = cur.fetchall()
    finally:
        cur.close()

    scores = {}
    playoff_teams = set()

    for home_id, away_id, home_wins, away_wins in rows:
        # 1 point for qualifying — awarded on first appearance
        for team_id in (home_id, away_id):
            if team_id not in playoff_teams:
                scores[team_id] = scores.get(team_id, 0) + 1
            playoff_teams.add(team_id)

        if home_wins == 4:
            scores[home_id] = scores.get(home_id, 0) + 2
        elif away_wins == 4:
            scores[away_id] = scores.get(away_id, 0) + 2

    return scores, playoff_teams
