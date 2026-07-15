import pandas as pd
from scipy import stats

from database.db_utils import get_db_connection
from analysis.roster_profiles import get_roster_profiles
from analysis.playoff_scores import get_playoff_scores

FEATURE_COLS = [
    "avg_age",
    "avg_height_inches", "avg_height_inches_f", "avg_height_inches_d",
    "avg_weight_lbs",    "avg_weight_lbs_f",    "avg_weight_lbs_d",
    "pct_canadian", "pct_american", "pct_european",
    "pct_chl", "pct_qmjhl", "pct_whl", "pct_ohl",
    "pct_defense",
]


def _correlate(df, label):
    print(f"\n=== {label} (n={len(df)}) ===")
    print(f"{'Feature':<22} {'Pearson r':>10} {'p':>8}  {'Spearman r':>11} {'p':>8}")
    print("-" * 66)
    results = []
    for col in FEATURE_COLS:
        valid = df[[col, "playoff_score"]].dropna()
        if len(valid) < 10:
            continue
        pr, pp = stats.pearsonr(valid[col], valid["playoff_score"])
        sr, sp = stats.spearmanr(valid[col], valid["playoff_score"])
        sig = "*" if pp < 0.05 else " "
        print(f"{col:<22} {pr:>10.4f} {pp:>7.4f}{sig}  {sr:>11.4f} {sp:>8.4f}")
        results.append({
            "feature": col,
            "pearson_r": pr, "pearson_p": pp,
            "spearman_r": sr, "spearman_p": sp,
            "n": len(valid),
        })
    return pd.DataFrame(results).sort_values("pearson_r", key=abs, ascending=False)


def run_correlations():
    conn = get_db_connection()
    profiles = get_roster_profiles(conn)
    scores, playoff_teams = get_playoff_scores(conn)
    conn.close()

    profiles["playoff_score"] = profiles["team_season_id"].map(scores).fillna(0)
    profiles["made_playoffs"] = profiles["team_season_id"].isin(playoff_teams)

    all_corr     = _correlate(profiles, "All Teams")
    playoff_corr = _correlate(profiles[profiles["made_playoffs"]].copy(), "Playoff Teams Only")

    return profiles, all_corr, playoff_corr


if __name__ == "__main__":
    run_correlations()
