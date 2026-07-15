from database.db_utils import get_db_connection
from database.create_schema import create_schema
from analysis.correlations import run_correlations
from analysis.db_write import insert_analysis_into_db
from analysis.visualize import plot_correlations, plot_boxplots, plot_scatter

if __name__ == "__main__":
    conn = get_db_connection()
    create_schema(conn)

    profiles, all_corr, playoff_corr = run_correlations()
    insert_analysis_into_db(conn, profiles)
    conn.close()

    plot_correlations(all_corr,     label="all_teams")
    plot_correlations(playoff_corr, label="playoff_teams")
    plot_boxplots(profiles)

    for feature in ["pct_chl", "pct_canadian", "avg_age", "avg_height_inches", "avg_weight_lbs"]:
        plot_scatter(profiles, feature)
