from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

OUTPUT_DIR = Path("analysis/output")

_RESULT_ORDER = [
    "Missed Playoffs", "R1 Exit", "R2 Exit",
    "Conf Finals Exit", "Finals Exit", "Champion",
]

# Scoring: 1 (qualified) + 2 per series win
# R1 exit=1, R2 exit=3, CF exit=5, Finals exit=7, Champion=9
_SCORE_TO_RESULT = {1: "R1 Exit", 3: "R2 Exit", 5: "Conf Finals Exit", 7: "Finals Exit", 9: "Champion"}


def _categorize(row):
    if not row["made_playoffs"]:
        return "Missed Playoffs"
    return _SCORE_TO_RESULT.get(row["playoff_score"], "Other")


def _ensure_output():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def plot_correlations(corr_df, label="all"):
    _ensure_output()
    sorted_df = corr_df.sort_values("pearson_r")
    colors = ["steelblue" if r >= 0 else "tomato" for r in sorted_df["pearson_r"]]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(sorted_df["feature"], sorted_df["pearson_r"], color=colors)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Pearson r")
    ax.set_title(f"Roster Attribute Correlation with Playoff Score ({label})")
    plt.tight_layout()

    path = OUTPUT_DIR / f"correlations_{label}.png"
    plt.savefig(path, dpi=150)
    print(f"Saved {path}")
    plt.close()


def plot_boxplots(profiles_df, features=None):
    _ensure_output()
    if features is None:
        features = [
            "avg_age",
            "avg_height_inches", "avg_height_inches_f", "avg_height_inches_d",
            "avg_weight_lbs",    "avg_weight_lbs_f",    "avg_weight_lbs_d",
            "pct_chl", "pct_canadian", "pct_european",
        ]

    df = profiles_df.copy()
    df["result"] = df.apply(_categorize, axis=1)
    present_order = [o for o in _RESULT_ORDER if o in df["result"].unique()]

    for col in features:
        fig, ax = plt.subplots(figsize=(11, 5))
        sns.boxplot(data=df, x="result", y=col, order=present_order, ax=ax)
        ax.set_title(f"{col} by Playoff Result")
        ax.set_xlabel("")
        plt.tight_layout()
        path = OUTPUT_DIR / f"boxplot_{col}.png"
        plt.savefig(path, dpi=150)
        print(f"Saved {path}")
        plt.close()


def plot_scatter(profiles_df, feature):
    _ensure_output()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(profiles_df[feature], profiles_df["playoff_score"], alpha=0.3, s=15)
    ax.set_xlabel(feature)
    ax.set_ylabel("Playoff Score")
    ax.set_title(f"{feature} vs Playoff Score")
    plt.tight_layout()
    path = OUTPUT_DIR / f"scatter_{feature}.png"
    plt.savefig(path, dpi=150)
    print(f"Saved {path}")
    plt.close()
