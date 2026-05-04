# =============================================================================
# exploratory_analysis.py  –  unified EDA + CDS prevalence analysis
#
# Replicates and adapts the analyses from:
#   "Prevalence of cognitive distortion markers in a suicide prevention
#    chat service: a mixed-methods study"
#
# Uses the real CDS.py + translations/list_of_CDS_NL.tsv lexicon.
#
# Produces:
#   output/exploratory_report.pdf   – all figures (year + month level)
#   output/cds_scores.csv           – per-message CDS flags
#   output/cds_per_user.csv         – per-user CDS prevalence summary
#
# Directory structure expected:
#   src/
#     exploratory_analysis.py   ← this file
#     CDS.py
#     translations/
#       list_of_CDS_NL.tsv
#   output/
#     messages_community.csv
#
# Run with:  python src/exploratory_analysis.py
# =============================================================================

from __future__ import annotations

import os
import sys
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
from scipy import stats

warnings.filterwarnings("ignore")

CDS_CATEGORY_COLS = [
    "Labeling and mislabeling", "Catastrophizing", "Dichotomous Reasoning",
    "Emotional Reasoning", "Disqualifying the Positive",
    "Magnification and Minimization", "Mental Filtering", "Mindreading",
    "Fortune-telling", "Overgeneralizing", "Personalizing", "Should statements"
]

# ── Make sure CDS.py is importable from src/ ─────────────────────────────────
sys.path.append(os.path.dirname(__file__))
from CDS import process_dataset

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_PATH    = "output/messages_community.csv"
OUTPUT_DIR    = "output"
PDF_PATH      = os.path.join(OUTPUT_DIR, "exploratory_report.pdf")
CDS_PATH      = os.path.join(OUTPUT_DIR, "cds_scores.csv")
USER_CDS_PATH = os.path.join(OUTPUT_DIR, "cds_per_user.csv")

POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
DATE_COL   = "PostDate"
TOPIC_COL  = "ForumTopicID"

# ── Colours ───────────────────────────────────────────────────────────────────
C_POST    = "#2166AC"   # blue  — opening posts
C_REPLY   = "#D6604D"   # red   — replies
PRIMARY   = "#2E5E8E"
ALT_GREY  = "#AAAAAA"


# =============================================================================
# Data loading and preparation
# =============================================================================

def load_data() -> pd.DataFrame:
    df = pd.read_csv(INPUT_PATH)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TEXT_COL]).copy()
    print(f"  Loaded {len(df)} messages from {df[POSTER_COL].nunique()} users.")
    return df


def label_roles(df: pd.DataFrame) -> pd.DataFrame:
    """Labels the first message in each thread as 'post', rest as 'reply'."""
    df = df.copy().sort_values(DATE_COL)
    first_idx = df.groupby(TOPIC_COL)[DATE_COL].idxmin()
    df["role"] = "reply"
    df.loc[first_idx, "role"] = "post"
    return df


def add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"]       = df[DATE_COL].dt.year
    df["year_month"] = df[DATE_COL].dt.to_period("M")
    df["month_dt"]   = df[DATE_COL].dt.to_period("M").dt.to_timestamp()
    return df


# =============================================================================
# CDS scoring — uses the real CDS.py + NL lexicon
# =============================================================================

# AFTER
def compute_cds(df: pd.DataFrame) -> pd.DataFrame:
    print("  Running CDS scoring (this may take a few minutes)…")

    tweets = pd.DataFrame({"text": df[TEXT_COL].fillna("").str.lower().values})

    cds_phrases, cds_per_category, cds_per_tweet = process_dataset(
        tweets, output="all_variants", language="NL"
    )

    df = df.reset_index(drop=True)
    df["CDS"] = cds_per_tweet["CDS"].values
    for col in cds_per_category.columns:
        df[col] = cds_per_category[col].values   # ← loop body ends here

    # These three lines are outside the loop
    overall = df["CDS"].mean() * 100
    print(f"  Overall CDS prevalence: {overall:.2f}%")
    return df


# =============================================================================
# Figure helpers
# =============================================================================

def _style_ax(ax, title, xlabel, ylabel, fontsize=10):
    ax.set_title(title, fontsize=fontsize, fontweight="bold", color=PRIMARY, pad=8)
    ax.set_xlabel(xlabel, fontsize=fontsize - 2, color="#333333")
    ax.set_ylabel(ylabel, fontsize=fontsize - 2, color="#333333")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(colors="#555555", labelsize=fontsize - 3)


def _cover_page(title, subtitle):
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(PRIMARY)
    ax.set_facecolor(PRIMARY)
    ax.axis("off")
    ax.text(0.5, 0.65, title, transform=ax.transAxes,
            ha="center", fontsize=20, fontweight="bold", color="white")
    ax.text(0.5, 0.45, subtitle, transform=ax.transAxes,
            ha="center", fontsize=12, color="#DDDDDD")
    return fig


def _section_divider(title):
    fig, ax = plt.subplots(figsize=(10, 1.5))
    fig.patch.set_facecolor("#EEF3F8")
    ax.set_facecolor("#EEF3F8")
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=14,
            fontweight="bold", color=PRIMARY)
    return fig


# =============================================================================
# Section 1 — Basic time series (from plotsperyear.py + plot_messages.py)
# Year-level and month-level
# =============================================================================

def fig_messages_per_year(df: pd.DataFrame) -> plt.Figure:
    per_year = df.groupby("year").size()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(per_year.index, per_year.values, marker="o",
            color=PRIMARY, linewidth=2)
    ax.fill_between(per_year.index, per_year.values, alpha=0.12, color=PRIMARY)
    _style_ax(ax, "Messages per Year", "Year", "Number of messages")
    fig.tight_layout()
    return fig


def fig_messages_per_month(df: pd.DataFrame) -> plt.Figure:
    per_month = df.groupby("month_dt").size()
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(per_month.index, per_month.values,
            color=PRIMARY, linewidth=1.5)
    ax.fill_between(per_month.index, per_month.values, alpha=0.12, color=PRIMARY)
    _style_ax(ax, "Messages per Month", "Month", "Number of messages")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


def fig_users_topics_per_year(df: pd.DataFrame) -> plt.Figure:
    users  = df.groupby("year")[POSTER_COL].nunique()
    topics = df.groupby("year")[TOPIC_COL].nunique()
    msg_per_user  = df.groupby("year").size() / users
    msg_per_topic = df.groupby("year").size() / topics

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    for ax, data, title, ylabel in [
        (axes[0, 0], users,         "Unique Users per Year",         "Users"),
        (axes[0, 1], topics,        "Unique Topics per Year",        "Topics"),
        (axes[1, 0], msg_per_user,  "Messages per User per Year",    "Msg / user"),
        (axes[1, 1], msg_per_topic, "Messages per Topic per Year",   "Msg / topic"),
    ]:
        ax.plot(data.index, data.values, marker="o", color=PRIMARY, linewidth=2)
        ax.fill_between(data.index, data.values, alpha=0.12, color=PRIMARY)
        _style_ax(ax, title, "Year", ylabel)

    fig.suptitle("Forum Activity — Year-level Metrics",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def fig_users_topics_per_month(df: pd.DataFrame) -> plt.Figure:
    users  = df.groupby("month_dt")[POSTER_COL].nunique()
    topics = df.groupby("month_dt")[TOPIC_COL].nunique()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), sharex=True)

    ax1.plot(users.index, users.values, color=C_POST, linewidth=1.5)
    ax1.fill_between(users.index, users.values, alpha=0.12, color=C_POST)
    _style_ax(ax1, "Unique Users per Month", "", "Users")

    ax2.plot(topics.index, topics.values, color=C_REPLY, linewidth=1.5)
    ax2.fill_between(topics.index, topics.values, alpha=0.12, color=C_REPLY)
    _style_ax(ax2, "Unique Topics per Month", "Month", "Topics")
    ax2.tick_params(axis="x", rotation=30)

    fig.suptitle("Forum Activity — Month-level Metrics",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def fig_volume_by_role_year(df: pd.DataFrame) -> plt.Figure:
    pivot = (
        df.groupby(["year", "role"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    fig, ax = plt.subplots(figsize=(10, 4))
    if "post" in pivot.columns:
        ax.plot(pivot["year"], pivot["post"], marker="o",
                color=C_POST, linewidth=2, label="Opening posts")
    if "reply" in pivot.columns:
        ax.plot(pivot["year"], pivot["reply"], marker="s",
                color=C_REPLY, linewidth=2, label="Replies")
    ax.legend(fontsize=9)
    _style_ax(ax, "Message Volume by Role — Year Level",
              "Year", "Number of messages")
    fig.tight_layout()
    return fig


def fig_volume_by_role_month(df: pd.DataFrame) -> plt.Figure:
    pivot = (
        df.groupby(["month_dt", "role"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    fig, ax = plt.subplots(figsize=(12, 4))
    if "post" in pivot.columns:
        ax.plot(pivot["month_dt"], pivot["post"],
                color=C_POST, linewidth=1.5, label="Opening posts")
    if "reply" in pivot.columns:
        ax.plot(pivot["month_dt"], pivot["reply"],
                color=C_REPLY, linewidth=1.5, label="Replies")
    ax.legend(fontsize=9)
    _style_ax(ax, "Message Volume by Role — Month Level",
              "Month", "Number of messages")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


# =============================================================================
# Section 2 — CDS prevalence over time (Figure 1 of manuscript)
# =============================================================================

def fig_cds_volume_and_prevalence_month(df: pd.DataFrame) -> plt.Figure:
    """Replicates Figure 1 — volume + CDS prevalence side by side."""
    monthly = df.groupby("month_dt").agg(
        total=("CDS", "count"),
        cds_matches=("CDS", "sum"),
    ).reset_index()
    monthly["prevalence"] = monthly["cds_matches"] / monthly["total"]
    mean_prev = monthly["prevalence"].mean() * 100

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 4))

    ax1.plot(monthly["month_dt"], monthly["total"],
             color=C_POST, linewidth=1.5)
    ax1.fill_between(monthly["month_dt"], monthly["total"],
                     alpha=0.12, color=C_POST)
    _style_ax(ax1, "Monthly Message Volume", "Month", "Messages")
    ax1.tick_params(axis="x", rotation=30)

    ax2.plot(monthly["month_dt"], monthly["prevalence"] * 100,
             color=C_REPLY, linewidth=1.5)
    ax2.fill_between(monthly["month_dt"], monthly["prevalence"] * 100,
                     alpha=0.12, color=C_REPLY)
    ax2.axhline(mean_prev, color=ALT_GREY, linestyle="--", linewidth=1,
                label=f"Mean: {mean_prev:.1f}%")
    ax2.legend(fontsize=8)
    _style_ax(ax2, "Monthly CDS Prevalence (%)", "Month", "CDS prevalence (%)")
    ax2.tick_params(axis="x", rotation=30)

    fig.suptitle("Message Volume and CDS Prevalence Over Time (Monthly)",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def fig_cds_prevalence_year(df: pd.DataFrame) -> plt.Figure:
    """Year-level CDS prevalence for posts vs replies."""
    yearly = df.groupby(["year", "role"]).agg(
        total=("CDS", "count"),
        cds_matches=("CDS", "sum"),
    ).reset_index()
    yearly["prevalence"] = yearly["cds_matches"] / yearly["total"] * 100

    fig, ax = plt.subplots(figsize=(10, 4))
    for role, color, label in [
        ("post",  C_POST,  "Opening posts"),
        ("reply", C_REPLY, "Replies"),
    ]:
        sub = yearly[yearly["role"] == role]
        ax.plot(sub["year"], sub["prevalence"], marker="o",
                color=color, linewidth=2, label=label)
    ax.legend(fontsize=9)
    _style_ax(ax, "CDS Prevalence by Role — Year Level",
              "Year", "CDS prevalence (%)")
    fig.tight_layout()
    return fig


def fig_cds_prevalence_month_by_role(df: pd.DataFrame) -> plt.Figure:
    """Month-level CDS prevalence for posts vs replies."""
    monthly = df.groupby(["month_dt", "role"]).agg(
        total=("CDS", "count"),
        cds_matches=("CDS", "sum"),
    ).reset_index()
    monthly["prevalence"] = monthly["cds_matches"] / monthly["total"] * 100

    fig, ax = plt.subplots(figsize=(12, 4))
    for role, color, label in [
        ("post",  C_POST,  "Opening posts"),
        ("reply", C_REPLY, "Replies"),
    ]:
        sub = monthly[monthly["role"] == role]
        ax.plot(sub["month_dt"], sub["prevalence"],
                color=color, linewidth=1.5, label=label)
        mean_val = sub["prevalence"].mean()
        ax.axhline(mean_val, color=color, linestyle=":",
                   linewidth=1, alpha=0.6,
                   label=f"{label} mean: {mean_val:.1f}%")
    ax.legend(fontsize=8)
    _style_ax(ax, "CDS Prevalence by Role — Month Level",
              "Month", "CDS prevalence (%)")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


# =============================================================================
# Section 3 — CDS prevalence by category (Figure 2 of manuscript)
# =============================================================================

def fig_cds_by_category(df: pd.DataFrame) -> plt.Figure:
    """
    Grouped bar chart — CDS prevalence per category for posts vs replies.
    Sorted by prevalence ratio (descending), matching Figure 2 of manuscript.
    """
    cds_cols = [c for c in CDS_CATEGORY_COLS if c in df.columns]
    if not cds_cols:
        print("  WARNING: no CDS category columns found in DataFrame, skipping.")
        fig, ax = plt.subplots(figsize=(10, 3))
        ax.text(0.5, 0.5, "No CDS category columns found",
                ha="center", va="center", transform=ax.transAxes)
        ax.axis("off")
        return fig

    posts   = df[df["role"] == "post"]
    replies = df[df["role"] == "reply"]

    rows = []
    for col in cds_cols:
        cat   = col
        p_post  = posts[col].mean()   if len(posts)   > 0 else 0
        p_reply = replies[col].mean() if len(replies) > 0 else 0
        ratio   = (p_post / p_reply) if p_reply > 1e-9 else (
            float("inf") if p_post > 0 else 1.0
        )
        rows.append({"category": cat, "post": p_post,
                     "reply": p_reply, "ratio": ratio})

    res = (
        pd.DataFrame(rows)
        .sort_values("ratio", ascending=False)
        .reset_index(drop=True)
    )

    fig, ax = plt.subplots(figsize=(14, 5))
    x     = np.arange(len(res))
    width = 0.35

    ax.bar(x - width/2, res["post"]  * 100, width,
           label="Opening posts", color=C_POST,  alpha=0.85)
    ax.bar(x + width/2, res["reply"] * 100, width,
           label="Replies",        color=C_REPLY, alpha=0.85)

    for i, row in res.iterrows():
        r_str = (f"{row.ratio:.2f}×"
                 if row.ratio != float("inf") else "∞")
        arrow = "↑" if row.ratio >= 1 else "↓"
        top   = max(row.post, row.reply) * 100 + 0.1
        ax.text(i, top, f"{arrow}{r_str}",
                ha="center", va="bottom", fontsize=6.5, color="#333333")

    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(res["category"], rotation=35, ha="right", fontsize=8)
    ax.legend(fontsize=9)
    _style_ax(ax,
              "CDS Prevalence by Category — Posts vs Replies\n"
              "(sorted by prevalence ratio, log scale)",
              "CDS Category", "CDS prevalence (%, log scale)")
    fig.tight_layout()
    return fig


# =============================================================================
# Section 4 — Within-user CDS distribution (Figure 3 of manuscript)
# =============================================================================

def fig_cds_distribution_kde(df: pd.DataFrame) -> plt.Figure:
    """KDE + histograms of within-user CDS prevalence, posts vs replies."""
    from scipy.stats import gaussian_kde

    posts_u  = df[df["role"] == "post"].groupby(POSTER_COL)["CDS"].mean()
    reply_u  = df[df["role"] == "reply"].groupby(POSTER_COL)["CDS"].mean()

    fig, axes = plt.subplots(1, 3, figsize=(14, 4))

    # KDE
    ax = axes[0]
    for data, color, label in [
        (posts_u,  C_POST,  f"Posts (N={len(posts_u)})"),
        (reply_u,  C_REPLY, f"Replies (N={len(reply_u)})"),
    ]:
        if len(data) > 1:
            kde = gaussian_kde(data)
            x   = np.linspace(0, 1, 300)
            ax.plot(x, kde(x), color=color, linewidth=2, label=label)
            ax.axvline(data.mean(), color=color, linestyle="--",
                       linewidth=1, alpha=0.7)
    ax.legend(fontsize=8)
    _style_ax(ax, "KDE: Within-user CDS Prevalence",
              "CDS prevalence", "Density")

    # Histograms
    for i, (data, color, label) in enumerate([
        (posts_u,  C_POST,  "Posts"),
        (reply_u,  C_REPLY, "Replies"),
    ]):
        ax = axes[i + 1]
        ax.hist(data, bins=30, color=color, alpha=0.75, edgecolor="white")
        ax.axvline(data.mean(), color="#333333", linestyle="--",
                   linewidth=1.2,
                   label=f"Mean: {data.mean():.3f}")
        ax.legend(fontsize=8)
        _style_ax(ax, f"Distribution: {label}",
                  "CDS prevalence", "Number of users")

    fig.suptitle("Within-User CDS Prevalence Distribution",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


# =============================================================================
# Section 5 — CDS by category over time (Figure S1 of manuscript)
# =============================================================================

def fig_cds_category_over_time(df: pd.DataFrame,
                                granularity: str = "month") -> plt.Figure:
    """
    Small multiples — one subplot per CDS category showing prevalence over time.
    granularity: 'year' or 'month'
    """
    cds_cols = [c for c in CDS_CATEGORY_COLS if c in df.columns]
    if not cds_cols:
        print("  WARNING: no CDS category columns found, skipping.")
        fig, ax = plt.subplots(figsize=(10, 3))
        ax.text(0.5, 0.5, "No CDS category columns found",
                ha="center", va="center", transform=ax.transAxes)
        ax.axis("off")
        return fig

    time_col = "year" if granularity == "year" else "month_dt"
    xlabel   = "Year"  if granularity == "year" else "Month"

    n    = len(cds_cols)
    cols = 3
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols,
                             figsize=(14, rows * 2.8),
                             sharex=False)
    axes_flat = axes.flatten()

    for i, col in enumerate(cds_cols):
        ax  = axes_flat[i]
        cat = col.replace("cds_", "").replace("_", " ").title()

        monthly = df.groupby(time_col).agg(
            total=(col, "count"),
            matches=(col, "sum"),
        ).reset_index()
        monthly["prevalence"] = monthly["matches"] / monthly["total"] * 100

        ax.plot(monthly[time_col],
                monthly["prevalence"],
                color=PRIMARY, linewidth=1.2)
        ax.fill_between(monthly[time_col],
                        monthly["prevalence"],
                        alpha=0.12, color=PRIMARY)
        ax.set_title(cat, fontsize=8, fontweight="bold", color=PRIMARY)
        ax.tick_params(axis="x", rotation=30, labelsize=6)
        ax.tick_params(axis="y", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    for j in range(len(cds_cols), len(axes_flat)):
        axes_flat[j].set_visible(False)

    level = "Year" if granularity == "year" else "Month"
    fig.suptitle(f"CDS Prevalence per Category Over Time ({level} Level)",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


# =============================================================================
# Statistical tests (Table 4 of manuscript)
# =============================================================================

def run_statistical_tests(df: pd.DataFrame):
    posts_u  = df[df["role"] == "post"].groupby(POSTER_COL)["CDS"].mean()
    reply_u  = df[df["role"] == "reply"].groupby(POSTER_COL)["CDS"].mean()

    sep = "\n" + "─" * 60
    print(sep)
    print("STATISTICAL TESTS — CDS PREVALENCE: POSTS vs REPLIES")
    print(f"  Posts   — mean: {posts_u.mean():.4f}  SD: {posts_u.std():.4f}  N: {len(posts_u)}")
    print(f"  Replies — mean: {reply_u.mean():.4f}  SD: {reply_u.std():.4f}  N: {len(reply_u)}")

    b_stat, b_p = stats.bartlett(posts_u, reply_u)
    t_stat, t_p = stats.ttest_ind(posts_u, reply_u, equal_var=False)
    pooled_sd   = np.sqrt((posts_u.std()**2 + reply_u.std()**2) / 2)
    cohens_d    = (posts_u.mean() - reply_u.mean()) / pooled_sd if pooled_sd > 0 else 0

    print(f"\n  Bartlett's test: T={b_stat:.2f}, p={b_p:.4f}")
    print(f"  Welch's t-test:  t={t_stat:.2f}, p={t_p:.4f}, Cohen's d={cohens_d:.3f}")


# =============================================================================
# Per-user CDS summary
# =============================================================================

def compute_user_cds(df: pd.DataFrame) -> pd.DataFrame:
    cds_cols = [c for c in CDS_CATEGORY_COLS if c in df.columns]
    agg = {col: "mean" for col in cds_cols}
    agg["CDS"]     = "mean"
    agg[TEXT_COL]  = "count"

    per_user = (
        df.groupby([POSTER_COL, "role"])
        .agg(agg)
        .reset_index()
        .rename(columns={TEXT_COL: "message_count"})
    )
    return per_user


# =============================================================================
# Main
# =============================================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading data…")
    df = load_data()

    print("\nLabelling thread roles…")
    df = label_roles(df)

    print("\nAdding time columns…")
    df = add_time_columns(df)

    print("\nComputing CDS scores…")
    df = compute_cds(df)

    # Save scored messages
    df.to_csv(CDS_PATH, index=False)
    print(f"  CDS scores saved → {CDS_PATH}")

    # Per-user summary
    user_cds = compute_user_cds(df)
    user_cds.to_csv(USER_CDS_PATH, index=False)
    print(f"  Per-user CDS saved → {USER_CDS_PATH}")

    # Statistical tests
    run_statistical_tests(df)

    # ── Build PDF ─────────────────────────────────────────────────────────────
    print(f"\nBuilding PDF → {PDF_PATH}")
    with pdf_backend.PdfPages(PDF_PATH) as pdf:

        def save(fig):
            pdf.savefig(fig, bbox_inches="tight")
            plt.close("all")

        save(_cover_page(
            "Depression Connect Forum",
            "Exploratory Analysis — Time Series & CDS Prevalence"
        ))

        # ── Section 1: Basic activity metrics ────────────────────────────────
        save(_section_divider("Section 1 — Forum Activity Over Time"))
        print("  Section 1: activity metrics")

        save(fig_messages_per_year(df))
        save(fig_messages_per_month(df))
        save(fig_users_topics_per_year(df))
        save(fig_users_topics_per_month(df))
        save(fig_volume_by_role_year(df))
        save(fig_volume_by_role_month(df))

        # ── Section 2: CDS prevalence over time ──────────────────────────────
        save(_section_divider("Section 2 — CDS Prevalence Over Time"))
        print("  Section 2: CDS over time")

        save(fig_cds_volume_and_prevalence_month(df))
        save(fig_cds_prevalence_year(df))
        save(fig_cds_prevalence_month_by_role(df))

        # ── Section 3: CDS by category ───────────────────────────────────────
        save(_section_divider("Section 3 — CDS Prevalence by Category"))
        print("  Section 3: CDS by category")

        save(fig_cds_by_category(df))

        # ── Section 4: Within-user distribution ──────────────────────────────
        save(_section_divider("Section 4 — Within-User CDS Distribution"))
        print("  Section 4: CDS distribution")

        save(fig_cds_distribution_kde(df))

        # ── Section 5: CDS by category over time ─────────────────────────────
        save(_section_divider("Section 5 — CDS per Category Over Time"))
        print("  Section 5: CDS per category over time")

        save(fig_cds_category_over_time(df, granularity="year"))
        save(fig_cds_category_over_time(df, granularity="month"))

    print(f"\n✓ Done.")
    print(f"  {PDF_PATH}")
    print(f"  {CDS_PATH}")
    print(f"  {USER_CDS_PATH}")


if __name__ == "__main__":
    main()
