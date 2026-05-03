# =============================================================================
# eda_report.py  –  exploratory data analysis → PDF report + filtered CSV
#
# Generates:
#   output/eda_report.pdf              – histograms + summary statistics
#   output/messages_multi_posters.csv  – messages from users who posted > 1 time
#
# Run with:  python src/eda_report.py
# =============================================================================

import os
import re
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
from collections import Counter

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_PATH    = "output/messages_community.csv"
OUTPUT_DIR    = "output"
PDF_PATH      = os.path.join(OUTPUT_DIR, "eda_report_all_users.pdf")
FILTERED_PATH = os.path.join(OUTPUT_DIR, "messages_multi_posters.csv")
PDF_PATH_MULTI = os.path.join(OUTPUT_DIR, "eda_report_multi_posters.pdf")

POSTER_COL  = "PosterID"
TEXT_COL    = "MessageText"
DATE_COL    = "PostDate"
TOPIC_COL   = "ForumTopicID"

# Dutch stopwords for word frequency
STOPWORDS = {
    "de", "het", "een", "en", "van", "in", "is", "ik", "dat", "op",
    "te", "met", "voor", "zijn", "er", "maar", "ook", "als", "aan",
    "niet", "ze", "je", "me", "hij", "we", "bij", "zo", "dan", "nog",
    "wel", "om", "die", "wat", "mij", "dit", "al", "nu", "heb", "was",
    "kan", "meer", "heeft", "hem", "haar", "dit", "hun", "uit", "door"
}

# ── Colour palette ────────────────────────────────────────────────────────────
PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"

# =============================================================================
# Loaders and helpers
# =============================================================================

def load_data() -> pd.DataFrame:
    df = pd.read_csv(INPUT_PATH)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL])
    print(f"Loaded {len(df)} messages from {df[POSTER_COL].nunique()} posters.")
    return df


def _central_tendency(series: pd.Series, label: str) -> dict:
    mode_vals = series.mode()
    return {
        "label":  label,
        "mean":   round(series.mean(), 2),
        "median": round(series.median(), 2),
        "mode":   round(float(mode_vals.iloc[0]), 2) if not mode_vals.empty else None,
        "min":    round(series.min(), 2),
        "max":    round(series.max(), 2),
    }


def _count_word(text: str, word: str) -> int:
    return len(re.findall(rf"\b{word}\b", str(text), flags=re.IGNORECASE))


# =============================================================================
# Plot helpers
# =============================================================================

def _style_ax(ax, title: str, xlabel: str, ylabel: str = "Frequency"):
    ax.set_title(title, fontsize=13, fontweight="bold", color=PRIMARY, pad=10)
    ax.set_xlabel(xlabel, fontsize=10, color="#333333")
    ax.set_ylabel(ylabel, fontsize=10, color="#333333")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CCCCCC")
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.tick_params(colors="#555555")
    ax.set_facecolor(SECONDARY)


def _add_stats_text(ax, stats: dict):
    txt = (
        f"Mean: {stats['mean']}  |  "
        f"Median: {stats['median']}  |  "
        f"Mode: {stats['mode']}"
    )
    ax.text(
        0.98, 0.97, txt,
        transform=ax.transAxes,
        ha="right", va="top",
        fontsize=8, color="#555555",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7)
    )


def _histogram(series: pd.Series, title: str, xlabel: str,
               bins: int = 40, cap_pct: float = 0.99) -> plt.Figure:
    """Single histogram with stats annotation. Caps x-axis at cap_pct percentile."""
    fig, ax = plt.subplots(figsize=(10, 4))
    cap = series.quantile(cap_pct)
    clipped = series.clip(upper=cap)
    ax.hist(clipped, bins=bins, color=PRIMARY, edgecolor="white", linewidth=0.5)
    _style_ax(ax, title, xlabel)
    stats = _central_tendency(series, title)
    _add_stats_text(ax, stats)
    if cap < series.max():
        ax.set_xlabel(f"{xlabel}  (top {int((1-cap_pct)*100)}% capped for readability)",
                      fontsize=10, color="#333333")
    fig.tight_layout()
    return fig


def _bar(labels, values, title: str, xlabel: str,
         ylabel: str = "Post count", rotate: bool = False) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    bars = ax.bar(labels, values, color=PRIMARY, edgecolor="white", linewidth=0.5)
    # Highlight max bar
    max_idx = int(np.argmax(values))
    bars[max_idx].set_color(ACCENT)
    _style_ax(ax, title, xlabel, ylabel)
    if rotate:
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    fig.tight_layout()
    return fig


def _cover_page(title: str, subtitle: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor(PRIMARY)
    ax.set_facecolor(PRIMARY)
    ax.axis("off")
    ax.text(0.5, 0.65, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=22,
            fontweight="bold", color="white")
    ax.text(0.5, 0.45, subtitle, transform=ax.transAxes,
            ha="center", va="center", fontsize=13, color="#DDDDDD")
    return fig


def _stats_table_fig(rows: list[dict]) -> plt.Figure:
    """Renders a summary statistics table as a matplotlib figure."""
    fig, ax = plt.subplots(figsize=(10, max(2, len(rows) * 0.55 + 1)))
    ax.axis("off")
    col_labels = ["Metric", "Mean", "Median", "Mode", "Min", "Max"]
    cell_data = [
        [r["label"], r["mean"], r["median"], r["mode"], r["min"], r["max"]]
        for r in rows
    ]
    tbl = ax.table(
        cellText=cell_data,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)

    # Style header row
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    # Alternate row shading
    for i in range(1, len(cell_data) + 1):
        for j in range(len(col_labels)):
            if i % 2 == 0:
                tbl[(i, j)].set_facecolor("#EEF3F8")

    ax.set_title("Summary Statistics", fontsize=13, fontweight="bold",
                 color=PRIMARY, pad=10)
    fig.tight_layout()
    return fig


# =============================================================================
# Compute all statistics
# =============================================================================

def compute_stats(df: pd.DataFrame) -> dict:
    print("Computing statistics…")

    # Posts per user
    posts_per_user = df.groupby(POSTER_COL).size()

    # Posts per thread
    thread_counts = df.groupby(TOPIC_COL).size()

    # First post per topic → replies
    df_sorted = df.sort_values(DATE_COL)
    first_idx = df_sorted.groupby(TOPIC_COL)[DATE_COL].idxmin()
    df_sorted["role"] = "reply"
    df_sorted.loc[first_idx, "role"] = "post"
    replies_per_thread = (
        df_sorted[df_sorted["role"] == "reply"]
        .groupby(TOPIC_COL).size()
    )

    # Activity span
    span = df.groupby(POSTER_COL)[DATE_COL].agg(
        first_post="min", last_post="max"
    )
    span["span_days"] = (span["last_post"] - span["first_post"]).dt.days
    df_copy = df.copy()
    df_copy["date"] = df_copy[DATE_COL].dt.date
    active_days = df_copy.groupby(POSTER_COL)["date"].nunique()

    # Word counts
    df_copy["word_count"] = df_copy[TEXT_COL].fillna("").apply(
        lambda t: len(str(t).split())
    )
    words_per_user = df_copy.groupby(POSTER_COL)["word_count"].sum()
    words_per_post = df_copy["word_count"]

    # ik and mijn
    df_copy["ik_count"]   = df_copy[TEXT_COL].apply(lambda t: _count_word(t, "ik"))
    df_copy["mijn_count"] = df_copy[TEXT_COL].apply(lambda t: _count_word(t, "mijn"))
    ik_per_user   = df_copy.groupby(POSTER_COL)["ik_count"].sum()
    mijn_per_user = df_copy.groupby(POSTER_COL)["mijn_count"].sum()
    ik_pct_per_user = (
        ik_per_user / words_per_user.clip(lower=1) * 100
    ).round(3)
    mijn_pct_per_user = (
        mijn_per_user / words_per_user.clip(lower=1) * 100
    ).round(3)

    # Time patterns
    hours        = df[DATE_COL].dt.hour.value_counts().sort_index()
    day_order    = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    days         = df[DATE_COL].dt.day_name().value_counts().reindex(day_order)
    month_order  = ["January","February","March","April","May","June",
                    "July","August","September","October","November","December"]
    months       = df[DATE_COL].dt.month_name().value_counts().reindex(month_order)

    return {
        "posts_per_user":      posts_per_user,
        "thread_counts":       thread_counts,
        "replies_per_thread":  replies_per_thread,
        "span_days":           span["span_days"],
        "active_days":         active_days,
        "words_per_user":      words_per_user,
        "words_per_post":      words_per_post,
        "ik_per_user":         ik_per_user,
        "mijn_per_user":       mijn_per_user,
        "ik_pct_per_user":     ik_pct_per_user,
        "mijn_pct_per_user":   mijn_pct_per_user,
        "hours":               hours,
        "days":                days,
        "months":              months,
        "df_copy":             df_copy,
    }


# =============================================================================
# Build PDF
# =============================================================================

def build_pdf(stats: dict, pdf_path: str = None, subtitle: str = "All Users"):
    if pdf_path is None:
        pdf_path = PDF_PATH
    print(f"Building PDF → {pdf_path}")

    with pdf_backend.PdfPages(pdf_path) as pdf:

        # ── Cover ─────────────────────────────────────────────────────────────
        pdf.savefig(_cover_page(
            "Depression Connect Forum",
            f"Exploratory Data Analysis Report — {subtitle}"
        ), bbox_inches="tight")
        plt.close("all")

        # ── Summary statistics table ──────────────────────────────────────────
        summary_rows = [
            _central_tendency(stats["posts_per_user"],     "Posts per user"),
            _central_tendency(stats["thread_counts"],      "Messages per thread (total)"),
            _central_tendency(stats["replies_per_thread"], "Replies per thread"),
            _central_tendency(stats["span_days"],          "Activity span (days)"),
            _central_tendency(stats["active_days"],        "Active days per user"),
            _central_tendency(stats["words_per_post"],     "Words per post"),
            _central_tendency(stats["words_per_user"],     "Words per user (total)"),
            _central_tendency(stats["ik_per_user"],        "'ik' count per user"),
            _central_tendency(stats["ik_pct_per_user"],    "'ik' % per user"),
            _central_tendency(stats["mijn_per_user"],      "'mijn' count per user"),
            _central_tendency(stats["mijn_pct_per_user"],  "'mijn' % per user"),
        ]
        pdf.savefig(_stats_table_fig(summary_rows), bbox_inches="tight")
        plt.close("all")

        # ── Posts per user ────────────────────────────────────────────────────
        pdf.savefig(_histogram(
            stats["posts_per_user"],
            "Distribution: Posts per User",
            "Number of posts"
        ), bbox_inches="tight")
        plt.close("all")

        # ── Messages per thread ───────────────────────────────────────────────
        pdf.savefig(_histogram(
            stats["thread_counts"],
            "Distribution: Total Messages per Thread",
            "Number of messages"
        ), bbox_inches="tight")
        plt.close("all")

        pdf.savefig(_histogram(
            stats["replies_per_thread"],
            "Distribution: Replies per Thread (excluding opening post)",
            "Number of replies"
        ), bbox_inches="tight")
        plt.close("all")

        # ── Activity span ─────────────────────────────────────────────────────
        pdf.savefig(_histogram(
            stats["span_days"],
            "Distribution: User Activity Span (days between first and last post)",
            "Days"
        ), bbox_inches="tight")
        plt.close("all")

        pdf.savefig(_histogram(
            stats["active_days"],
            "Distribution: Active Days per User (distinct days with at least one post)",
            "Days"
        ), bbox_inches="tight")
        plt.close("all")

        # ── Word counts ───────────────────────────────────────────────────────
        pdf.savefig(_histogram(
            stats["words_per_post"],
            "Distribution: Words per Post",
            "Word count"
        ), bbox_inches="tight")
        plt.close("all")

        pdf.savefig(_histogram(
            stats["words_per_user"],
            "Distribution: Total Words per User",
            "Word count"
        ), bbox_inches="tight")
        plt.close("all")

        # ── ik and mijn ───────────────────────────────────────────────────────
        pdf.savefig(_histogram(
            stats["ik_per_user"],
            "Distribution: 'ik' Count per User",
            "Count"
        ), bbox_inches="tight")
        plt.close("all")

        pdf.savefig(_histogram(
            stats["ik_pct_per_user"],
            "Distribution: 'ik' as % of Total Words per User",
            "Percentage (%)"
        ), bbox_inches="tight")
        plt.close("all")

        pdf.savefig(_histogram(
            stats["mijn_per_user"],
            "Distribution: 'mijn' Count per User",
            "Count"
        ), bbox_inches="tight")
        plt.close("all")

        pdf.savefig(_histogram(
            stats["mijn_pct_per_user"],
            "Distribution: 'mijn' as % of Total Words per User",
            "Percentage (%)"
        ), bbox_inches="tight")
        plt.close("all")

        # ── Popular hours ─────────────────────────────────────────────────────
        h = stats["hours"]
        pdf.savefig(_bar(
            [f"{i:02d}:00" for i in h.index],
            h.values,
            "Popular Hours of Day",
            "Hour",
            rotate=True
        ), bbox_inches="tight")
        plt.close("all")

        # ── Popular days ──────────────────────────────────────────────────────
        d = stats["days"]
        pdf.savefig(_bar(
            d.index.tolist(),
            d.values,
            "Popular Days of Week",
            "Day"
        ), bbox_inches="tight")
        plt.close("all")

        # ── Popular months ────────────────────────────────────────────────────
        m = stats["months"]
        pdf.savefig(_bar(
            m.index.tolist(),
            m.values,
            "Popular Months",
            "Month",
            rotate=True
        ), bbox_inches="tight")
        plt.close("all")

    print(f"  PDF saved → {pdf_path}")


# =============================================================================
# Save filtered dataset (exclude single-post users)
# =============================================================================

def save_filtered(df: pd.DataFrame):
    posts_per_user = df.groupby(POSTER_COL).size()
    single_posters = set(posts_per_user[posts_per_user == 1].index)

    filtered = df[~df[POSTER_COL].isin(single_posters)].copy()

    print(f"\nFiltered dataset:")
    print(f"  Single-post users removed: {len(single_posters)}")
    print(f"  Remaining users:  {filtered[POSTER_COL].nunique()}")
    print(f"  Remaining messages: {len(filtered)}")

    filtered.to_csv(FILTERED_PATH, index=False)
    print(f"  Saved → {FILTERED_PATH}")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Report 1: all users ───────────────────────────────────────────────────
    print("\n=== Report 1: All users ===")
    df = load_data()
    stats = compute_stats(df)
    build_pdf(stats, pdf_path=PDF_PATH, subtitle="All Users")

    # ── Save filtered dataset ─────────────────────────────────────────────────
    save_filtered(df)

    # ── Report 2: multi-posters only ─────────────────────────────────────────
    print("\n=== Report 2: Multi-posters only ===")
    df_multi = pd.read_csv(FILTERED_PATH)
    df_multi[DATE_COL] = pd.to_datetime(df_multi[DATE_COL], errors="coerce")
    df_multi = df_multi.dropna(subset=[DATE_COL])
    print(f"Loaded {len(df_multi)} messages from {df_multi[POSTER_COL].nunique()} posters.")
    stats_multi = compute_stats(df_multi)
    build_pdf(stats_multi, pdf_path=PDF_PATH_MULTI, subtitle="Multi-Posters Only")

    print("\n✓ Done.")
    print(f"  {PDF_PATH}")
    print(f"  {PDF_PATH_MULTI}")
    print(f"  {FILTERED_PATH}")
