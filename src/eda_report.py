# =============================================================================
# eda_report.py  –  exploratory data analysis → PDF report + filtered CSV
#
# Produces (per dataset variant):
#   output/eda_report_all_users{_variant}.pdf
#   output/eda_report_multi_posters{_variant}.pdf
#   output/messages_multi_posters{_variant}.csv
#
# The PDF includes standard histograms/bar charts plus a full role-based
# section (word count, popular words, sentence structure, emoji use) via
# role_analysis.py.
#
# Run with:  python src/eda_report.py [--dataset combined|old|new_only]
# =============================================================================

import os
import re
import argparse
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend

from dataset_io import add_dataset_arg, structured_path, variant_path, subtitle_for
from role_analysis import add_role_section_to_pdf
from utils.thread_utils import strip_entity_placeholders_col

warnings.filterwarnings("ignore")

OUTPUT_DIR  = "output"
POSTER_COL  = "PosterID"
TEXT_COL    = "MessageText"
DATE_COL    = "PostDate"
TOPIC_COL   = "ForumTopicID"

PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"


# =============================================================================
# Loaders and helpers
# =============================================================================

def load_data(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL])
    df = strip_entity_placeholders_col(df, TEXT_COL)
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
    txt = (f"Mean: {stats['mean']}  |  Median: {stats['median']}  |  Mode: {stats['mode']}")
    ax.text(0.98, 0.97, txt, transform=ax.transAxes, ha="right", va="top",
            fontsize=8, color="#555555",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7))


def _histogram(series: pd.Series, title: str, xlabel: str,
               bins: int = 40, cap_pct: float = 0.99) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    cap = series.quantile(cap_pct)
    ax.hist(series.clip(upper=cap), bins=bins, color=PRIMARY, edgecolor="white", linewidth=0.5)
    _style_ax(ax, title, xlabel)
    _add_stats_text(ax, _central_tendency(series, title))
    if cap < series.max():
        ax.set_xlabel(f"{xlabel}  (top {int((1-cap_pct)*100)}% capped for readability)",
                      fontsize=10, color="#333333")
    fig.tight_layout()
    return fig


def _bar(labels, values, title: str, xlabel: str,
         ylabel: str = "Post count", rotate: bool = False) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    bars = ax.bar(labels, values, color=PRIMARY, edgecolor="white", linewidth=0.5)
    bars[int(np.argmax(values))].set_color(ACCENT)
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
            ha="center", va="center", fontsize=22, fontweight="bold", color="white")
    ax.text(0.5, 0.45, subtitle, transform=ax.transAxes,
            ha="center", va="center", fontsize=13, color="#DDDDDD")
    return fig


def _stats_table_fig(rows: list[dict]) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, max(2, len(rows) * 0.55 + 1)))
    ax.axis("off")
    col_labels = ["Metric", "Mean", "Median", "Mode", "Min", "Max"]
    cell_data = [[r["label"], r["mean"], r["median"], r["mode"], r["min"], r["max"]] for r in rows]
    tbl = ax.table(cellText=cell_data, colLabels=col_labels, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cell_data) + 1):
        if i % 2 == 0:
            for j in range(len(col_labels)):
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title("Summary Statistics", fontsize=13, fontweight="bold", color=PRIMARY, pad=10)
    fig.tight_layout()
    return fig


# =============================================================================
# Compute statistics
# =============================================================================

def compute_stats(df: pd.DataFrame) -> dict:
    print("Computing statistics…")

    posts_per_user = df.groupby(POSTER_COL).size()
    thread_counts  = df.groupby(TOPIC_COL).size()

    df_sorted = df.sort_values(DATE_COL)
    first_idx = df_sorted.groupby(TOPIC_COL)[DATE_COL].idxmin()
    df_sorted["role"] = "reply"
    df_sorted.loc[first_idx, "role"] = "post"
    replies_per_thread = df_sorted[df_sorted["role"] == "reply"].groupby(TOPIC_COL).size()

    span = df.groupby(POSTER_COL)[DATE_COL].agg(first_post="min", last_post="max")
    span["span_days"] = (span["last_post"] - span["first_post"]).dt.days

    df_copy = df.copy()
    df_copy["date"] = df_copy[DATE_COL].dt.date
    active_days = df_copy.groupby(POSTER_COL)["date"].nunique()

    df_copy["word_count"] = df_copy[TEXT_COL].fillna("").apply(lambda t: len(str(t).split()))
    words_per_user = df_copy.groupby(POSTER_COL)["word_count"].sum()
    words_per_post = df_copy["word_count"]

    df_copy["ik_count"]   = df_copy[TEXT_COL].apply(lambda t: _count_word(t, "ik"))
    df_copy["mijn_count"] = df_copy[TEXT_COL].apply(lambda t: _count_word(t, "mijn"))
    ik_per_user   = df_copy.groupby(POSTER_COL)["ik_count"].sum()
    mijn_per_user = df_copy.groupby(POSTER_COL)["mijn_count"].sum()
    ik_pct_per_user   = (ik_per_user   / words_per_user.clip(lower=1) * 100).round(3)
    mijn_pct_per_user = (mijn_per_user / words_per_user.clip(lower=1) * 100).round(3)

    hours       = df[DATE_COL].dt.hour.value_counts().sort_index()
    day_order   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    days        = df[DATE_COL].dt.day_name().value_counts().reindex(day_order)
    month_order = ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    months      = df[DATE_COL].dt.month_name().value_counts().reindex(month_order)

    return {
        "posts_per_user":     posts_per_user,
        "thread_counts":      thread_counts,
        "replies_per_thread": replies_per_thread,
        "span_days":          span["span_days"],
        "active_days":        active_days,
        "words_per_user":     words_per_user,
        "words_per_post":     words_per_post,
        "ik_per_user":        ik_per_user,
        "mijn_per_user":      mijn_per_user,
        "ik_pct_per_user":    ik_pct_per_user,
        "mijn_pct_per_user":  mijn_pct_per_user,
        "hours":              hours,
        "days":               days,
        "months":             months,
        "df_copy":            df_copy,
    }


# =============================================================================
# Build PDF
# =============================================================================

def build_pdf(stats: dict, pdf_path: str, subtitle: str = "All Users"):
    print(f"Building PDF → {pdf_path}")

    with pdf_backend.PdfPages(pdf_path) as pdf:

        def save(fig):
            pdf.savefig(fig, bbox_inches="tight")
            plt.close("all")

        save(_cover_page("Depression Connect Forum",
                         f"Exploratory Data Analysis Report — {subtitle}"))

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
        save(_stats_table_fig(summary_rows))

        save(_histogram(stats["posts_per_user"],     "Distribution: Posts per User",         "Number of posts"))
        save(_histogram(stats["thread_counts"],      "Distribution: Total Messages per Thread", "Number of messages"))
        save(_histogram(stats["replies_per_thread"], "Distribution: Replies per Thread (excluding opening post)", "Number of replies"))
        save(_histogram(stats["span_days"],          "Distribution: User Activity Span (days between first and last post)", "Days"))
        save(_histogram(stats["active_days"],        "Distribution: Active Days per User (distinct days with at least one post)", "Days"))
        save(_histogram(stats["words_per_post"],     "Distribution: Words per Post",          "Word count"))
        save(_histogram(stats["words_per_user"],     "Distribution: Total Words per User",    "Word count"))
        save(_histogram(stats["ik_per_user"],        "Distribution: 'ik' Count per User",     "Count"))
        save(_histogram(stats["ik_pct_per_user"],    "Distribution: 'ik' as % of Total Words per User", "Percentage (%)"))
        save(_histogram(stats["mijn_per_user"],      "Distribution: 'mijn' Count per User",   "Count"))
        save(_histogram(stats["mijn_pct_per_user"],  "Distribution: 'mijn' as % of Total Words per User", "Percentage (%)"))

        h = stats["hours"]
        save(_bar([f"{i:02d}:00" for i in h.index], h.values, "Popular Hours of Day", "Hour", rotate=True))
        d = stats["days"]
        save(_bar(d.index.tolist(), d.values, "Popular Days of Week", "Day"))
        m = stats["months"]
        save(_bar(m.index.tolist(), m.values, "Popular Months", "Month", rotate=True))

        # ── Role-based section: word count, popular words, sentence structure, emoji ──
        add_role_section_to_pdf(pdf, stats["df_copy"])

    print(f"  PDF saved → {pdf_path}")


# =============================================================================
# Save filtered dataset (exclude single-post users)
# =============================================================================

def save_filtered(df: pd.DataFrame, filtered_path: str) -> pd.DataFrame:
    posts_per_user = df.groupby(POSTER_COL).size()
    single_posters = set(posts_per_user[posts_per_user == 1].index)
    filtered = df[~df[POSTER_COL].isin(single_posters)].copy()
    print(f"\nFiltered: {len(single_posters)} single-post users removed")
    print(f"  Remaining: {filtered[POSTER_COL].nunique()} users, {len(filtered)} messages")
    filtered.to_csv(filtered_path, index=False)
    print(f"  Saved → {filtered_path}")
    return filtered


# =============================================================================
# Main
# =============================================================================

def _parse_args():
    parser = argparse.ArgumentParser(description="Build the EDA PDF report(s).")
    add_dataset_arg(parser)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ds             = args.dataset
    input_path     = structured_path(OUTPUT_DIR, ds)
    pdf_path       = variant_path(OUTPUT_DIR, "eda_report_all_users.pdf",    ds)
    pdf_path_multi = variant_path(OUTPUT_DIR, "eda_report_multi_posters.pdf", ds)
    filtered_path  = variant_path(OUTPUT_DIR, "messages_multi_posters.csv",  ds)
    sub            = subtitle_for(ds)

    print(f"\n=== Report 1: All users ({ds}) ===")
    df = load_data(input_path)
    stats = compute_stats(df)
    build_pdf(stats, pdf_path=pdf_path, subtitle=f"All Users — {sub}")

    df_multi = save_filtered(df, filtered_path)

    print(f"\n=== Report 2: Multi-posters only ({ds}) ===")
    print(f"Loaded {len(df_multi)} messages from {df_multi[POSTER_COL].nunique()} posters.")
    stats_multi = compute_stats(df_multi)
    build_pdf(stats_multi, pdf_path=pdf_path_multi, subtitle=f"Multi-Posters Only — {sub}")

    print("\n✓ Done.")
    print(f"  {pdf_path}")
    print(f"  {pdf_path_multi}")
    print(f"  {filtered_path}")
