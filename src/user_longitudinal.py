"""
Per-user LIWC and CDS scores over time for the top N most active posters.

Usage:
    python src/user_longitudinal.py
    python src/user_longitudinal.py --dataset old --top 5
    python src/user_longitudinal.py --input output/messages_structured.csv
"""
from __future__ import annotations

import os
import argparse
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend

from utils.thread_utils import label_roles
from liwc_analysis import load_liwc, score_messages, ensure_fps
from dataset_io import add_dataset_arg, structured_path, variant_path

DATE_COL   = "PostDate"
POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
OUTPUT_DIR = "output"
LIWC_PATH  = "src/liwc15.dic"

PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
PALETTE   = ["#2E5E8E", "#E8A838", "#5A9E6F", "#C0392B", "#8E44AD"]


# =============================================================================
# Data loading
# =============================================================================

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    return df.dropna(subset=[DATE_COL])


def get_top_posters(df: pd.DataFrame, n: int = 5) -> list[str]:
    return df[POSTER_COL].value_counts().head(n).index.tolist()


# =============================================================================
# Scoring
# =============================================================================

def score_cds(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Returns per-message CDS category flags with PosterID and PostDate."""
    from utils.CDS import process_dataset

    working = df[[POSTER_COL, DATE_COL, TEXT_COL]].copy()
    working["text"] = working[TEXT_COL].fillna("").str.lower()

    cds_cats = process_dataset(working, output="per_category", language="NL")
    cds_cats = cds_cats.reset_index(drop=True)
    meta = working[[POSTER_COL, DATE_COL]].reset_index(drop=True)
    result = pd.concat([meta, cds_cats], axis=1)
    cat_cols = list(cds_cats.columns)
    return result, cat_cols


def score_liwc(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Returns LIWC percentage columns per message with PosterID and PostDate."""
    if not os.path.exists(LIWC_PATH):
        print(f"  LIWC dictionary not found at {LIWC_PATH} — skipping LIWC plots.")
        return pd.DataFrame(), []

    term_to_categories, category_map = load_liwc(LIWC_PATH)
    all_categories = sorted(set(category_map.values()))
    term_to_categories, all_categories = ensure_fps(term_to_categories, all_categories)
    scored, liwc_cols = score_messages(df, term_to_categories, all_categories)

    pct_cols = [c + "_pct" for c in liwc_cols]
    # Prioritise first-person singular; fill remaining slots by prevalence
    fps_priority = [f"liwc_{c}_pct" for c in ("i", "fps_dutch")]
    fps_present  = [c for c in fps_priority if c in pct_cols]
    others = [
        c for c in scored[pct_cols].mean().sort_values(ascending=False).index
        if c not in fps_present
    ]
    top_pct_cols = (fps_present + others)[:8]
    return scored[[POSTER_COL, DATE_COL] + top_pct_cols], top_pct_cols


# =============================================================================
# Aggregation
# =============================================================================

def aggregate_monthly(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    df["month"] = df[DATE_COL].dt.to_period("M")
    return (
        df.groupby([POSTER_COL, "month"])[value_cols]
        .mean()
        .reset_index()
    )


# =============================================================================
# PDF helpers
# =============================================================================

def _cover(title: str, subtitle: str):
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(PRIMARY)
    ax.set_facecolor(PRIMARY)
    ax.axis("off")
    ax.text(0.5, 0.65, title, transform=ax.transAxes,
            ha="center", fontsize=20, fontweight="bold", color="white")
    ax.text(0.5, 0.45, subtitle, transform=ax.transAxes,
            ha="center", fontsize=12, color="#DDDDDD")
    return fig


def _section(title: str):
    fig, ax = plt.subplots(figsize=(10, 1.5))
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=13, fontweight="bold", color=PRIMARY)
    return fig


def _time_series_page(monthly_df: pd.DataFrame, top_users: list[str],
                      value_cols: list[str], suptitle: str):
    """One subplot per user; each value column is one line."""
    n = len(top_users)
    fig, axes = plt.subplots(n, 1, figsize=(12, n * 3.5))
    if n == 1:
        axes = [axes]

    for ax, user_id in zip(axes, top_users):
        user_data = monthly_df[monthly_df[POSTER_COL] == user_id].sort_values("month")
        if user_data.empty:
            ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center")
            ax.set_title(user_id, fontsize=9, fontweight="bold")
            continue

        x = [str(m) for m in user_data["month"]]
        for i, col in enumerate(value_cols):
            label = col.replace("liwc_", "").replace("_pct", "")
            ax.plot(x, user_data[col], marker="o", markersize=3,
                    linewidth=1.3, label=label, color=PALETTE[i % len(PALETTE)])

        ax.set_title(user_id, fontsize=9, fontweight="bold", color=PRIMARY)
        ax.tick_params(axis="x", rotation=45, labelsize=7)
        ax.tick_params(axis="y", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.legend(fontsize=6, loc="upper right", ncol=3)

    fig.suptitle(suptitle, fontsize=14, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


# =============================================================================
# Main
# =============================================================================

def run(input_path: str | None = None, dataset: str | None = None, top_n: int = 5):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ds = dataset or "combined"
    if input_path is None:
        input_path = structured_path(OUTPUT_DIR, ds)
    pdf_path = variant_path(OUTPUT_DIR, "user_longitudinal_report.pdf", ds)

    print(f"Loading {input_path}…")
    df = load_data(input_path)
    top_users = get_top_posters(df, n=top_n)
    print(f"Top {top_n} posters: {top_users}")
    df_top = df[df[POSTER_COL].isin(top_users)].copy()

    print("Scoring CDS…")
    cds_df, cds_cols = score_cds(df_top)
    cds_monthly = aggregate_monthly(cds_df, cds_cols) if cds_cols else pd.DataFrame()

    print("Scoring LIWC…")
    liwc_df, liwc_cols = score_liwc(df_top)
    liwc_monthly = aggregate_monthly(liwc_df, liwc_cols) if not liwc_df.empty else pd.DataFrame()

    print(f"Building PDF → {pdf_path}")
    with pdf_backend.PdfPages(pdf_path) as pdf:
        def save(fig):
            pdf.savefig(fig, bbox_inches="tight")
            plt.close("all")

        save(_cover("Depression Connect Forum",
                    f"Longitudinal Analysis — Top {top_n} Most Active Posters"))

        if not cds_monthly.empty and cds_cols:
            save(_section("CDS Category Scores Per Month"))
            save(_time_series_page(cds_monthly, top_users, cds_cols,
                                   "CDS Categories Over Time (mean per month)"))

        if not liwc_monthly.empty and liwc_cols:
            save(_section("LIWC Category Scores Per Month"))
            save(_time_series_page(liwc_monthly, top_users, liwc_cols,
                                   "Top LIWC Categories Over Time (mean % of words per month)"))

    print(f"  Saved → {pdf_path}")
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Per-user longitudinal LIWC and CDS analysis."
    )
    add_dataset_arg(parser)
    parser.add_argument("--top", type=int, default=5,
                        help="Number of top posters to analyse (default: 5)")
    parser.add_argument("--input", help="Override input file path (ignores --dataset)")
    args = parser.parse_args()

    run(input_path=args.input, dataset=args.dataset, top_n=args.top)
