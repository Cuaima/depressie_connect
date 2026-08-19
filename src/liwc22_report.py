# =============================================================================
# liwc22_report.py  –  standalone descriptive report for LIWC-22 CLI scores
#
# Mirrors liwc_report.pdf (liwc_analysis.py) but reports the official LIWC-22
# CLI output (output/liwc22_scores{_variant}.csv) directly, rather than the
# custom scorer. This is descriptive reporting of LIWC-22's own numbers — the
# custom-vs-LIWC-22 agreement comparison is a separate report
# (liwc_validation_report.py, master report section 7).
#
# Reads:  output/liwc22_scores{_variant}.csv  (written by liwc22_cli_runner.py)
# Writes: output/liwc22_report{_variant}.pdf
#
# Run with:  python src/liwc22_report.py [--dataset combined|old|new_only]
# Prerequisite: liwc22_cli_runner.py must have been run first.
# =============================================================================

from __future__ import annotations

import os
import argparse

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend

from dataset_io import add_dataset_arg, variant_path, subtitle_for
from liwc22_cli_runner import (
    LIWC22_STRUCTURAL_COLS, LIWC22_SUMMARY_VARS,
    POSTER_COL, DATE_COL, TOPIC_COL, ROW_IDX_COL,
)
from liwc_analysis import PRIMARY, C_POST, C_REPLY, _style_ax, _cover_page, _section_divider
from utils.spinner import Spinner
from utils.thread_utils import parse_post_dates

OUTPUT_DIR = "output"

_NON_CATEGORY_COLS = LIWC22_STRUCTURAL_COLS | {
    ROW_IDX_COL, POSTER_COL, DATE_COL, TOPIC_COL, "role", "month_dt",
}


# =============================================================================
# Data loading
# =============================================================================

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df[DATE_COL] = parse_post_dates(df[DATE_COL])
    df = df.dropna(subset=[DATE_COL])
    df["month_dt"] = df[DATE_COL].dt.to_period("M").dt.to_timestamp()
    return df


def category_columns(df: pd.DataFrame) -> list[str]:
    """LIWC-22 content-category columns: everything except IDs, structural
    metrics (WC, punctuation counts, ...), and the summary variables (which
    get their own note page rather than being plotted as ordinary categories)."""
    return [c for c in df.columns
            if c not in _NON_CATEGORY_COLS and c not in LIWC22_SUMMARY_VARS]


# =============================================================================
# Figures
# =============================================================================

def fig_category_prevalence(df: pd.DataFrame, cat_cols: list[str]) -> plt.Figure:
    """Horizontal bar chart: mean % of each LIWC-22 category, sorted descending."""
    means = df[cat_cols].mean().sort_values(ascending=False)

    n = len(means)
    fig, ax = plt.subplots(figsize=(10, max(4, n * 0.28)))
    y = np.arange(n)
    ax.barh(y, means.values, color=PRIMARY, alpha=0.85, edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(means.index, fontsize=7)
    ax.invert_yaxis()
    _style_ax(ax, "Overall LIWC-22 Category Prevalence (mean % of words)",
              "Mean % of words", "Category")
    fig.tight_layout()
    return fig


def fig_posts_vs_replies(df: pd.DataFrame, cat_cols: list[str], top_n: int = 20) -> plt.Figure:
    """Grouped bar chart: top N LIWC-22 categories, posts vs replies."""
    posts   = df[df["role"] == "post"]
    replies = df[df["role"] == "reply"]

    rows = [{
        "category": c,
        "post":  posts[c].mean()   if len(posts)   > 0 else 0,
        "reply": replies[c].mean() if len(replies) > 0 else 0,
    } for c in cat_cols]

    res = (
        pd.DataFrame(rows)
        .assign(total=lambda d: d["post"] + d["reply"])
        .sort_values("total", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    x = np.arange(len(res))
    w = 0.35
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.bar(x - w/2, res["post"],  w, label="Opening posts", color=C_POST,  alpha=0.85)
    ax.bar(x + w/2, res["reply"], w, label="Replies",        color=C_REPLY, alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels(res["category"], rotation=35, ha="right", fontsize=8)
    ax.legend(fontsize=9)
    _style_ax(ax, f"Top {top_n} LIWC-22 Categories — Posts vs Replies (mean % of words)",
              "Category", "Mean % of words")
    fig.tight_layout()
    return fig


def fig_category_over_time(df: pd.DataFrame, cat_cols: list[str], top_n: int = 12) -> plt.Figure:
    """Small multiples: monthly prevalence for the top N LIWC-22 categories."""
    overall_means = df[cat_cols].mean().sort_values(ascending=False)
    top_cols = list(overall_means.head(top_n).index)

    n    = len(top_cols)
    cols = 3
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(14, rows * 3), sharex=False)
    axes_flat = axes.flatten()

    for i, c in enumerate(top_cols):
        ax = axes_flat[i]
        monthly = df.groupby("month_dt")[c].mean().reset_index()
        ax.plot(monthly["month_dt"], monthly[c], color=PRIMARY, linewidth=1.2)
        ax.fill_between(monthly["month_dt"], monthly[c], alpha=0.12, color=PRIMARY)
        ax.set_title(c, fontsize=8, fontweight="bold", color=PRIMARY)
        ax.tick_params(axis="x", rotation=30, labelsize=6)
        ax.tick_params(axis="y", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    for j in range(n, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle(f"Top {top_n} LIWC-22 Categories — Monthly Prevalence Over Time",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def fig_summary_vars_note(df: pd.DataFrame) -> plt.Figure:
    """Informational page: Analytic/Clout/Authentic/Tone are absent with an
    external Dutch dictionary (same fact as liwc_validation_report.py §3)."""
    present = [v for v in LIWC22_SUMMARY_VARS if v in df.columns]
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.axis("off")
    if present:
        body = "Summary variables present in this run:\n\n" + ", ".join(present)
    else:
        body = (
            "Analytic · Clout · Authentic · Tone are NOT present in this run.\n\n"
            "These four scores are computed by LIWC-22 from a proprietary\n"
            "regression model trained on the built-in English dictionary. They\n"
            "do not generalise to external dictionary files, so LIWC-22 omits\n"
            "them when a .dicx file is supplied (as here, with the Dutch\n"
            "LIWC-2015 dictionary).\n\n"
            "See output/liwc_validation_report.pdf, Section 3, for detail."
        )
    ax.text(0.5, 0.5, body, transform=ax.transAxes, ha="center", va="center",
            fontsize=11, color="#333333", linespacing=1.6,
            bbox=dict(boxstyle="round,pad=0.9", facecolor="#FFF3CD", alpha=0.9))
    ax.set_title("LIWC-22 Summary Variables", fontsize=13, fontweight="bold",
                 color=PRIMARY, pad=12)
    fig.tight_layout()
    return fig


# =============================================================================
# PDF builder
# =============================================================================

def build_pdf(df: pd.DataFrame, cat_cols: list[str], pdf_path: str, subtitle: str) -> None:
    with Spinner(f"Building PDF → {pdf_path}"):
        with pdf_backend.PdfPages(pdf_path) as pdf:
            def save(fig):
                pdf.savefig(fig, bbox_inches="tight")
                plt.close("all")

            save(_cover_page("Depression Connect Forum",
                             f"LIWC-22 Psycholinguistic Feature Analysis\n{subtitle}"))

            save(_section_divider("Section 1 — Overall Category Prevalence"))
            save(fig_category_prevalence(df, cat_cols))

            save(_section_divider("Section 2 — Posts vs Replies"))
            save(fig_posts_vs_replies(df, cat_cols))

            save(_section_divider("Section 3 — Top Categories Over Time"))
            save(fig_category_over_time(df, cat_cols))

            save(_section_divider("Section 4 — Summary Variables"))
            save(fig_summary_vars_note(df))
    print(f"  PDF saved → {pdf_path}")


# =============================================================================
# Main
# =============================================================================

def main(dataset: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    input_path = variant_path(OUTPUT_DIR, "liwc22_scores.csv", dataset)
    pdf_path   = variant_path(OUTPUT_DIR, "liwc22_report.pdf", dataset)

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"{input_path} not found.\n"
            f"Run liwc22_cli_runner.py first (make liwc22 DATASET={dataset})."
        )

    print(f"Loading {input_path}…")
    df = load_data(input_path)
    print(f"  {len(df)} messages from {df[POSTER_COL].nunique()} users.")

    cat_cols = category_columns(df)
    print(f"  {len(cat_cols)} LIWC-22 content categories.")

    build_pdf(df, cat_cols, pdf_path, subtitle=f"Dataset: {subtitle_for(dataset)}")

    print(f"\n✓ Done.")
    print(f"  {pdf_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Standalone descriptive report for LIWC-22 CLI scores."
    )
    add_dataset_arg(parser)
    args = parser.parse_args()
    main(dataset=args.dataset)
