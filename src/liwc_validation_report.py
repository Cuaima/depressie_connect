# =============================================================================
# liwc_validation_report.py  –  compare custom LIWC scorer vs LIWC-22 CLI
#
# Reads:
#   output/liwc_scores{_variant}.csv      – custom scorer (liwc_analysis.py)
#   output/liwc22_scores{_variant}.csv    – LIWC-22 CLI (liwc22_cli_runner.py)
#
# Produces:
#   output/liwc_validation_report{_variant}.pdf
#
# Report sections:
#   1. Scoring agreement — per-category Pearson r and MAE table + correlation bar
#   2. Divergence analysis — scatter plots for most divergent categories
#   3. LIWC-22 summary variables — Analytic / Clout / Authentic / Tone note
#   4. Coverage differences — categories present in one scorer but not the other
#   5. Diagnostics — word-count agreement between scorers
#
# Run with:  python src/liwc_validation_report.py [--dataset combined|old|new_only]
# Prerequisites: liwc22_cli_runner.py must have been run first.
# =============================================================================

from __future__ import annotations

import os
import argparse
import warnings
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend

from dataset_io import add_dataset_arg, variant_path, subtitle_for
from utils.thread_utils import parse_post_dates
from liwc22_cli_runner import (
    LIWC22_STRUCTURAL_COLS, LIWC22_SUMMARY_VARS,
    ROW_IDX_COL, POSTER_COL, DATE_COL,
)

warnings.filterwarnings("ignore")

OUTPUT_DIR = "output"

PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"
C_CUSTOM  = "#2166AC"
C_LIWC22  = "#D6604D"

# Columns in liwc22_scores.csv that are not content-category scores
_LIWC22_NON_CAT: frozenset[str] = LIWC22_STRUCTURAL_COLS | frozenset({
    ROW_IDX_COL, "PosterID", "PostDate", "ForumTopicID", "role",
})

# Number of scatter plots shown in the divergence section
_TOP_SCATTER = 12


# =============================================================================
# Data loading and alignment
# =============================================================================

def _custom_cat_map(df: pd.DataFrame) -> dict[str, str]:
    """Return {bare_category: liwc_X_pct column} for the custom scorer output."""
    return {
        col[len("liwc_"):-len("_pct")]: col
        for col in df.columns
        if col.startswith("liwc_") and col.endswith("_pct")
    }


def _liwc22_cat_map(df: pd.DataFrame) -> dict[str, str]:
    """Return {bare_category: column} for LIWC-22 content-category columns."""
    return {
        col: col
        for col in df.columns
        if col not in _LIWC22_NON_CAT and col not in LIWC22_SUMMARY_VARS
    }


def load_and_align(
    custom_path: str,
    liwc22_path: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    """
    Load both score files and return (custom_df, liwc22_df, custom_cats, liwc22_cats).

    Alignment strategy: rows are aligned on (PosterID, PostDate), which both
    files carry per message, and the keys are verified row-by-row after
    sorting. The files' _row_idx column must NOT be trusted as an alignment
    key: label_roles() date-sorts the frame inside liwc22_cli_runner.py, so
    _row_idx holds original pre-sort positions stored in date-sorted rows —
    merging a fresh positional index against it scrambles every pair
    (identical means, r ≈ 0; the bug shipped in the Jul/Aug validation
    reports).

    Raises (loud failure, no fallback) if:
      - LIWC-22 split messages into Segment rows that can't be re-aggregated
      - row counts differ after any aggregation
      - (PosterID, PostDate) keys are duplicated or don't match row-by-row
      - the 'function' category correlation lands below 0.95 after alignment
    """
    custom_df = pd.read_csv(custom_path)
    liwc22_df = pd.read_csv(liwc22_path)

    # LIWC-22 may split long messages into multiple Segment rows. If so,
    # aggregate back to one row per _row_idx (word-count-weighted mean for
    # category columns) before any comparison.
    if liwc22_df[ROW_IDX_COL].duplicated().any():
        n_before = len(liwc22_df)
        meta_cols = [c for c in liwc22_df.columns
                     if c in (ROW_IDX_COL, POSTER_COL, DATE_COL, "ForumTopicID", "role")]
        num_cols = [c for c in liwc22_df.columns if c not in meta_cols]
        wc = liwc22_df["WC"].clip(lower=1)

        weighted = liwc22_df[num_cols].mul(wc, axis=0)
        weighted[ROW_IDX_COL] = liwc22_df[ROW_IDX_COL]
        sums = weighted.groupby(ROW_IDX_COL).sum()
        wsum = wc.groupby(liwc22_df[ROW_IDX_COL]).sum()
        agg = sums.div(wsum, axis=0)
        agg["WC"] = wsum  # total word count, not weighted mean

        meta = liwc22_df[meta_cols].drop_duplicates(ROW_IDX_COL).set_index(ROW_IDX_COL)
        liwc22_df = meta.join(agg).reset_index()
        print(f"  Aggregated {n_before} LIWC-22 segment rows → {len(liwc22_df)} messages.")

    if len(custom_df) != len(liwc22_df):
        raise ValueError(
            f"Row count mismatch: custom scorer has {len(custom_df)} rows, "
            f"LIWC-22 has {len(liwc22_df)} rows.\n"
            "Ensure both were produced from the same --dataset variant and the "
            "same messages_structured.csv without re-running postprocess.py between them."
        )

    # Align on (PosterID, PostDate). Both files derive from the same
    # date-sorted structured file, so a STABLE sort keeps rows that share a
    # timestamp (e.g. one user posting twice in the same second) in identical
    # relative order in both frames — they then pair correctly by position
    # without needing a unique key. Duplicate (PosterID, PostDate) rows are
    # therefore expected and fine; only a genuine row-order divergence is a bug.
    key = [POSTER_COL, DATE_COL]
    for name, d in (("custom", custom_df), ("liwc22", liwc22_df)):
        d[DATE_COL] = parse_post_dates(d[DATE_COL])
        if d[DATE_COL].isna().any():
            raise ValueError(f"{name} scores contain unparseable PostDate values.")

    custom_df = custom_df.sort_values(key, kind="mergesort").reset_index(drop=True)
    liwc22_df = liwc22_df.sort_values(key, kind="mergesort").reset_index(drop=True)
    assert (custom_df[POSTER_COL].values == liwc22_df[POSTER_COL].values).all() \
        and (custom_df[DATE_COL].values == liwc22_df[DATE_COL].values).all(), \
        "join verloor rijen"  # keys diverge row-by-row after sort

    # Overwrite both keys positionally: alignment is now by verified row order.
    custom_df[ROW_IDX_COL] = custom_df.index
    liwc22_df[ROW_IDX_COL] = liwc22_df.index

    # Sanity: 'function' is high-prevalence in both scorers; if the join is
    # right its correlation must be near-perfect.
    if "liwc_function_pct" in custom_df.columns and "function" in liwc22_df.columns:
        r = float(custom_df["liwc_function_pct"].corr(liwc22_df["function"]))
        assert r > 0.95, f"nog steeds scheef: function r = {r:.4f}"
        print(f"  Alignment check: function r = {r:.4f} ✓")

    custom_cats = _custom_cat_map(custom_df)
    liwc22_cats = _liwc22_cat_map(liwc22_df)
    return custom_df, liwc22_df, custom_cats, liwc22_cats


# =============================================================================
# Per-category statistics
# =============================================================================

def compute_comparison(
    custom_df: pd.DataFrame,
    liwc22_df: pd.DataFrame,
    custom_cats: dict[str, str],
    liwc22_cats: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, set[str], set[str], set[str]]:
    """
    Returns
    -------
    merged       : wide DataFrame with custom_X and liwc22_X columns per category
    comparison   : per-category Pearson r, MAE, means — sorted by MAE descending
    common       : categories present in both scorers
    custom_only  : categories only in custom scorer
    liwc22_only  : categories only in LIWC-22
    """
    common      = set(custom_cats) & set(liwc22_cats)
    custom_only = set(custom_cats) - common
    liwc22_only = set(liwc22_cats) - common

    # Build merged frame
    custom_sub = custom_df[[ROW_IDX_COL]].copy()
    liwc22_sub = liwc22_df[[ROW_IDX_COL]].copy()
    for cat in common:
        custom_sub[f"custom_{cat}"] = custom_df[custom_cats[cat]].values
        liwc22_sub[f"liwc22_{cat}"] = liwc22_df[liwc22_cats[cat]].values

    merged = custom_sub.merge(liwc22_sub, on=ROW_IDX_COL)

    rows = []
    for cat in sorted(common):
        x = merged[f"custom_{cat}"].fillna(0)
        y = merged[f"liwc22_{cat}"].fillna(0)

        if x.std() > 0 and y.std() > 0:
            r, pval = scipy_stats.pearsonr(x, y)
        else:
            r, pval = np.nan, np.nan

        rows.append({
            "category":    cat,
            "pearson_r":   round(float(r), 4)           if not np.isnan(r)    else np.nan,
            "p_value":     round(float(pval), 4)        if not np.isnan(pval) else np.nan,
            "mae_pp":      round(float((x - y).abs().mean()), 4),
            "mean_custom": round(float(x.mean()), 4),
            "mean_liwc22": round(float(y.mean()), 4),
        })

    comparison = (
        pd.DataFrame(rows)
        .sort_values("mae_pp", ascending=False)
        .reset_index(drop=True)
    )
    return merged, comparison, common, custom_only, liwc22_only


# =============================================================================
# Figures
# =============================================================================

def _style_ax(ax, title, xlabel, ylabel="", fontsize=10):
    ax.set_title(title, fontsize=fontsize, fontweight="bold", color=PRIMARY, pad=8)
    ax.set_xlabel(xlabel, fontsize=fontsize - 2, color="#333333")
    ax.set_ylabel(ylabel, fontsize=fontsize - 2, color="#333333")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(colors="#555555", labelsize=fontsize - 3)


def _cover_page(subtitle: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(PRIMARY)
    ax.set_facecolor(PRIMARY)
    ax.axis("off")
    ax.text(0.5, 0.65, "LIWC-22 Validation Report", transform=ax.transAxes,
            ha="center", fontsize=20, fontweight="bold", color="white")
    ax.text(0.5, 0.48, "Custom scorer vs LIWC-22 CLI (Dutch LIWC-2015 dictionary)",
            transform=ax.transAxes, ha="center", fontsize=12, color="#DDDDDD")
    ax.text(0.5, 0.33, subtitle, transform=ax.transAxes,
            ha="center", fontsize=11, color="#AAAAAA")
    return fig


def _section_divider(title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 1.5))
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes, ha="center", va="center",
            fontsize=14, fontweight="bold", color=PRIMARY)
    return fig


def fig_summary_table(comparison: pd.DataFrame) -> plt.Figure:
    """Table: per-category Pearson r, MAE (pp), mean from each scorer."""
    col_labels = ["Category", "Pearson r", "p-value", "MAE (pp)", "Mean (custom)", "Mean (LIWC-22)"]
    display_cols = ["category", "pearson_r", "p_value", "mae_pp", "mean_custom", "mean_liwc22"]
    cell_data = [[str(row[c]) for c in display_cols] for _, row in comparison.iterrows()]

    fig, ax = plt.subplots(figsize=(14, max(3, len(comparison) * 0.38 + 1)))
    ax.axis("off")
    tbl = ax.table(cellText=cell_data, colLabels=col_labels, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1, 1.5)

    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cell_data) + 1):
        # Highlight high-MAE rows (> 2 percentage points)
        try:
            mae = float(cell_data[i - 1][3])
            if mae > 2.0:
                for j in range(len(col_labels)):
                    tbl[(i, j)].set_facecolor("#FFF3CD")
            elif i % 2 == 0:
                for j in range(len(col_labels)):
                    tbl[(i, j)].set_facecolor(SECONDARY)
        except (ValueError, IndexError):
            pass

    ax.set_title(
        "Per-Category Scoring Agreement (sorted by MAE descending)\n"
        "MAE = mean absolute difference in percentage points; yellow = MAE > 2 pp",
        fontsize=11, fontweight="bold", color=PRIMARY, pad=10,
    )
    fig.tight_layout()
    return fig


def fig_correlation_bar(comparison: pd.DataFrame) -> plt.Figure:
    """Horizontal bar chart of per-category Pearson r, sorted descending."""
    data = comparison.dropna(subset=["pearson_r"]).sort_values("pearson_r", ascending=True)

    fig, ax = plt.subplots(figsize=(10, max(4, len(data) * 0.35)))
    colors = [ACCENT if r < 0.7 else C_CUSTOM for r in data["pearson_r"]]
    ax.barh(data["category"], data["pearson_r"], color=colors, edgecolor="white")
    ax.axvline(0.7, color="#888888", linestyle="--", linewidth=0.8, alpha=0.7)
    ax.text(0.71, len(data) * 0.02, "r = 0.7", fontsize=7, color="#888888")
    _style_ax(ax, "Per-Category Pearson r — Custom Scorer vs LIWC-22",
              "Pearson r", "Category")
    ax.set_xlim(-0.05, 1.05)
    fig.tight_layout()
    return fig


def fig_scatter_grid(
    merged: pd.DataFrame,
    comparison: pd.DataFrame,
    top_n: int = _TOP_SCATTER,
) -> plt.Figure:
    """Scatter plots for the top_n most divergent categories (highest MAE)."""
    top = comparison.dropna(subset=["pearson_r"]).head(top_n)
    n = len(top)
    if n == 0:
        return _section_divider("No scatter data available")

    ncols = 3
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, nrows * 3.5))
    axes_flat = axes.flatten() if n > 1 else [axes]

    for i, (_, row) in enumerate(top.iterrows()):
        ax  = axes_flat[i]
        cat = row["category"]
        x   = merged[f"custom_{cat}"].fillna(0)
        y   = merged[f"liwc22_{cat}"].fillna(0)

        ax.scatter(x, y, alpha=0.15, s=8, color=C_CUSTOM, rasterized=True)
        lim = max(x.max(), y.max()) * 1.05 or 1
        ax.plot([0, lim], [0, lim], color="#888888", linewidth=0.8, linestyle="--")
        ax.set_xlim(0, lim)
        ax.set_ylim(0, lim)
        ax.set_title(cat, fontsize=8, fontweight="bold", color=PRIMARY)
        ax.set_xlabel("Custom scorer (%)", fontsize=7)
        ax.set_ylabel("LIWC-22 (%)", fontsize=7)
        ax.tick_params(labelsize=6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.text(0.97, 0.05,
                f"r = {row['pearson_r']:.2f}\nMAE = {row['mae_pp']:.2f} pp",
                transform=ax.transAxes, ha="right", fontsize=7, color="#555555")

    for j in range(n, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle(
        f"Top {n} Most Divergent Categories — Custom vs LIWC-22 (dashed = identity line)",
        fontsize=13, fontweight="bold", color=PRIMARY,
    )
    fig.tight_layout()
    return fig


def fig_summary_vars_notice() -> plt.Figure:
    """Informational page for the absent Analytic / Clout / Authentic / Tone."""
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.axis("off")
    body = (
        "The four LIWC-22 summary variables are not available in this run:\n\n"
        "    Analytic   ·   Clout   ·   Authentic   ·   Tone\n\n"
        "These scores are computed by LIWC-22 from a proprietary regression model\n"
        "trained on the built-in English dictionary. They do not generalise to\n"
        "external dictionary files, so LIWC-22 omits them when a .dicx file is\n"
        "supplied (as here, with the Dutch LIWC-2015 dictionary).\n\n"
        "To obtain these variables you would need to run LIWC-22 with its built-in\n"
        "English dictionary (-d LIWC22), which is not appropriate for Dutch text.\n\n"
        "See docs/statistical_decisions.md §5 for context."
    )
    ax.text(0.5, 0.5, body, transform=ax.transAxes, ha="center", va="center",
            fontsize=10.5, color="#333333", linespacing=1.7,
            bbox=dict(boxstyle="round,pad=0.9", facecolor="#FFF3CD", alpha=0.9))
    ax.set_title("LIWC-22 Summary Variables — Not Available with External Dictionary",
                 fontsize=13, fontweight="bold", color=PRIMARY, pad=12)
    fig.tight_layout()
    return fig


def fig_coverage_note(
    custom_only: set[str],
    liwc22_only: set[str],
) -> plt.Figure:
    """Table listing categories present in only one of the two scorers."""
    rows: list[list[str]] = []
    n = max(len(custom_only), len(liwc22_only), 1)
    custom_sorted = sorted(custom_only)
    liwc22_sorted = sorted(liwc22_only)
    for i in range(n):
        rows.append([
            custom_sorted[i] if i < len(custom_sorted) else "—",
            liwc22_sorted[i] if i < len(liwc22_sorted) else "—",
        ])

    fig, ax = plt.subplots(figsize=(10, max(3, n * 0.36 + 1.5)))
    ax.axis("off")
    tbl = ax.table(
        cellText=rows,
        colLabels=["Custom scorer only", "LIWC-22 only"],
        cellLoc="center",
        loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    for j in range(2):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, n + 1):
        if i % 2 == 0:
            for j in range(2):
                tbl[(i, j)].set_facecolor(SECONDARY)

    ax.set_title(
        "Category Coverage Differences\n"
        "(custom-only includes fps_dutch fallback; "
        "LIWC-22-only includes structural metrics like WC, Emoji, AllPunc)",
        fontsize=11, fontweight="bold", color=PRIMARY, pad=10,
    )
    fig.tight_layout()
    return fig


def fig_word_count_check(
    custom_df: pd.DataFrame,
    liwc22_df: pd.DataFrame,
) -> plt.Figure:
    """
    Scatter: custom scorer word_count vs LIWC-22 WC.
    Both should agree closely; divergence signals tokenisation differences.
    """
    if "word_count" not in custom_df.columns or "WC" not in liwc22_df.columns:
        return _section_divider("Word-count diagnostic not available")

    x = custom_df["word_count"].fillna(0)
    y = liwc22_df["WC"].fillna(0)
    lim = max(x.max(), y.max()) * 1.05 or 1

    r = np.nan
    if x.std() > 0 and y.std() > 0:
        r, _ = scipy_stats.pearsonr(x, y)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(x, y, alpha=0.15, s=8, color=C_CUSTOM, rasterized=True)
    ax.plot([0, lim], [0, lim], color="#888888", linewidth=0.9, linestyle="--")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.text(0.97, 0.05,
            f"Pearson r = {r:.4f}" if not np.isnan(r) else "r = n/a",
            transform=ax.transAxes, ha="right", fontsize=9, color="#555555")
    _style_ax(
        ax,
        "Tokenisation Sanity Check — Word Count Agreement\n"
        "(custom scorer re.findall vs LIWC-22 WC; dashed = identity)",
        "Custom scorer word_count", "LIWC-22 WC",
    )
    fig.tight_layout()
    return fig


# =============================================================================
# PDF builder
# =============================================================================

def build_pdf(
    custom_df: pd.DataFrame,
    liwc22_df: pd.DataFrame,
    merged: pd.DataFrame,
    comparison: pd.DataFrame,
    custom_only: set[str],
    liwc22_only: set[str],
    pdf_path: str,
    pdf=None,
    include_cover: bool = True,
    subtitle: str = "",
) -> None:
    def _write(writer):
        def save(fig):
            writer.savefig(fig, bbox_inches="tight")
            plt.close("all")

        if include_cover:
            save(_cover_page(subtitle))
        else:
            save(_section_divider("LIWC-22 Validation"))

        save(_section_divider("Section 1 — Scoring Agreement"))
        save(fig_summary_table(comparison))
        save(fig_correlation_bar(comparison))

        save(_section_divider("Section 2 — Divergence Analysis"))
        save(fig_scatter_grid(merged, comparison))

        save(_section_divider("Section 3 — LIWC-22 Summary Variables"))
        save(fig_summary_vars_notice())

        save(_section_divider("Section 4 — Category Coverage Differences"))
        save(fig_coverage_note(custom_only, liwc22_only))

        save(_section_divider("Section 5 — Diagnostics"))
        save(fig_word_count_check(custom_df, liwc22_df))

    if pdf is not None:
        _write(pdf)
    else:
        print(f"Building PDF → {pdf_path}")
        with pdf_backend.PdfPages(pdf_path) as writer:
            _write(writer)
        print(f"  PDF saved → {pdf_path}")


# =============================================================================
# Main
# =============================================================================

def main(dataset: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ds = dataset

    custom_path = variant_path(OUTPUT_DIR, "liwc_scores.csv",   ds)
    liwc22_path = variant_path(OUTPUT_DIR, "liwc22_scores.csv", ds)
    pdf_path    = variant_path(OUTPUT_DIR, "liwc_validation_report.pdf", ds)

    for p, label in [(custom_path, "liwc_scores.csv"), (liwc22_path, "liwc22_scores.csv")]:
        if not os.path.exists(p):
            raise FileNotFoundError(
                f"{label} not found at: {p}\n"
                + ("Run liwc_analysis.py first."
                   if "liwc_scores" in p
                   else "Run liwc22_cli_runner.py first.")
            )

    print(f"Loading scores for dataset '{ds}'…")
    custom_df, liwc22_df, custom_cats, liwc22_cats = load_and_align(custom_path, liwc22_path)
    print(f"  Custom scorer: {len(custom_cats)} categories")
    print(f"  LIWC-22:       {len(liwc22_cats)} categories")

    merged, comparison, common, custom_only, liwc22_only = compute_comparison(
        custom_df, liwc22_df, custom_cats, liwc22_cats
    )
    print(f"  Overlap: {len(common)} categories  |  "
          f"custom-only: {len(custom_only)}  |  LIWC-22-only: {len(liwc22_only)}")

    csv_path = variant_path(OUTPUT_DIR, "liwc_validation_comparison.csv", ds)
    comparison.to_csv(csv_path, index=False)
    print(f"  Saved comparison table → {csv_path}")

    build_pdf(
        custom_df, liwc22_df, merged, comparison,
        custom_only, liwc22_only,
        pdf_path=pdf_path,
        subtitle=f"Dataset: {subtitle_for(ds)}",
    )

    print(f"\n✓ Done.")
    print(f"  {pdf_path}")
    print(f"  {csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LIWC-22 validation report (custom scorer vs CLI)."
    )
    add_dataset_arg(parser)
    args = parser.parse_args()
    main(dataset=args.dataset)
