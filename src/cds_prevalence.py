# =============================================================================
# cds_prevalence.py  –  most common cognitive distortions in forum messages
#
# Reads the CDS scores produced by exploratory_analysis.py (cds_scores.csv),
# or re-scores messages from scratch if that file doesn't exist yet.
#
# Produces:
#   output/cds_prevalence_report.pdf   – ranked charts, trends, post vs reply
#   output/cds_category_ranking.csv    – ranked category prevalence table
#   output/cds_phrase_ranking.csv      – ranked individual CDS phrase table
#
# Run with:  python src/cds_prevalence.py
# =============================================================================

from __future__ import annotations

import os
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
from scipy import stats as scipy_stats
from statsmodels.stats.multitest import multipletests

from utils.CDS import process_dataset, load_CDS
from utils.thread_utils import label_roles
from utils.spinner import Spinner
from dataset_io import add_dataset_arg, structured_path, variant_path

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_PATH    = "output/messages_structured.csv"
SCORED_PATH   = "output/cds_scores.csv"       # written by exploratory_analysis.py
OUTPUT_DIR    = "output"
PDF_PATH      = os.path.join(OUTPUT_DIR, "cds_prevalence_report.pdf")
CAT_RANK_PATH = os.path.join(OUTPUT_DIR, "cds_category_ranking.csv")
PHR_RANK_PATH = os.path.join(OUTPUT_DIR, "cds_phrase_ranking.csv")

LIWC_LANGUAGE = "NL"  # change to "EN" if using English CDS lexicon

POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
DATE_COL   = "PostDate"
TOPIC_COL  = "ForumTopicID"

PRIMARY   = "#2E5E8E"
C_POST    = "#2166AC"
C_REPLY   = "#D6604D"
C_BAR     = "#4A9EBF"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"

CDS_CATEGORY_COLS = [
    "Labeling and mislabeling", "Catastrophizing", "Dichotomous Reasoning",
    "Emotional Reasoning", "Disqualifying the Positive",
    "Magnification and Minimization", "Mental Filtering", "Mindreading",
    "Fortune-telling", "Overgeneralizing", "Personalizing", "Should statements"
]


# =============================================================================
# Data loading
# =============================================================================

def load_messages(path: str | None = None) -> pd.DataFrame:
    df = pd.read_csv(path or INPUT_PATH)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TEXT_COL]).copy()
    print(f"  Loaded {len(df)} messages from {df[POSTER_COL].nunique()} users.")
    return df


def add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"]     = df[DATE_COL].dt.year
    df["month_dt"] = df[DATE_COL].dt.to_period("M").dt.to_timestamp()
    return df


def get_scored_df(input_path: str | None = None,
                  scored_path: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (scored_df, cds_phrases_df).
    Loads from scored_path (cds_scores.csv) if available, otherwise re-scores from input_path.
    cds_phrases_df has one column per individual CDS phrase (not category).
    """
    scored_path = scored_path or SCORED_PATH
    if os.path.exists(scored_path):
        print(f"  Loading pre-scored data from {scored_path}")
        df = pd.read_csv(scored_path)
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    else:
        print("  cds_scores.csv not found — running CDS scoring from scratch…")
        df = load_messages(input_path)

    df = label_roles(df)
    df = add_time_columns(df)

    # Re-score at phrase level if columns aren't already in the file
    # (exploratory_analysis.py saves category columns but not phrase columns)
    print("  Scoring individual CDS phrases (for phrase-level ranking)…")
    raw = pd.DataFrame({"text": df[TEXT_COL].fillna("").str.lower().values})
    cds_phrases, cds_categories, _ = process_dataset(raw, output="all_variants",
                                                      language=LIWC_LANGUAGE)
    cds_phrases = cds_phrases.reset_index(drop=True)
    cds_categories = cds_categories.reset_index(drop=True)
    df = df.reset_index(drop=True)

    # Attach category scores (may already be there, overwrite to be safe)
    for col in cds_categories.columns:
        df[col] = cds_categories[col].values

    # Attach CDS flag
    if "CDS" not in df.columns:
        df["CDS"] = (cds_categories.sum(axis=1) > 0).astype(int)

    return df, cds_phrases


# =============================================================================
# Category ranking
# =============================================================================

def compute_category_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each CDS category: prevalence overall, in posts, and in replies.
    Also computes a Chi-square test (post vs reply) and effect size (Cramér's V).
    """
    cat_cols = [c for c in CDS_CATEGORY_COLS if c in df.columns]
    if not cat_cols:
        raise ValueError(
            "No CDS category columns found in the scored DataFrame. "
            "Make sure exploratory_analysis.py has been run first."
        )

    posts   = df[df["role"] == "post"]
    replies = df[df["role"] == "reply"]

    rows = []
    for col in cat_cols:
        n_total  = len(df)
        n_posts  = len(posts)
        n_replies = len(replies)

        match_total  = int(df[col].sum())
        match_posts  = int(posts[col].sum())
        match_replies = int(replies[col].sum())

        prev_total  = match_total  / n_total   * 100
        prev_posts  = match_posts  / n_posts   * 100  if n_posts  > 0 else 0
        prev_replies = match_replies / n_replies * 100 if n_replies > 0 else 0

        # Chi-square: posts vs replies
        contingency = np.array([
            [match_posts,  n_posts   - match_posts],
            [match_replies, n_replies - match_replies],
        ])
        if contingency.min() > 0:
            chi2, p_val, _, _ = scipy_stats.chi2_contingency(contingency)
            n = n_posts + n_replies
            cramers_v = np.sqrt(chi2 / (n * (min(contingency.shape) - 1)))
        else:
            chi2, p_val, cramers_v = np.nan, np.nan, np.nan

        rows.append({
            "category":            col,
            "prevalence_pct":      round(prev_total,  2),
            "prevalence_posts_pct": round(prev_posts,  2),
            "prevalence_replies_pct": round(prev_replies, 2),
            "ratio_post_reply":    round(prev_posts / prev_replies, 3)
                                   if prev_replies > 0 else np.nan,
            "chi2":                round(chi2, 3)     if not np.isnan(chi2) else np.nan,
            "p_value":             round(p_val, 4)    if not np.isnan(p_val) else np.nan,
            "cramers_v":           round(cramers_v, 4) if not np.isnan(cramers_v) else np.nan,
            "n_matches_total":     match_total,
            "n_matches_posts":     match_posts,
            "n_matches_replies":   match_replies,
        })

    # Benjamini–Hochberg FDR correction across all category tests
    valid_pvals = [r["p_value"] for r in rows if r["p_value"] == r["p_value"]]
    if valid_pvals:
        _, corrected, _, _ = multipletests(valid_pvals, method="fdr_bh")
        ci = 0
        for r in rows:
            if r["p_value"] == r["p_value"]:
                r["p_value_bh"]          = round(float(corrected[ci]), 4)
                r["sig_after_correction"] = bool(corrected[ci] < 0.05)
                ci += 1
            else:
                r["p_value_bh"]          = None
                r["sig_after_correction"] = False
    else:
        for r in rows:
            r["p_value_bh"]          = None
            r["sig_after_correction"] = False

    return (
        pd.DataFrame(rows)
        .sort_values("prevalence_pct", ascending=False)
        .reset_index(drop=True)
    )


# =============================================================================
# Phrase ranking
# =============================================================================

def compute_phrase_ranking(
    df: pd.DataFrame,
    cds_phrases: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each individual CDS phrase: how often does it appear, and in which category?
    """
    cds_df = load_CDS(language=LIWC_LANGUAGE)

    rows = []
    for phrase in cds_phrases.columns:
        count = int(cds_phrases[phrase].sum())
        prevalence = count / len(cds_phrases) * 100
        category = cds_df.loc[phrase, "categories"] if phrase in cds_df.index else "unknown"

        # Split by role
        phrase_series = cds_phrases[phrase].values
        post_mask  = (df["role"] == "post").values
        reply_mask = (df["role"] == "reply").values
        n_posts_match  = int(phrase_series[post_mask].sum())
        n_reply_match  = int(phrase_series[reply_mask].sum())

        rows.append({
            "phrase":            phrase,
            "category":          category,
            "total_matches":     count,
            "prevalence_pct":    round(prevalence, 3),
            "matches_posts":     n_posts_match,
            "matches_replies":   n_reply_match,
        })

    return (
        pd.DataFrame(rows)
        .sort_values("total_matches", ascending=False)
        .reset_index(drop=True)
    )


# =============================================================================
# Figures
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
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=14,
            fontweight="bold", color=PRIMARY)
    return fig


def fig_category_ranking(cat_ranking: pd.DataFrame) -> plt.Figure:
    """Horizontal bar chart — categories ranked by overall prevalence."""
    fig, ax = plt.subplots(figsize=(10, max(4, len(cat_ranking) * 0.5)))
    y = np.arange(len(cat_ranking))
    colors = [ACCENT if i == 0 else C_BAR for i in range(len(cat_ranking))]
    ax.barh(y, cat_ranking["prevalence_pct"], color=colors,
            edgecolor="white", linewidth=0.5)

    for i, (_, row) in enumerate(cat_ranking.iterrows()):
        ax.text(row["prevalence_pct"] + 0.1, i,
                f"{row['prevalence_pct']:.1f}%",
                va="center", fontsize=8, color="#333333")

    ax.set_yticks(y)
    ax.set_yticklabels(cat_ranking["category"], fontsize=9)
    ax.invert_yaxis()
    _style_ax(ax,
              "CDS Category Prevalence Ranking\n(% of messages containing at least one phrase from this category)",
              "Prevalence (%)", "CDS Category")
    fig.tight_layout()
    return fig


def fig_category_posts_vs_replies(cat_ranking: pd.DataFrame) -> plt.Figure:
    """Grouped bar chart — posts vs replies per category."""
    n = len(cat_ranking)
    x = np.arange(n)
    w = 0.35

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.bar(x - w/2, cat_ranking["prevalence_posts_pct"],  w,
           label="Opening posts", color=C_POST,  alpha=0.85, edgecolor="white")
    ax.bar(x + w/2, cat_ranking["prevalence_replies_pct"], w,
           label="Replies",        color=C_REPLY, alpha=0.85, edgecolor="white")

    ax.set_xticks(x)
    ax.set_xticklabels(cat_ranking["category"], rotation=35, ha="right", fontsize=8)
    ax.legend(fontsize=9)
    _style_ax(ax,
              "CDS Prevalence by Category — Opening Posts vs Replies",
              "CDS Category", "Prevalence (%)")
    fig.tight_layout()
    return fig


def fig_top_phrases(phrase_ranking: pd.DataFrame, top_n: int = 25) -> plt.Figure:
    """Horizontal bar chart — top N individual CDS phrases by frequency."""
    top = phrase_ranking.head(top_n).copy()
    top = top.iloc[::-1]  # flip so highest is at top

    fig, ax = plt.subplots(figsize=(11, max(5, top_n * 0.4)))
    colors = [C_REPLY if i == len(top) - 1 else C_BAR for i in range(len(top))]
    ax.barh(range(len(top)), top["total_matches"], color=colors,
            edgecolor="white", linewidth=0.5)

    for i, (_, row) in enumerate(top.iterrows()):
        ax.text(row["total_matches"] + 0.5, i,
                f"{row['prevalence_pct']:.1f}%  [{row['category']}]",
                va="center", fontsize=7, color="#444444")

    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(top["phrase"], fontsize=8)
    _style_ax(ax,
              f"Top {top_n} Most Frequent CDS Phrases\n(label = prevalence % | category)",
              "Number of messages matched", "CDS Phrase")
    fig.tight_layout()
    return fig


def fig_phrase_posts_vs_replies(
    phrase_ranking: pd.DataFrame,
    df: pd.DataFrame,
    top_n: int = 20,
) -> plt.Figure:
    """
    Stacked bar: for top N phrases, show how matches split between posts and replies.
    """
    top = phrase_ranking.head(top_n).copy().reset_index(drop=True)
    n_posts  = (df["role"] == "post").sum()
    n_replies = (df["role"] == "reply").sum()

    # Normalise to prevalence %
    top["pct_posts"]   = top["matches_posts"]   / n_posts   * 100
    top["pct_replies"] = top["matches_replies"]  / n_replies * 100

    x = np.arange(len(top))
    w = 0.35
    fig, ax = plt.subplots(figsize=(13, 5))
    ax.bar(x - w/2, top["pct_posts"],   w,
           label="Opening posts", color=C_POST,  alpha=0.85, edgecolor="white")
    ax.bar(x + w/2, top["pct_replies"], w,
           label="Replies",        color=C_REPLY, alpha=0.85, edgecolor="white")

    ax.set_xticks(x)
    ax.set_xticklabels(top["phrase"], rotation=40, ha="right", fontsize=7)
    ax.legend(fontsize=9)
    _style_ax(ax,
              f"Top {top_n} CDS Phrases — Opening Posts vs Replies (prevalence %)",
              "CDS Phrase", "Prevalence (%)")
    fig.tight_layout()
    return fig


def fig_category_trend(df: pd.DataFrame) -> plt.Figure:
    """Small multiples — monthly CDS category prevalence over time."""
    cat_cols = [c for c in CDS_CATEGORY_COLS if c in df.columns]
    n    = len(cat_cols)
    cols = 3
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols,
                             figsize=(14, rows * 2.8),
                             sharex=False)
    axes_flat = axes.flatten()

    for i, col in enumerate(cat_cols):
        ax  = axes_flat[i]
        monthly = df.groupby("month_dt").agg(
            total=(col, "count"),
            matches=(col, "sum"),
        ).reset_index()
        monthly["prevalence"] = monthly["matches"] / monthly["total"] * 100

        ax.plot(monthly["month_dt"], monthly["prevalence"],
                color=PRIMARY, linewidth=1.2)
        ax.fill_between(monthly["month_dt"], monthly["prevalence"],
                        alpha=0.12, color=PRIMARY)
        mean_val = monthly["prevalence"].mean()
        ax.axhline(mean_val, color=ACCENT, linestyle="--", linewidth=0.8, alpha=0.7)
        ax.set_title(col, fontsize=7.5, fontweight="bold", color=PRIMARY)
        ax.tick_params(axis="x", rotation=30, labelsize=5.5)
        ax.tick_params(axis="y", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    for j in range(n, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle("CDS Category Prevalence Over Time (Monthly, dashed = mean)",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def fig_stats_table(cat_ranking: pd.DataFrame) -> plt.Figure:
    """Renders the category ranking as a formatted table figure."""
    display_cols = [
        "category", "prevalence_pct", "prevalence_posts_pct",
        "prevalence_replies_pct", "ratio_post_reply", "p_value", "p_value_bh", "cramers_v"
    ]
    col_labels = [
        "Category", "Overall %", "Posts %", "Replies %",
        "Ratio P/R", "p-value", "p-BH", "Cramér's V"
    ]
    cols_present = [c for c in display_cols if c in cat_ranking.columns]
    col_labels_present = [col_labels[display_cols.index(c)] for c in cols_present]

    cell_data = []
    sig_rows: set[int] = set()
    for i, (_, row) in enumerate(cat_ranking.iterrows()):
        cell_data.append([str(row[c]) for c in cols_present])
        if row.get("sig_after_correction", False):
            sig_rows.add(i)

    fig, ax = plt.subplots(figsize=(15, max(3, len(cat_ranking) * 0.5 + 1)))
    ax.axis("off")
    tbl = ax.table(
        cellText=cell_data,
        colLabels=col_labels_present,
        cellLoc="center",
        loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1, 1.5)

    for j in range(len(col_labels_present)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cell_data) + 1):
        for j in range(len(col_labels_present)):
            if (i - 1) in sig_rows:
                tbl[(i, j)].set_facecolor("#FFF3CD")   # yellow = survives BH correction
            elif i % 2 == 0:
                tbl[(i, j)].set_facecolor(SECONDARY)

    ax.set_title(
        "CDS Category Ranking with Statistical Tests\n"
        "(p-BH = Benjamini–Hochberg corrected; yellow rows significant after correction)",
        fontsize=11, fontweight="bold", color=PRIMARY, pad=10,
    )
    fig.tight_layout()
    return fig


# =============================================================================
# PDF builder (can write to an existing PdfPages handle for consolidated reports)
# =============================================================================

def build_pdf(df: pd.DataFrame, cds_phrases: pd.DataFrame,
              cat_ranking: pd.DataFrame, phrase_ranking: pd.DataFrame,
              pdf_path: str | None = None, pdf=None, include_cover: bool = True):
    """Write the CDS prevalence section to pdf_path or an existing pdf handle."""
    if pdf_path is None:
        pdf_path = PDF_PATH

    def _write(writer):
        def save(fig):
            writer.savefig(fig, bbox_inches="tight")
            plt.close("all")

        if include_cover:
            save(_cover_page("Depression Connect Forum",
                             "Most Common Cognitive Distortions (CDS Prevalence)"))
        else:
            save(_section_divider("CDS Prevalence Analysis"))

        save(_section_divider("Section 1 — Category Ranking"))
        save(fig_stats_table(cat_ranking))
        save(fig_category_ranking(cat_ranking))
        save(fig_category_posts_vs_replies(cat_ranking))

        save(_section_divider("Section 2 — Individual CDS Phrases"))
        save(fig_top_phrases(phrase_ranking, top_n=25))
        save(fig_phrase_posts_vs_replies(phrase_ranking, df, top_n=20))

        save(_section_divider("Section 3 — Category Trends Over Time"))
        save(fig_category_trend(df))

    if pdf is not None:
        _write(pdf)
    else:
        with Spinner(f"Building PDF → {pdf_path}"):
            with pdf_backend.PdfPages(pdf_path) as writer:
                _write(writer)
        print(f"  PDF saved → {pdf_path}")


# =============================================================================
# Main
# =============================================================================

def main(dataset: str | None = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ds             = dataset or "combined"
    input_path     = structured_path(OUTPUT_DIR, ds)
    scored_path    = variant_path(OUTPUT_DIR, "cds_scores.csv",              ds)
    pdf_path_out   = variant_path(OUTPUT_DIR, "cds_prevalence_report.pdf",   ds)
    cat_rank_out   = variant_path(OUTPUT_DIR, "cds_category_ranking.csv",    ds)
    phr_rank_out   = variant_path(OUTPUT_DIR, "cds_phrase_ranking.csv",      ds)

    print("Loading and scoring data…")
    df, cds_phrases = get_scored_df(input_path=input_path, scored_path=scored_path)

    print("\nComputing category ranking…")
    cat_ranking = compute_category_ranking(df)
    cat_ranking.to_csv(cat_rank_out, index=False)
    print(f"  Saved → {cat_rank_out}")

    print("\nComputing phrase ranking…")
    phrase_ranking = compute_phrase_ranking(df, cds_phrases)
    phrase_ranking.to_csv(phr_rank_out, index=False)
    print(f"  Saved → {phr_rank_out}")

    sep = "\n" + "─" * 60
    print(sep)
    print("CDS CATEGORY RANKING (most to least prevalent)")
    print(cat_ranking[
        ["category", "prevalence_pct", "prevalence_posts_pct",
         "prevalence_replies_pct", "p_value"]
    ].to_string(index=False))
    print(sep)
    print("TOP 20 INDIVIDUAL CDS PHRASES")
    print(phrase_ranking[
        ["phrase", "category", "total_matches", "prevalence_pct"]
    ].head(20).to_string(index=False))

    build_pdf(df, cds_phrases, cat_ranking, phrase_ranking, pdf_path=pdf_path_out)

    print(f"\n✓ Done.")
    print(f"  {pdf_path_out}")
    print(f"  {cat_rank_out}")
    print(f"  {phr_rank_out}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="CDS prevalence report")
    add_dataset_arg(ap)
    args = ap.parse_args()
    main(dataset=args.dataset)
