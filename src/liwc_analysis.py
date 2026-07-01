# =============================================================================
# liwc_analysis.py  –  LIWC dictionary scoring for Depression Connect messages
#
# Mirrors the structure of exploratory_analysis.py / CDS.py so that LIWC
# categories are processed in the same way as cognitive-distortion schemata.
#
# Supports two LIWC dictionary formats:
#   A) Tab-separated  (.dic / .tsv)  with a header block like:
#          %
#          1   affect
#          2   posemo
#          %
#          word1   1 2
#          word2   1
#   B) CSV (.csv) with columns: term, category  (one row per term-category pair)
#      This is the format exported by LIWC-22 when you export the dictionary.
#
# Produces:
#   output/liwc_scores.csv          – per-message LIWC category flags / counts
#   output/liwc_per_user.csv        – per-user LIWC category prevalence
#   output/liwc_report.pdf          – bar charts + time-series for each category
#
# Run with:  python src/liwc_analysis.py
# =============================================================================

from __future__ import annotations

import os
import re
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
from collections import defaultdict

from tqdm import tqdm
from utils.thread_utils import label_roles
from utils.absolutist import absolutist_rate as _absolutist_rate
from utils.spinner import Spinner
from dataset_io import add_dataset_arg, structured_path, variant_path

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_PATH    = "output/messages_structured.csv"
OUTPUT_DIR    = "output"
PDF_PATH      = os.path.join(OUTPUT_DIR, "liwc_report.pdf")
SCORES_PATH   = os.path.join(OUTPUT_DIR, "liwc_scores.csv")
USER_PATH     = os.path.join(OUTPUT_DIR, "liwc_per_user.csv")

# ── Path to your LIWC dictionary file ────────────────────────────────────────
# Set this to the path of your .dic, .tsv, or .csv LIWC dictionary file.
LIWC_DICT_PATH = "src/liwc15.dic"   # ← change to your actual path

POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
DATE_COL   = "PostDate"
TOPIC_COL  = "ForumTopicID"

PRIMARY  = "#2E5E8E"
C_POST   = "#2166AC"
C_REPLY  = "#D6604D"
SECONDARY = "#EEF3F8"


# =============================================================================
# 1. Load the LIWC dictionary
# =============================================================================

def load_liwc_dic(path: str) -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    Parses a standard LIWC .dic file (percent-sign header format).

    Returns
    -------
    term_to_categories : dict  { "word*" : ["category1", "category2", ...] }
        Keys may contain a trailing wildcard (*) meaning prefix match.
    category_map : dict  { "1" : "affect", "2" : "posemo", ... }
    """
    term_to_categories: dict[str, list[str]] = {}
    category_map: dict[str, str] = {}

    with open(path, encoding="utf-8", errors="replace") as fh:
        lines = [l.rstrip("\n") for l in fh]

    # Find the two % markers that delimit the header
    pct_indices = [i for i, l in enumerate(lines) if l.strip() == "%"]
    if len(pct_indices) < 2:
        raise ValueError(
            "LIWC .dic file must contain two '%' lines surrounding the "
            "category header. Check your file format."
        )

    header_start = pct_indices[0] + 1
    header_end   = pct_indices[1]
    body_start   = header_end + 1

    # Parse category header: "<id>\t<name>"
    for line in lines[header_start:header_end]:
        parts = line.split("\t")
        if len(parts) >= 2:
            cat_id, cat_name = parts[0].strip(), parts[1].strip()
            if cat_id:
                category_map[cat_id] = cat_name

    # Parse body: "<term>\t<id1>\t<id2>..."
    for line in lines[body_start:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        term = parts[0].strip().lower()
        cat_ids = [p.strip() for p in parts[1:] if p.strip()]
        cat_names = [category_map[c] for c in cat_ids if c in category_map]
        if term and cat_names:
            term_to_categories[term] = cat_names

    print(f"  LIWC .dic loaded: {len(category_map)} categories, "
          f"{len(term_to_categories)} terms")
    return term_to_categories, category_map


def load_liwc_csv(path: str) -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    Parses a CSV with columns: term, category  (one row per term-category pair).
    Also accepts a single column where comma-separated categories are in a
    'categories' column.
    """
    df = pd.read_csv(path)
    df.columns = df.columns.str.lower().str.strip()

    # Normalise: expect at least 'term' and 'category' columns
    if "term" not in df.columns:
        raise ValueError("CSV dictionary must have a 'term' column.")
    if "category" not in df.columns and "categories" not in df.columns:
        raise ValueError("CSV dictionary must have a 'category' or 'categories' column.")

    cat_col = "category" if "category" in df.columns else "categories"
    term_to_categories: dict[str, list[str]] = defaultdict(list)

    for _, row in df.iterrows():
        term = str(row["term"]).strip().lower()
        cats_raw = str(row[cat_col]).strip()
        # Allow comma-separated categories in one cell
        cats = [c.strip() for c in cats_raw.split(",") if c.strip()]
        term_to_categories[term].extend(cats)

    category_map = {c: c for cats in term_to_categories.values() for c in cats}
    term_to_categories = dict(term_to_categories)

    print(f"  LIWC CSV loaded: {len(category_map)} unique categories, "
          f"{len(term_to_categories)} terms")
    return term_to_categories, category_map


def load_liwc(path: str) -> tuple[dict[str, list[str]], dict[str, str]]:
    """Auto-detects format and loads the LIWC dictionary."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return load_liwc_csv(path)
    else:  # .dic, .tsv, or unknown → try .dic format
        return load_liwc_dic(path)


# First-person singular — LIWC category detection and Dutch fallback
_FPS_LIWC_CATEGORY = "i"       # standard LIWC-15 category name
_FPS_CATEGORY_NL   = "fps_dutch"
_FPS_DUTCH         = ["ik", "mij", "me", "mijn", "mezelf"]


def ensure_fps(
    term_to_categories: dict[str, list[str]],
    all_categories: list[str],
) -> tuple[dict[str, list[str]], list[str]]:
    """Ensure first-person singular is tracked; inject Dutch fallback if missing."""
    has_fps = any(_FPS_LIWC_CATEGORY in cats for cats in term_to_categories.values())
    if not has_fps:
        print("  WARNING: no first-person-singular ('i') category in LIWC dict "
              "— adding Dutch FPS fallback ('fps_dutch').")
        term_to_categories = dict(term_to_categories)
        for word in _FPS_DUTCH:
            term_to_categories.setdefault(word, []).append(_FPS_CATEGORY_NL)
        all_categories = sorted(set(all_categories) | {_FPS_CATEGORY_NL})
    return term_to_categories, all_categories


# =============================================================================
# 2. Score messages against LIWC dictionary
# =============================================================================

def _tokenize(text: str) -> list[str]:
    """Lowercase and split into word tokens (no punctuation)."""
    return re.findall(r"\b\w+\b", str(text).lower())


def _match_term(token: str, term: str) -> bool:
    """
    Matches a token against a LIWC term.
    Supports wildcard suffix (*): 'happ*' matches 'happy', 'happiness', etc.
    """
    if term.endswith("*"):
        return token.startswith(term[:-1])
    return token == term


def score_text(
    text: str,
    term_to_categories: dict[str, list[str]],
    all_categories: list[str],
) -> dict[str, int]:
    """
    Counts how many tokens in `text` match each LIWC category.

    Returns a dict { category_name: count }.
    Wildcards in dictionary terms are respected.
    """
    tokens = _tokenize(text)
    counts = {cat: 0 for cat in all_categories}

    for token in tokens:
        for term, cats in term_to_categories.items():
            if _match_term(token, term):
                for cat in cats:
                    if cat in counts:
                        counts[cat] += 1
                break  # first matching term wins (standard LIWC behaviour)

    return counts


def score_messages(
    df: pd.DataFrame,
    term_to_categories: dict[str, list[str]],
    all_categories: list[str],
) -> pd.DataFrame:
    """
    Applies LIWC scoring to every row in df[TEXT_COL].

    Adds columns:
      - one count column per LIWC category  (e.g. 'liwc_affect')
      - 'word_count'     – total tokens in the message
      - one pct column per category  (e.g. 'liwc_affect_pct') – count / word_count * 100
    """
    print(f"  Scoring {len(df)} messages against {len(all_categories)} LIWC categories…")

    results = []
    for text in tqdm(df[TEXT_COL].fillna(""), desc="LIWC scoring", unit="msg"):
        results.append(score_text(text, term_to_categories, all_categories))

    scores_df = pd.DataFrame(results)

    # Prefix category columns so they don't clash with other columns
    scores_df = scores_df.add_prefix("liwc_")
    liwc_cols = list(scores_df.columns)

    df = df.reset_index(drop=True)
    df = pd.concat([df, scores_df], axis=1)

    # Word count
    df["word_count"] = df[TEXT_COL].fillna("").apply(lambda t: len(_tokenize(t)))

    # Percentage columns
    for col in liwc_cols:
        pct_col = col + "_pct"
        df[pct_col] = (df[col] / df["word_count"].clip(lower=1) * 100).round(3)

    return df, liwc_cols


def add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"]       = df[DATE_COL].dt.year
    df["year_month"] = df[DATE_COL].dt.to_period("M")
    df["month_dt"]   = df[DATE_COL].dt.to_period("M").dt.to_timestamp()
    return df


# =============================================================================
# 4. Per-user aggregation
# =============================================================================

def per_user_summary(
    df: pd.DataFrame, liwc_cols: list[str]
) -> pd.DataFrame:
    """
    Returns a per-user summary with:
      - total word count
      - sum of each LIWC category count
      - mean percentage of each LIWC category
    """
    agg = {col: "sum" for col in liwc_cols}
    agg["word_count"] = "sum"
    agg[TEXT_COL] = "count"

    per_user = (
        df.groupby(POSTER_COL)
        .agg(agg)
        .reset_index()
        .rename(columns={TEXT_COL: "message_count"})
    )

    # Recompute percentages at user level
    for col in liwc_cols:
        pct_col = col + "_pct"
        per_user[pct_col] = (
            per_user[col] / per_user["word_count"].clip(lower=1) * 100
        ).round(3)

    return per_user.sort_values("word_count", ascending=False)


# =============================================================================
# 5. PDF report
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


def fig_absolutist_by_role(df: pd.DataFrame) -> plt.Figure:
    """Bar chart: mean absolutist word rate by role (posts vs replies)."""
    posts   = df[df["role"] == "post"]["absolutist_rate"].mean()
    replies = df[df["role"] == "reply"]["absolutist_rate"].mean()

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(["Opening posts", "Replies"], [posts, replies],
           color=[C_POST, C_REPLY], alpha=0.85, edgecolor="white")
    for i, v in enumerate([posts, replies]):
        ax.text(i, v + 0.005, f"{v:.2f}%", ha="center", fontsize=9, color="#333333")
    _style_ax(
        ax,
        "Absolutist Word Rate by Role\n(Dutch function-word list; Al-Mosaiwi & Johnstone 2018)",
        "Role", "Mean absolutist word rate (%)",
    )
    fig.tight_layout()
    return fig


def fig_category_prevalence(df: pd.DataFrame, liwc_cols: list[str]) -> plt.Figure:
    """
    Horizontal bar chart: mean % of each LIWC category across all messages,
    sorted descending.
    """
    pct_cols = [c + "_pct" for c in liwc_cols if c + "_pct" in df.columns]
    means = df[pct_cols].mean().sort_values(ascending=False)
    labels = [c.replace("liwc_", "").replace("_pct", "") for c in means.index]

    n = len(labels)
    fig, ax = plt.subplots(figsize=(10, max(4, n * 0.35)))
    y = np.arange(n)
    ax.barh(y, means.values, color=PRIMARY, alpha=0.85, edgecolor="white")
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    _style_ax(ax, "Overall LIWC Category Prevalence (mean % of words)",
              "Mean % of words", "Category")
    fig.tight_layout()
    return fig


def fig_posts_vs_replies_categories(
    df: pd.DataFrame, liwc_cols: list[str], top_n: int = 20
) -> plt.Figure:
    """
    Grouped bar chart: top N LIWC categories, posts vs replies.
    """
    posts   = df[df["role"] == "post"]
    replies = df[df["role"] == "reply"]

    pct_cols = [c + "_pct" for c in liwc_cols if c + "_pct" in df.columns]
    rows = []
    for col in pct_cols:
        cat = col.replace("liwc_", "").replace("_pct", "")
        rows.append({
            "category": cat,
            "post":  posts[col].mean()  if len(posts)   > 0 else 0,
            "reply": replies[col].mean() if len(replies) > 0 else 0,
        })

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
    _style_ax(ax,
              f"Top {top_n} LIWC Categories — Posts vs Replies (mean % of words)",
              "Category", "Mean % of words")
    fig.tight_layout()
    return fig


def fig_category_over_time(
    df: pd.DataFrame, liwc_cols: list[str], top_n: int = 12
) -> plt.Figure:
    """
    Small multiples: monthly prevalence for the top N LIWC categories.
    """
    pct_cols = [c + "_pct" for c in liwc_cols if c + "_pct" in df.columns]
    overall_means = df[pct_cols].mean().sort_values(ascending=False)
    top_cols = list(overall_means.head(top_n).index)

    n    = len(top_cols)
    cols = 3
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(14, rows * 3), sharex=False)
    axes_flat = axes.flatten()

    for i, col in enumerate(top_cols):
        ax  = axes_flat[i]
        cat = col.replace("liwc_", "").replace("_pct", "")

        monthly = df.groupby("month_dt")[col].mean().reset_index()
        monthly.columns = ["month_dt", "mean_pct"]

        ax.plot(monthly["month_dt"], monthly["mean_pct"],
                color=PRIMARY, linewidth=1.2)
        ax.fill_between(monthly["month_dt"], monthly["mean_pct"],
                        alpha=0.12, color=PRIMARY)
        ax.set_title(cat, fontsize=8, fontweight="bold", color=PRIMARY)
        ax.tick_params(axis="x", rotation=30, labelsize=6)
        ax.tick_params(axis="y", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    for j in range(n, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle(f"Top {top_n} LIWC Categories — Monthly Prevalence Over Time",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def build_pdf(df: pd.DataFrame, liwc_cols: list[str],
              pdf_path: str | None = None, pdf=None, include_cover: bool = True):
    """Write the LIWC analysis section to pdf_path or an existing pdf handle."""
    if pdf_path is None:
        pdf_path = PDF_PATH

    def _write(writer):
        def save(fig):
            writer.savefig(fig, bbox_inches="tight")
            plt.close("all")

        if include_cover:
            save(_cover_page("Depression Connect Forum",
                             "LIWC Psycholinguistic Feature Analysis"))
        else:
            save(_section_divider("LIWC Psycholinguistic Analysis"))

        save(_section_divider("Section 1 — Overall Category Prevalence"))
        save(fig_category_prevalence(df, liwc_cols))

        save(_section_divider("Section 2 — Posts vs Replies"))
        save(fig_posts_vs_replies_categories(df, liwc_cols))

        save(_section_divider("Section 3 — Top Categories Over Time"))
        save(fig_category_over_time(df, liwc_cols))

        if "absolutist_rate" in df.columns:
            save(_section_divider("Section 4 — Absolutist Words"))
            save(fig_absolutist_by_role(df))

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
    ds           = dataset or "combined"
    input_path   = structured_path(OUTPUT_DIR, ds)
    scores_out   = variant_path(OUTPUT_DIR, "liwc_scores.csv",   ds)
    user_out     = variant_path(OUTPUT_DIR, "liwc_per_user.csv", ds)
    pdf_out      = variant_path(OUTPUT_DIR, "liwc_report.pdf",   ds)

    print("Loading messages…")
    df = pd.read_csv(input_path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TEXT_COL]).copy()
    print(f"  {len(df)} messages from {df[POSTER_COL].nunique()} users.")

    df = label_roles(df)
    df = add_time_columns(df)

    print(f"\nLoading LIWC dictionary from {LIWC_DICT_PATH}…")
    if not os.path.exists(LIWC_DICT_PATH):
        raise FileNotFoundError(
            f"LIWC dictionary not found at: {LIWC_DICT_PATH}\n"
            "Set LIWC_DICT_PATH at the top of this script to your .dic or .csv file."
        )
    term_to_categories, category_map = load_liwc(LIWC_DICT_PATH)
    all_categories = sorted(set(category_map.values()))

    term_to_categories, all_categories = ensure_fps(term_to_categories, all_categories)

    print("\nScoring messages…")
    df, liwc_cols = score_messages(df, term_to_categories, all_categories)
    df["absolutist_rate"] = df[TEXT_COL].apply(_absolutist_rate)

    df.to_csv(scores_out, index=False)
    print(f"  Saved scored messages → {scores_out}")

    user_df = per_user_summary(df, liwc_cols)
    user_df.to_csv(user_out, index=False)
    print(f"  Saved per-user summary → {user_out}")

    pct_cols = [c + "_pct" for c in liwc_cols if c + "_pct" in df.columns]
    top10 = df[pct_cols].mean().sort_values(ascending=False).head(10)
    print("\nTop 10 LIWC categories by mean % of words:")
    for cat, val in top10.items():
        print(f"  {cat.replace('liwc_', '').replace('_pct', ''):<30}  {val:.3f}%")

    print()
    build_pdf(df, liwc_cols, pdf_path=pdf_out)

    print("\n✓ Done.")
    print(f"  {scores_out}")
    print(f"  {user_out}")
    print(f"  {pdf_out}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="LIWC analysis report")
    add_dataset_arg(ap)
    args = ap.parse_args()
    main(dataset=args.dataset)
