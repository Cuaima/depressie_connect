"""
full_report.py — consolidated full-pipeline PDF report

Loads data ONCE for a given dataset, scores CDS and LIWC once, then writes
every analysis section into a single PDF file.

Sections
--------
1. Exploratory Data Analysis (exploration.py)
2. Forum Activity & Time Series (exploratory_analysis.py)
3. CDS Prevalence (cds_prevalence.py)
4. LIWC Psycholinguistic Analysis (liwc_analysis.py)   ← skipped if no .dic file

Usage
-----
    python src/full_report.py
    python src/full_report.py --dataset old
    python src/full_report.py --all          # run old, new_only, combined
"""
from __future__ import annotations

import argparse
import os
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
import pandas as pd

from utils.thread_utils import label_roles
from utils.CDS import process_dataset
from utils.absolutist import absolutist_rate as _absolutist_rate

import exploration         as ex
import exploratory_analysis as ea
import cds_prevalence      as cp
import liwc_analysis       as la

from dataset_io import add_dataset_arg, variant_path, subtitle_for

warnings.filterwarnings("ignore")

OUTPUT_DIR = "output"
_DATASET_LABEL = {
    None:       "Combined Dataset",
    "combined": "Combined Dataset",
    "old":      "Old Dataset",
    "new_only": "New Dataset",
}

DATE_COL   = ea.DATE_COL
TEXT_COL   = ea.TEXT_COL
POSTER_COL = ea.POSTER_COL


# =============================================================================
# Helpers
# =============================================================================

def _cover(title: str, subtitle: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("#2E5E8E")
    ax.set_facecolor("#2E5E8E")
    ax.axis("off")
    ax.text(0.5, 0.65, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=24,
            fontweight="bold", color="white")
    ax.text(0.5, 0.48, subtitle, transform=ax.transAxes,
            ha="center", va="center", fontsize=14, color="#DDDDDD")
    return fig


def _section_divider(title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 1.8))
    fig.patch.set_facecolor("#EEF3F8")
    ax.set_facecolor("#EEF3F8")
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=16,
            fontweight="bold", color="#2E5E8E")
    fig.tight_layout()
    return fig


# =============================================================================
# Data loading + scoring
# =============================================================================

def load_and_score(dataset: str | None) -> dict:
    """Load, label, score CDS and LIWC for the given dataset. Returns a result dict."""
    from dataset_io import structured_path
    input_path = structured_path(OUTPUT_DIR, dataset or "combined")

    print(f"Loading {input_path}…")
    df = pd.read_csv(input_path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TEXT_COL]).copy()
    print(f"  {len(df)} messages from {df[POSTER_COL].nunique()} users.")

    df = label_roles(df)
    df = ea.add_time_columns(df)

    # ── CDS scoring (one call for all outputs) ────────────────────────────────
    print("Scoring CDS…")
    raw = pd.DataFrame({"text": df[TEXT_COL].fillna("").str.lower().values})
    cds_phrases, cds_per_category, cds_per_tweet = process_dataset(
        raw, output="all_variants", language="NL"
    )
    cds_phrases      = cds_phrases.reset_index(drop=True)
    cds_per_category = cds_per_category.reset_index(drop=True)
    df               = df.reset_index(drop=True)
    df["CDS"]        = cds_per_tweet["CDS"].values
    for col in cds_per_category.columns:
        df[col] = cds_per_category[col].values
    overall = df["CDS"].mean() * 100
    print(f"  Overall CDS prevalence: {overall:.2f}%")

    # ── LIWC scoring (skip gracefully if no dictionary) ───────────────────────
    liwc_cols: list[str] = []
    if os.path.exists(la.LIWC_DICT_PATH):
        print(f"Scoring LIWC from {la.LIWC_DICT_PATH}…")
        term_to_cats, cat_map  = la.load_liwc(la.LIWC_DICT_PATH)
        all_cats               = sorted(set(cat_map.values()))
        term_to_cats, all_cats = la.ensure_fps(term_to_cats, all_cats)
        df, liwc_cols          = la.score_messages(df, term_to_cats, all_cats)
        df["absolutist_rate"]  = df[TEXT_COL].apply(_absolutist_rate)
        df                     = la.add_time_columns(df)
    else:
        print(f"  LIWC dictionary not found at {la.LIWC_DICT_PATH} — skipping LIWC section.")

    # ── Pre-compute rankings needed by cds_prevalence figures ─────────────────
    cat_ranking    = cp.compute_category_ranking(df)
    phrase_ranking = cp.compute_phrase_ranking(df, cds_phrases)

    return {
        "df":             df,
        "cds_phrases":    cds_phrases,
        "cat_ranking":    cat_ranking,
        "phrase_ranking": phrase_ranking,
        "liwc_cols":      liwc_cols,
    }


# =============================================================================
# Report builder
# =============================================================================

def build_full_report(dataset: str | None = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ds       = dataset or "combined"
    label    = _DATASET_LABEL.get(dataset, "Combined Dataset")
    pdf_path = variant_path(OUTPUT_DIR, "full_report.pdf", ds)

    data = load_and_score(dataset)
    df            = data["df"]
    cds_phrases   = data["cds_phrases"]
    cat_ranking   = data["cat_ranking"]
    phrase_ranking= data["phrase_ranking"]
    liwc_cols     = data["liwc_cols"]

    eda_stats = ex.compute_stats(df)

    print(f"\nBuilding full report → {pdf_path}")
    with pdf_backend.PdfPages(pdf_path) as pdf:

        def save(fig):
            pdf.savefig(fig, bbox_inches="tight")
            plt.close("all")

        # ── Cover ─────────────────────────────────────────────────────────────
        save(_cover("Depression Connect Forum",
                    f"Full Analysis Report — {label}"))

        # ── Section 1: EDA ────────────────────────────────────────────────────
        print("  Section 1: EDA…")
        save(_section_divider("Part 1 — Exploratory Data Analysis"))
        ex.build_pdf(eda_stats, subtitle=label, df=df,
                     pdf=pdf, include_cover=False)

        # ── Section 2: Activity & CDS time series ─────────────────────────────
        print("  Section 2: Activity & CDS time series…")
        save(_section_divider("Part 2 — Forum Activity & CDS Prevalence Over Time"))
        ea.build_pdf(df, pdf=pdf, include_cover=False)

        # ── Section 3: CDS prevalence detail ──────────────────────────────────
        print("  Section 3: CDS prevalence detail…")
        save(_section_divider("Part 3 — CDS Category & Phrase Analysis"))
        cp.build_pdf(df, cds_phrases, cat_ranking, phrase_ranking,
                     pdf=pdf, include_cover=False)

        # ── Section 4: LIWC (optional) ────────────────────────────────────────
        if liwc_cols:
            print("  Section 4: LIWC…")
            save(_section_divider("Part 4 — LIWC Psycholinguistic Analysis"))
            la.build_pdf(df, liwc_cols, pdf=pdf, include_cover=False)

    print(f"\n✓ Full report saved → {pdf_path}")
    return pdf_path


# =============================================================================
# Entry point
# =============================================================================

def main():
    from dataset_io import DATASET_CHOICES
    ap = argparse.ArgumentParser(description="Generate the full consolidated analysis report.")
    add_dataset_arg(ap)
    ap.add_argument("--all", dest="run_all", action="store_true",
                    help="Run for all three dataset variants sequentially")
    args = ap.parse_args()

    if args.run_all:
        for ds in DATASET_CHOICES:
            print(f"\n{'='*60}\n  Dataset: {ds}\n{'='*60}")
            build_full_report(dataset=ds)
    else:
        build_full_report(dataset=args.dataset)


if __name__ == "__main__":
    main()
