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

from utils.thread_utils import label_roles, strip_entity_placeholders_col, parse_post_dates
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
    df[DATE_COL] = parse_post_dates(df[DATE_COL])
    df = df.dropna(subset=[DATE_COL])
    return strip_entity_placeholders_col(df, TEXT_COL)


def get_top_posters(df: pd.DataFrame, n: int = 5) -> list[str]:
    return df[POSTER_COL].value_counts().head(n).index.tolist()


# =============================================================================
# User engagement / selection
# =============================================================================

def compute_user_engagement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-user engagement metrics: post volume, active span, and posting
    intensity. Moderators are already excluded upstream (preprocess.py).

    Columns: PosterID, n_posts, active_days, posts_per_active_month,
             r_posts, r_span, sustained  (rank-based composite in [0,1]).
    """
    g = (
        df.groupby(POSTER_COL)
        .agg(n_posts=(DATE_COL, "size"),
             first=(DATE_COL, "min"),
             last=(DATE_COL, "max"))
        .reset_index()
    )
    g["active_days"] = (g["last"] - g["first"]).dt.days
    # posts per active month; floor the span at one day so single-day users
    # don't divide by zero (they get their full count as "one month").
    months = (g["active_days"] / 30.44).clip(lower=1 / 30.44)
    g["posts_per_active_month"] = (g["n_posts"] / months).round(2)
    # Rank-based composite so volume and span combine on a common [0,1] scale.
    g["r_posts"] = g["n_posts"].rank(pct=True)
    g["r_span"] = g["active_days"].rank(pct=True)
    g["sustained"] = g[["r_posts", "r_span"]].mean(axis=1)
    return g.drop(columns=["first", "last"])


def classify_shape(eng_sel: pd.DataFrame, intensity_multiple: float = 2.0) -> pd.Series:
    """
    Within the selected users, label posting *shape* by posting cadence:
      - 'high-intensity'   : posts-per-active-month at least `intensity_multiple`
                             times the group median (bursts of many posts).
      - 'long-haul steady' : otherwise (a lower cadence sustained over a long span).

    Using a multiple of the median (rather than the median itself) avoids
    splitting users with near-identical cadence onto opposite sides — only a
    genuine step up in intensity is labelled 'high-intensity'. If the group is
    uniform in cadence, all are 'long-haul steady', which is the honest result.
    """
    threshold = eng_sel["posts_per_active_month"].median() * intensity_multiple
    return eng_sel["posts_per_active_month"].apply(
        lambda v: "high-intensity" if v >= threshold else "long-haul steady"
    )


def select_users(df: pd.DataFrame, n: int, mode: str) -> tuple[list[str], pd.DataFrame]:
    """
    Return (ordered user id list, engagement table for the selection).

    mode='count'     : top-n by raw post volume (legacy behaviour).
    mode='sustained' : top-n by the rank composite of volume AND active span,
                       i.e. users who are both prolific and long-active.
    """
    eng = compute_user_engagement(df)
    key = "n_posts" if mode == "count" else "sustained"
    sel = eng.sort_values(key, ascending=False).head(n).copy()
    if mode == "sustained":
        sel["shape"] = classify_shape(sel)
        # Group by shape, then by sustained score, so the report reads as a
        # side-by-side comparison of the two shapes.
        sel = sel.sort_values(["shape", "sustained"], ascending=[True, False])
    return sel[POSTER_COL].tolist(), sel


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


def _engagement_table_page(eng_sel: pd.DataFrame, mode: str):
    """Rationale page: the selected users, their engagement metrics, and shape."""
    fig, ax = plt.subplots(figsize=(12, max(3, len(eng_sel) * 0.45 + 2.5)))
    ax.axis("off")

    has_shape = "shape" in eng_sel.columns
    cols = ["PosterID", "n_posts", "active_days", "posts_per_active_month"]
    headers = ["User", "Posts", "Active days", "Posts / active month"]
    if has_shape:
        cols.append("shape")
        headers.append("Shape")

    cell = [[str(r[c]) for c in cols] for _, r in eng_sel.iterrows()]
    tbl = ax.table(cellText=cell, colLabels=headers, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    for j in range(len(headers)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    # Tint rows by shape so the two groups read as blocks.
    shape_color = {"long-haul steady": "#EAF1F8", "high-intensity": "#FBEFD6"}
    for i, (_, r) in enumerate(eng_sel.iterrows(), start=1):
        bg = shape_color.get(r.get("shape"), SECONDARY) if has_shape else \
            (SECONDARY if i % 2 == 0 else "white")
        for j in range(len(headers)):
            tbl[(i, j)].set_facecolor(bg)

    if mode == "sustained":
        title = ("Selected Users — Sustained Engagement (high post volume AND long active span)")
        note = ("Selection ranks each user on post volume and active-span percentiles and "
                "takes the top by their average. Shape splits the group by posting cadence "
                "(posts per active month): high-intensity = at least twice the group's median "
                "cadence (bursty); long-haul steady = a lower cadence sustained over a long span.\n"
                "Moderators are excluded upstream. The pages that follow track each user's "
                "CDS and LIWC markers over their months of activity.")
    else:
        title = "Selected Users — Top by Raw Post Volume"
        note = ("Selection is by total post count only. Note this over-weights "
                "high-intensity users and can miss long-active, moderate-volume users "
                "(see --select sustained).")

    ax.set_title(title, fontsize=12, fontweight="bold", color=PRIMARY, pad=14)
    fig.text(0.5, 0.02, note, ha="center", va="bottom", fontsize=8.5,
             color="#444444", wrap=True)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    return fig


def _time_series_page(monthly_df: pd.DataFrame, top_users: list[str],
                      value_cols: list[str], suptitle: str,
                      user_labels: dict[str, str] | None = None):
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

        title = user_labels.get(user_id, user_id) if user_labels else user_id
        ax.set_title(title, fontsize=9, fontweight="bold", color=PRIMARY)
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

def run(input_path: str | None = None, dataset: str | None = None,
        top_n: int = 5, select: str = "count"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ds = dataset or "combined"
    if input_path is None:
        input_path = structured_path(OUTPUT_DIR, ds)
    # Give the sustained-engagement zoom-in its own filename so it doesn't
    # overwrite the default top-by-count longitudinal report.
    base = ("user_longitudinal_sustained.pdf" if select == "sustained"
            else "user_longitudinal_report.pdf")
    pdf_path = variant_path(OUTPUT_DIR, base, ds)

    print(f"Loading {input_path}…")
    df = load_data(input_path)
    top_users, eng_sel = select_users(df, n=top_n, mode=select)
    print(f"Selected {len(top_users)} users (mode={select}): {top_users}")
    df_top = df[df[POSTER_COL].isin(top_users)].copy()

    # Per-user subplot labels: append shape + intensity when in sustained mode.
    user_labels = None
    if select == "sustained" and "shape" in eng_sel.columns:
        user_labels = {
            r[POSTER_COL]: f"{r[POSTER_COL]}  ·  {r['shape']}  ·  "
                           f"{int(r['n_posts'])} posts / {int(r['active_days'])}d"
            for _, r in eng_sel.iterrows()
        }

    print("Scoring CDS…")
    cds_df, cds_cols = score_cds(df_top)
    cds_monthly = aggregate_monthly(cds_df, cds_cols) if cds_cols else pd.DataFrame()

    print("Scoring LIWC…")
    liwc_df, liwc_cols = score_liwc(df_top)
    liwc_monthly = aggregate_monthly(liwc_df, liwc_cols) if not liwc_df.empty else pd.DataFrame()

    subtitle = (f"Sustained-Engagement Zoom-In — Top {top_n} by Volume & Active Span"
                if select == "sustained"
                else f"Longitudinal Analysis — Top {top_n} Most Active Posters")

    print(f"Building PDF → {pdf_path}")
    with pdf_backend.PdfPages(pdf_path) as pdf:
        def save(fig):
            pdf.savefig(fig, bbox_inches="tight")
            plt.close("all")

        save(_cover("Depression Connect Forum", subtitle))
        save(_engagement_table_page(eng_sel, mode=select))

        if not cds_monthly.empty and cds_cols:
            save(_section("CDS Category Scores Per Month"))
            save(_time_series_page(cds_monthly, top_users, cds_cols,
                                   "CDS Categories Over Time (mean per month)",
                                   user_labels=user_labels))

        if not liwc_monthly.empty and liwc_cols:
            save(_section("LIWC Category Scores Per Month"))
            save(_time_series_page(liwc_monthly, top_users, liwc_cols,
                                   "Top LIWC Categories Over Time (mean % of words per month)",
                                   user_labels=user_labels))

    print(f"  Saved → {pdf_path}")
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Per-user longitudinal LIWC and CDS analysis."
    )
    add_dataset_arg(parser)
    parser.add_argument("--top", type=int, default=5,
                        help="Number of top posters to analyse (default: 5)")
    parser.add_argument("--select", choices=["count", "sustained"], default="count",
                        help="User selection: 'count' = raw post volume (default); "
                             "'sustained' = high volume AND long active span, "
                             "compared by posting shape.")
    parser.add_argument("--input", help="Override input file path (ignores --dataset)")
    args = parser.parse_args()

    run(input_path=args.input, dataset=args.dataset, top_n=args.top, select=args.select)
