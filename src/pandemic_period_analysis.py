# =============================================================================
# pandemic_period_analysis.py  –  psycholinguistic markers across pandemic periods
#
# Operationalizes Yahya & Abdul Rahim (2023), "Linguistic markers of
# depression" (Language and Health 1, 36-50) on the Depression Connect forum:
# compares LIWC-style features across three pandemic periods (pre / during /
# post), per dataset variant.
#
# Deliberate statistical departure from the paper: Yahya used pooled-corpus
# log-likelihood, which suffers from pseudo-replication. We aggregate to
# per-user rates first, then use Kruskal-Wallis (omnibus) + pairwise
# Mann-Whitney U (post-hoc) with Benjamini-Hochberg FDR correction.
# See docs/statistical_decisions.md §9.
#
# Reads (produced by earlier pipeline steps — not re-scored here):
#   output/liwc_scores{_variant}.csv     – custom LIWC-2015 scorer (liwc_analysis.py)
#   output/liwc22_scores{_variant}.csv   – LIWC-22 CLI (liwc22_cli_runner.py)
#   output/messages_structured{_v}.csv   – for the variant × period diagnostic
#
# At least one of the two score files must exist; otherwise RuntimeError.
#
# Produces:
#   output/pandemic_period_report.pdf{_variant}
#   output/pandemic_period_stats.csv{_variant}
#
# Period boundaries come from config.py (PANDEMIC_CUTOFF_DATE /
# PANDEMIC_END_DATE). A --end-date override is accepted for experimentation;
# the report is then marked PROVISIONAL. If no end date is available at all,
# the script refuses to run (loud failure, non-zero exit).
#
# Run with:  python src/pandemic_period_analysis.py [--dataset combined|old|new_only]
#            python src/pandemic_period_analysis.py --all
# =============================================================================

from __future__ import annotations

import os
import sys
import argparse
import warnings

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from statsmodels.stats.multitest import multipletests
from tqdm import tqdm
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend

from config import PANDEMIC_CUTOFF_DATE, PANDEMIC_END_DATE
from dataset_io import (
    add_dataset_arg, structured_path, variant_path, subtitle_for, DATASET_CHOICES,
)
from utils.spinner import Spinner
from utils.absolutist import absolutist_rate
import liwc_analysis
from liwc22_cli_runner import LIWC22_STRUCTURAL_COLS, LIWC22_SUMMARY_VARS

warnings.filterwarnings("ignore")

OUTPUT_DIR = "output"
POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
DATE_COL   = "PostDate"

PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"
C_PRE     = "#2166AC"
C_DURING  = "#E8A838"
C_POST    = "#D6604D"

PERIODS       = ["pre", "during", "post"]
PERIOD_COLORS = {"pre": C_PRE, "during": C_DURING, "post": C_POST}
PAIRS         = [("pre", "during"), ("during", "post"), ("pre", "post")]

# Minimum users per period group for a feature to enter a test.
MIN_USERS_PER_GROUP = 5

# Max features shown as boxplot pages in the PDF
_MAX_BOXPLOTS = 12

# ── Yahya & Abdul Rahim (2023) §2.3 feature groups ───────────────────────────
# Alias lists are matched (lowercase) against whatever categories the
# dictionaries actually contain — only available categories are analyzed.
YAHYA_GROUPS: dict[str, set[str]] = {
    "pronouns":       {"pronoun", "ppron", "i", "we", "you", "shehe", "they",
                       "ipron", "fps_dutch"},
    "function_words": {"funct", "function", "article", "prep", "preps", "conj",
                       "auxverb", "auxvb", "negate", "quant", "number"},
    "other_grammar":  {"verb", "adverb", "adj", "compare", "interrog"},
    "tense":          {"past", "present", "future",
                       "focuspast", "focuspresent", "focusfuture"},
    "emotion":        {"affect", "posemo", "negemo", "anx", "anger", "sad",
                       "emotion", "emo_pos", "emo_neg", "tone_pos", "tone_neg"},
    "informal":       {"informal", "swear", "netspeak", "assent",
                       "nonflu", "nonfl", "filler"},
    "summary":        {"analytic"},
    "absolutist":     {"absolutist"},
}


def yahya_group_of(category: str) -> str | None:
    cat = category.lower()
    for group, aliases in YAHYA_GROUPS.items():
        if cat in aliases:
            return group
    return None


# =============================================================================
# Period assignment
# =============================================================================

def assign_period(dates: pd.Series, cutoff: str, end: str) -> pd.Series:
    cutoff_ts = pd.Timestamp(cutoff)
    end_ts    = pd.Timestamp(end)
    if end_ts <= cutoff_ts:
        raise ValueError(
            f"PANDEMIC_END_DATE ({end}) must be after PANDEMIC_CUTOFF_DATE ({cutoff})."
        )
    period = pd.Series("during", index=dates.index)
    period[dates < cutoff_ts] = "pre"
    period[dates >= end_ts]   = "post"
    return pd.Categorical(period, categories=PERIODS, ordered=True)


# =============================================================================
# Data loading + feature extraction
# =============================================================================

def _load_dated_csv(path: str, usecols=None) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=usecols, low_memory=False)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    return df.dropna(subset=[DATE_COL])


def load_liwc2015_features(dataset: str) -> tuple[pd.DataFrame | None, dict[str, str]]:
    """
    Load liwc_scores{_v}.csv and return (df, {feature_col: bare_category}).
    Features are the liwc_*_pct columns whose bare category maps to a Yahya
    group, plus absolutist_rate. Returns (None, {}) if the file is absent.
    """
    path = variant_path(OUTPUT_DIR, "liwc_scores.csv", dataset)
    if not os.path.exists(path):
        return None, {}
    with Spinner(f"Loading {path}"):
        df = _load_dated_csv(path)
    feat_map: dict[str, str] = {}
    for col in df.columns:
        if col.startswith("liwc_") and col.endswith("_pct"):
            bare = col[len("liwc_"):-len("_pct")]
            if yahya_group_of(bare):
                feat_map[col] = bare
    if "absolutist_rate" in df.columns:
        feat_map["absolutist_rate"] = "absolutist"
    print(f"  liwc_scores: {len(df)} messages, "
          f"{len(feat_map)} Yahya-relevant feature columns.")
    return df, feat_map


def load_liwc22_features(dataset: str) -> tuple[pd.DataFrame | None, dict[str, str]]:
    """
    Load liwc22_scores{_v}.csv and return (df, {feature_col: bare_category}).
    Content-category columns are matched against the Yahya alias lists;
    summary variables (Analytic etc.) are included only if actually present.
    Returns (None, {}) if the file is absent.
    """
    path = variant_path(OUTPUT_DIR, "liwc22_scores.csv", dataset)
    if not os.path.exists(path):
        return None, {}
    with Spinner(f"Loading {path}"):
        df = _load_dated_csv(path)
    non_cat = LIWC22_STRUCTURAL_COLS | {"_row_idx", POSTER_COL, DATE_COL,
                                        "ForumTopicID", "role"}
    feat_map: dict[str, str] = {}
    for col in df.columns:
        if col in non_cat:
            continue
        if col in LIWC22_SUMMARY_VARS or yahya_group_of(col):
            feat_map[col] = col
    print(f"  liwc22_scores: {len(df)} messages, "
          f"{len(feat_map)} Yahya-relevant feature columns.")
    return df, feat_map


def compute_absolutist_fallback(dataset: str) -> pd.DataFrame | None:
    """
    If liwc_scores.csv is unavailable, compute absolutist_rate directly from
    the structured messages so the Al-Mosaiwi & Johnstone feature is still
    covered. Returns None if the structured file is also missing.
    """
    path = structured_path(OUTPUT_DIR, dataset)
    if not os.path.exists(path):
        return None
    df = _load_dated_csv(path, usecols=[POSTER_COL, DATE_COL, TEXT_COL])
    tqdm.pandas(desc="Absolutist scoring", unit="msg")
    df["absolutist_rate"] = df[TEXT_COL].fillna("").progress_apply(absolutist_rate)
    return df[[POSTER_COL, DATE_COL, "absolutist_rate"]]


# =============================================================================
# Dictionary introspection
# =============================================================================

def introspect_dictionaries(
    liwc2015_map: dict[str, str],
    liwc22_map: dict[str, str],
) -> pd.DataFrame:
    """
    One row per Yahya group: which categories each source actually provides.
    Also loads the LIWC-2015 .dic (if present) as a cross-check on the score
    file's coverage.
    """
    dic_cats: set[str] = set()
    dic_path = liwc_analysis.LIWC_DICT_PATH
    if os.path.exists(dic_path):
        try:
            _terms, category_map = liwc_analysis.load_liwc(dic_path)
            dic_cats = {c.lower() for c in category_map.values()}
        except Exception as exc:  # introspection is diagnostic, not critical
            print(f"  WARNING: could not parse {dic_path}: {exc}")
    else:
        print(f"  NOTE: LIWC-2015 dictionary not found at {dic_path} "
              "(introspection uses score-file columns only).")

    rows = []
    for group, aliases in YAHYA_GROUPS.items():
        in_2015 = sorted(bare for bare in liwc2015_map.values()
                         if bare.lower() in aliases)
        in_22   = sorted(bare for bare in liwc22_map.values()
                         if bare.lower() in aliases
                         or (group == "summary" and bare in LIWC22_SUMMARY_VARS))
        in_dic  = sorted(aliases & dic_cats)
        rows.append({
            "yahya_group":   group,
            "liwc2015":      ", ".join(in_2015) if in_2015 else "—",
            "liwc22":        ", ".join(in_22)   if in_22   else "—",
            "dic_crosscheck": ", ".join(in_dic) if in_dic  else "—",
            "available":     bool(in_2015 or in_22),
        })
    avail = pd.DataFrame(rows)

    print("\n── Dictionary introspection (Yahya §2.3 vs available categories) ──")
    for _, r in avail.iterrows():
        marker = "✓" if r["available"] else "✗ OMITTED"
        print(f"  {marker:<10} {r['yahya_group']:<15} "
              f"LIWC-2015: {r['liwc2015']:<40} LIWC-22: {r['liwc22']}")
    return avail


# =============================================================================
# Per-user aggregation
# =============================================================================

def per_user_period_rates(
    df: pd.DataFrame,
    feat_map: dict[str, str],
    source: str,
    cutoff: str,
    end: str,
) -> pd.DataFrame:
    """
    Aggregate message-level rates to one row per (user, period), with feature
    columns renamed '{category}__{source}'. Never test at message level.
    """
    df = df.copy()
    df["period"] = assign_period(df[DATE_COL], cutoff, end)
    keep = [POSTER_COL, "period"] + list(feat_map)
    agg = (
        df[keep]
        .groupby([POSTER_COL, "period"], observed=True)
        .mean()
        .reset_index()
    )
    agg = agg.rename(columns={col: f"{bare}__{source}"
                              for col, bare in feat_map.items()})
    return agg


def merge_sources(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer-merge per-user-period frames from the available sources."""
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on=[POSTER_COL, "period"], how="outer")
    out["period"] = pd.Categorical(out["period"], categories=PERIODS, ordered=True)
    return out


# =============================================================================
# Diagnostics
# =============================================================================

def crosstab_variant_period(cutoff: str, end: str) -> pd.DataFrame:
    """
    Mandatory diagnostic: dataset_variant × pandemic_period cross-tab of
    message counts AND unique-user counts, across all variants whose
    structured file exists. Period effects are confounded with variant
    (old export ≈ pre+during, new export ≈ post) — this table makes that
    visible instead of buried.
    """
    rows = []
    for ds in DATASET_CHOICES:
        path = structured_path(OUTPUT_DIR, ds)
        if not os.path.exists(path):
            rows.append({"variant": ds, "period": "(file missing)",
                         "messages": np.nan, "users": np.nan})
            continue
        df = _load_dated_csv(path, usecols=[POSTER_COL, DATE_COL])
        df["period"] = assign_period(df[DATE_COL], cutoff, end)
        for p in PERIODS:
            sub = df[df["period"] == p]
            rows.append({"variant": ds, "period": p,
                         "messages": len(sub),
                         "users": sub[POSTER_COL].nunique()})
    tab = pd.DataFrame(rows)

    print("\n── Diagnostic: dataset_variant × pandemic_period ──")
    print(tab.to_string(index=False))
    empty = tab[(tab["messages"].notna()) & (tab["messages"] < MIN_USERS_PER_GROUP)]
    if len(empty):
        print("\n  ⚠  Empty or near-empty cells (period effects are confounded "
              "with dataset variant — interpret with care):")
        for _, r in empty.iterrows():
            print(f"     {r['variant']} × {r['period']}: "
                  f"{int(r['messages'])} messages")
    return tab


def multi_period_membership(user_df: pd.DataFrame) -> pd.DataFrame:
    """How many users appear in 1, 2, and 3 periods (independence caveat)."""
    counts = user_df.groupby(POSTER_COL, observed=True)["period"].nunique()
    dist = counts.value_counts().sort_index()
    total = len(counts)
    rows = [{"periods_appeared_in": int(k),
             "n_users": int(v),
             "pct_users": round(v / total * 100, 1)}
            for k, v in dist.items()]
    tab = pd.DataFrame(rows)
    print("\n── Users per number of periods (independence caveat) ──")
    print(tab.to_string(index=False))
    return tab


# =============================================================================
# Statistics
# =============================================================================

def _rank_biserial(u_stat: float, n1: int, n2: int) -> float:
    return 1.0 - (2.0 * u_stat) / (n1 * n2)


def _epsilon_squared(h_stat: float, n: int) -> float:
    return h_stat * (n + 1) / (n**2 - 1) if n > 1 else np.nan


def run_stats(user_df: pd.DataFrame, features: list[str], analysis: str) -> pd.DataFrame:
    """
    Per-user aggregation first (already done), then:
      1. Kruskal-Wallis omnibus across periods, per feature.
      2. BH on omnibus p-values; survivors get pairwise Mann-Whitney U.
      3. Rank-biserial for pairwise, epsilon-squared for omnibus.
      4. Final BH across the pooled set (omnibus + post-hoc); reported as p_bh.
    """
    rows = []
    omnibus_p: dict[str, float] = {}

    for feat in tqdm(features, desc=f"Kruskal-Wallis ({analysis})", unit="feat"):
        groups = {p: user_df.loc[user_df["period"] == p, feat].dropna()
                  for p in PERIODS}
        tested = {p: g for p, g in groups.items() if len(g) >= MIN_USERS_PER_GROUP}
        base = {
            "analysis": analysis, "feature": feat,
            "n_pre": len(groups["pre"]), "n_during": len(groups["during"]),
            "n_post": len(groups["post"]),
        }
        if len(tested) < 2:
            rows.append({**base, "comparison": "omnibus", "statistic": np.nan,
                         "p_raw": np.nan, "effect_size": np.nan,
                         "note": f"skipped: <2 periods with ≥{MIN_USERS_PER_GROUP} users"})
            continue
        h, p = scipy_stats.kruskal(*tested.values())
        n_total = sum(len(g) for g in tested.values())
        rows.append({**base, "comparison": "omnibus",
                     "statistic": round(float(h), 4),
                     "p_raw": float(p),
                     "effect_size": round(_epsilon_squared(h, n_total), 4),
                     "note": ""})
        omnibus_p[feat] = float(p)

    # BH on the omnibus family to select features for post-hoc testing
    survivors: set[str] = set()
    if omnibus_p:
        feats, pvals = zip(*omnibus_p.items())
        rej, _, _, _ = multipletests(pvals, method="fdr_bh")
        survivors = {f for f, r in zip(feats, rej) if r}

    for feat in sorted(survivors):
        for a, b in PAIRS:
            ga = user_df.loc[user_df["period"] == a, feat].dropna()
            gb = user_df.loc[user_df["period"] == b, feat].dropna()
            base = {
                "analysis": analysis, "feature": feat,
                "n_pre": len(ga) if a == "pre" else (len(gb) if b == "pre" else np.nan),
                "n_during": len(ga) if a == "during" else (len(gb) if b == "during" else np.nan),
                "n_post": len(gb) if b == "post" else (len(ga) if a == "post" else np.nan),
            }
            if len(ga) < MIN_USERS_PER_GROUP or len(gb) < MIN_USERS_PER_GROUP:
                rows.append({**base, "comparison": f"{a}_vs_{b}",
                             "statistic": np.nan, "p_raw": np.nan,
                             "effect_size": np.nan,
                             "note": f"skipped: group <{MIN_USERS_PER_GROUP} users"})
                continue
            u, p = scipy_stats.mannwhitneyu(ga, gb, alternative="two-sided")
            rows.append({**base, "comparison": f"{a}_vs_{b}",
                         "statistic": round(float(u), 1),
                         "p_raw": float(p),
                         "effect_size": round(_rank_biserial(u, len(ga), len(gb)), 4),
                         "note": ""})

    results = pd.DataFrame(rows)

    # Final BH across the pooled omnibus + post-hoc p-values
    valid = results["p_raw"].notna()
    results["p_bh"] = np.nan
    if valid.any():
        _, corrected, _, _ = multipletests(results.loc[valid, "p_raw"], method="fdr_bh")
        results.loc[valid, "p_bh"] = corrected
    results["significant"] = results["p_bh"] < 0.05
    results["p_raw"] = results["p_raw"].round(6)
    results["p_bh"]  = results["p_bh"].round(6)
    return results


# =============================================================================
# Figures (house style)
# =============================================================================

def _cover_page(subtitle: str, provisional: bool) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(PRIMARY)
    ax.set_facecolor(PRIMARY)
    ax.axis("off")
    ax.text(0.5, 0.68, "Pandemic-Period Comparison", transform=ax.transAxes,
            ha="center", fontsize=20, fontweight="bold", color="white")
    ax.text(0.5, 0.52, "Psycholinguistic markers pre / during / post pandemic\n"
            "after Yahya & Abdul Rahim (2023), with per-user rank-based statistics",
            transform=ax.transAxes, ha="center", fontsize=11, color="#DDDDDD")
    ax.text(0.5, 0.32, subtitle, transform=ax.transAxes,
            ha="center", fontsize=11, color="#AAAAAA")
    if provisional:
        ax.text(0.5, 0.14, "⚠ PROVISIONAL — end date overridden on the command line",
                transform=ax.transAxes, ha="center", fontsize=11,
                fontweight="bold", color=ACCENT)
    return fig


def _section_divider(title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 1.5))
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes, ha="center", va="center",
            fontsize=14, fontweight="bold", color=PRIMARY)
    return fig


def _table_page(df: pd.DataFrame, title: str,
                highlight_rows: list[int] | None = None) -> plt.Figure:
    cells = [[("" if pd.isna(v) else str(v)) for v in row]
             for row in df.itertuples(index=False)]
    fig, ax = plt.subplots(figsize=(14, max(3, len(cells) * 0.36 + 1.2)))
    ax.axis("off")
    tbl = ax.table(cellText=cells, colLabels=list(df.columns),
                   cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)
    tbl.scale(1, 1.4)
    for j in range(len(df.columns)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cells) + 1):
        if highlight_rows and (i - 1) in highlight_rows:
            for j in range(len(df.columns)):
                tbl[(i, j)].set_facecolor("#FFF3CD")
        elif i % 2 == 0:
            for j in range(len(df.columns)):
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title(title, fontsize=11, fontweight="bold", color=PRIMARY, pad=10)
    fig.tight_layout()
    return fig


def fig_period_definition(cutoff: str, end: str, provisional: bool) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.axis("off")
    body = (
        f"pre     :  PostDate  <  {cutoff}\n"
        f"during  :  {cutoff}  ≤  PostDate  <  {end}\n"
        f"post    :  PostDate  ≥  {end}\n\n"
        f"Cutoff  = WHO pandemic declaration ({cutoff}).\n"
        f"End     = end of most Dutch COVID restrictions ({end})."
        + ("\n\n⚠ End date overridden via --end-date; results are PROVISIONAL."
           if provisional else "")
    )
    ax.text(0.5, 0.5, body, transform=ax.transAxes, ha="center", va="center",
            fontsize=11, family="monospace", color="#333333", linespacing=1.8,
            bbox=dict(boxstyle="round,pad=0.9", facecolor=SECONDARY))
    ax.set_title("Period Definitions", fontsize=13, fontweight="bold",
                 color=PRIMARY, pad=12)
    fig.tight_layout()
    return fig


def fig_boxplots(user_df: pd.DataFrame, features: list[str],
                 results: pd.DataFrame) -> plt.Figure | None:
    feats = features[:_MAX_BOXPLOTS]
    n = len(feats)
    if n == 0:
        return None
    ncols = 3
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, nrows * 3.6))
    axes_flat = np.atleast_1d(axes).flatten()

    for i, feat in enumerate(feats):
        ax = axes_flat[i]
        data = [user_df.loc[user_df["period"] == p, feat].dropna() for p in PERIODS]
        bp = ax.boxplot(data, labels=PERIODS, patch_artist=True,
                        showfliers=False, widths=0.55)
        for patch, p in zip(bp["boxes"], PERIODS):
            patch.set_facecolor(PERIOD_COLORS[p])
            patch.set_alpha(0.7)
        for median in bp["medians"]:
            median.set_color("#333333")
        row = results[(results["feature"] == feat)
                      & (results["comparison"] == "omnibus")]
        p_bh = row["p_bh"].iloc[0] if len(row) else np.nan
        ax.set_title(f"{feat}\nomnibus p_bh = "
                     f"{p_bh:.4f}" if not pd.isna(p_bh) else feat,
                     fontsize=8, fontweight="bold", color=PRIMARY)
        ax.tick_params(labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_ylabel("per-user mean rate (%)", fontsize=7)

    for j in range(n, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle("Per-User Rates by Period — Significant Features (BH-corrected)",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


# =============================================================================
# PDF builder
# =============================================================================

def build_pdf(
    crosstab: pd.DataFrame,
    membership: pd.DataFrame,
    availability: pd.DataFrame,
    results_all: pd.DataFrame,
    results_single: pd.DataFrame,
    user_df: pd.DataFrame,
    pdf_path: str,
    subtitle: str,
    cutoff: str,
    end: str,
    provisional: bool,
    pdf=None,
    include_cover: bool = True,
) -> None:
    def _write(writer):
        def save(fig):
            if fig is not None:
                writer.savefig(fig, bbox_inches="tight")
                plt.close("all")

        if include_cover:
            save(_cover_page(subtitle, provisional))
        else:
            save(_section_divider("Pandemic-Period Comparison"))

        save(_section_divider("Section 1 — Diagnostics"))
        save(fig_period_definition(cutoff, end, provisional))
        near_empty = [i for i, r in crosstab.reset_index(drop=True).iterrows()
                      if pd.notna(r["messages"]) and r["messages"] < MIN_USERS_PER_GROUP]
        save(_table_page(
            crosstab,
            "Dataset Variant × Pandemic Period — messages and unique users\n"
            "⚠ Period effects are confounded with dataset variant "
            "(old export ≈ pre+during, new export ≈ post); yellow = near-empty cell",
            highlight_rows=near_empty,
        ))
        save(_table_page(
            membership,
            "Users by Number of Periods Appeared In\n"
            "Users in >1 period violate between-group independence — see the "
            "single-period sensitivity analysis in Section 4",
        ))

        save(_section_divider("Section 2 — Dictionary Introspection"))
        save(_table_page(
            availability.drop(columns=["available"]),
            "Yahya & Abdul Rahim (2023) §2.3 Categories vs Available Dictionaries\n"
            "Only available categories are analyzed; '—' = genuinely absent "
            "from that source (omission, not a bug)",
            highlight_rows=[i for i, r in availability.reset_index(drop=True).iterrows()
                            if not r["available"]],
        ))

        save(_section_divider("Section 3 — Main Results (all users)"))
        omni = results_all[results_all["comparison"] == "omnibus"].copy()
        omni = omni.sort_values("p_bh", na_position="last")
        sig_idx = [i for i, r in omni.reset_index(drop=True).iterrows()
                   if r["significant"]]
        save(_table_page(
            omni.drop(columns=["analysis"]),
            "Kruskal-Wallis Omnibus per Feature (per-user rates)\n"
            "effect_size = epsilon-squared; p_bh = BH over pooled "
            "omnibus + post-hoc family; yellow = significant",
            highlight_rows=sig_idx,
        ))
        posthoc = results_all[results_all["comparison"] != "omnibus"].copy()
        if len(posthoc):
            posthoc = posthoc.sort_values(["feature", "comparison"])
            sig_idx = [i for i, r in posthoc.reset_index(drop=True).iterrows()
                       if r["significant"]]
            save(_table_page(
                posthoc.drop(columns=["analysis"]),
                "Post-hoc Pairwise Mann-Whitney U (only BH-surviving omnibus features)\n"
                "effect_size = rank-biserial correlation; yellow = significant",
                highlight_rows=sig_idx,
            ))
        else:
            save(_section_divider("No features survived omnibus BH correction — "
                                  "no post-hoc tests run"))

        sig_feats = list(
            omni[omni["significant"]].sort_values("p_bh")["feature"]
        )
        save(fig_boxplots(user_df, sig_feats, results_all))

        save(_section_divider("Section 4 — Sensitivity: Single-Period Users Only"))
        omni_s = results_single[results_single["comparison"] == "omnibus"].copy()
        omni_s = omni_s.sort_values("p_bh", na_position="last")
        sig_idx = [i for i, r in omni_s.reset_index(drop=True).iterrows()
                   if r["significant"]]
        save(_table_page(
            omni_s.drop(columns=["analysis"]),
            "Same Design, Restricted to Users Appearing in Exactly One Period\n"
            "Removes the between-group dependence of multi-period users; compare "
            "significance patterns with Section 3",
            highlight_rows=sig_idx,
        ))

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

def run_variant(dataset: str, cutoff: str, end: str, provisional: bool) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pdf_path = variant_path(OUTPUT_DIR, "pandemic_period_report.pdf", dataset)
    csv_path = variant_path(OUTPUT_DIR, "pandemic_period_stats.csv", dataset)

    print(f"\n{'='*70}\nPandemic-period analysis — dataset: {dataset}\n{'='*70}")

    liwc2015_df, map2015 = load_liwc2015_features(dataset)
    liwc22_df,   map22   = load_liwc22_features(dataset)

    if liwc2015_df is None and liwc22_df is None:
        raise RuntimeError(
            "Neither LIWC score file is available:\n"
            f"  {variant_path(OUTPUT_DIR, 'liwc_scores.csv', dataset)}  "
            "(run: make liwc DATASET=" + dataset + ")\n"
            f"  {variant_path(OUTPUT_DIR, 'liwc22_scores.csv', dataset)}  "
            "(run: make liwc22 DATASET=" + dataset + ")\n"
            "At least one is required — refusing to continue without any "
            "psycholinguistic features (no silent fallback)."
        )

    availability = introspect_dictionaries(map2015, map22)

    frames = []
    if liwc2015_df is not None:
        frames.append(per_user_period_rates(liwc2015_df, map2015, "liwc2015",
                                            cutoff, end))
    if liwc22_df is not None:
        frames.append(per_user_period_rates(liwc22_df, map22, "liwc22",
                                            cutoff, end))
    if liwc2015_df is None:
        absol = compute_absolutist_fallback(dataset)
        if absol is not None:
            frames.append(per_user_period_rates(
                absol, {"absolutist_rate": "absolutist"}, "wordlist",
                cutoff, end))

    user_df = merge_sources(frames)
    features = [c for c in user_df.columns if c not in (POSTER_COL, "period")]
    print(f"\n  {user_df[POSTER_COL].nunique()} users × periods, "
          f"{len(features)} features from "
          f"{len(frames)} source(s).")

    crosstab   = crosstab_variant_period(cutoff, end)
    membership = multi_period_membership(user_df)

    results_all = run_stats(user_df, features, analysis="all_users")

    single_ids = (
        user_df.groupby(POSTER_COL, observed=True)["period"].nunique()
        .pipe(lambda s: s[s == 1].index)
    )
    single_df = user_df[user_df[POSTER_COL].isin(single_ids)]
    print(f"\n  Sensitivity subset: {len(single_ids)} single-period users "
          f"(of {user_df[POSTER_COL].nunique()}).")
    results_single = run_stats(single_df, features, analysis="single_period")

    results = pd.concat([results_all, results_single], ignore_index=True)
    results.to_csv(csv_path, index=False)
    print(f"\n  Saved statistics table → {csv_path}")

    n_sig = int(results_all[(results_all["comparison"] == "omnibus")
                            & results_all["significant"]].shape[0])
    print(f"  Significant omnibus features after BH (all users): {n_sig}")

    build_pdf(
        crosstab, membership, availability,
        results_all, results_single, user_df,
        pdf_path=pdf_path,
        subtitle=f"Dataset: {subtitle_for(dataset)}",
        cutoff=cutoff, end=end, provisional=provisional,
    )

    print(f"\n✓ Done ({dataset}).\n  {pdf_path}\n  {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pandemic-period psycholinguistic comparison "
                    "(Yahya & Abdul Rahim 2023, per-user rank-based statistics)."
    )
    add_dataset_arg(parser)
    parser.add_argument("--all", dest="all_variants", action="store_true",
                        help="Run all three dataset variants in one go.")
    parser.add_argument("--end-date", default=None,
                        help="Override PANDEMIC_END_DATE (YYYY-MM-DD) for "
                             "experimentation; report is marked PROVISIONAL.")
    args = parser.parse_args()

    end = args.end_date or PANDEMIC_END_DATE
    if end is None:
        print(
            "\n✗ REFUSING TO RUN: PANDEMIC_END_DATE is not set in config.py and "
            "no --end-date override was given.\n"
            "  The during/post boundary is a research decision (Claudia's call) "
            "— set it in config.py, or pass --end-date YYYY-MM-DD for a "
            "provisional experiment.\n",
            file=sys.stderr,
        )
        sys.exit(1)
    provisional = args.end_date is not None and args.end_date != PANDEMIC_END_DATE

    datasets = DATASET_CHOICES if args.all_variants else [args.dataset]
    for ds in datasets:
        run_variant(ds, PANDEMIC_CUTOFF_DATE, end, provisional)


if __name__ == "__main__":
    main()
