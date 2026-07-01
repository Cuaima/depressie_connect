# =============================================================================
# exploration.py  –  descriptive statistics for the community forum data
#
# All functions return a DataFrame or dict that can be:
#   - printed to terminal via print_all_statistics()
#   - passed directly to Streamlit widgets in app.py
#
# Assumes input is messages_community.csv (output of preprocess.py)
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
import emoji as emoji_lib

from utils.thread_utils import label_roles
from dataset_io import add_dataset_arg, structured_path, variant_path

warnings.filterwarnings("ignore")

# ── Constants ─────────────────────────────────────────────────────────────────

POSTER_COL  = "PosterID"
TEXT_COL    = "MessageText"
DATE_COL    = "PostDate"
TOPIC_COL   = "ForumTopicID"


# ── Loader ────────────────────────────────────────────────────────────────────

def load_messages(path: str = "output/messages_structured.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL])
    return df


# ── Helpers ───────────────────────────────────────────────────────────────────

def _central_tendency(series: pd.Series) -> dict:
    """Returns mean, median and mode for a numeric series."""
    mode_vals = series.mode()
    return {
        "mean":   round(series.mean(), 2),
        "median": round(series.median(), 2),
        "mode":   mode_vals.iloc[0] if not mode_vals.empty else None,
    }




# =============================================================================
# 1. Posts per user
# =============================================================================

def posts_per_user(df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df.groupby(POSTER_COL)
        .size()
        .reset_index(name="post_count")
        .sort_values("post_count", ascending=False)
    )
    return counts


def posts_per_user_stats(df: pd.DataFrame) -> dict:
    counts = posts_per_user(df)["post_count"]
    return _central_tendency(counts)


# =============================================================================
# 2. Posts per thread
#    - counts all messages per thread (topic post + replies)
#    - separately counts true replies (excludes the opening post)
# =============================================================================

def posts_per_thread(df: pd.DataFrame) -> pd.DataFrame:
    df = label_roles(df)

    total = (
        df.groupby(TOPIC_COL)
        .size()
        .reset_index(name="total_messages")
    )

    replies = (
        df[df["role"] == "reply"]
        .groupby(TOPIC_COL)
        .size()
        .reset_index(name="reply_count")
    )

    return total.merge(replies, on=TOPIC_COL, how="left").fillna(0)


def posts_per_thread_stats(df: pd.DataFrame) -> dict:
    counts = posts_per_thread(df)
    return {
        "total_messages": _central_tendency(counts["total_messages"]),
        "replies_only":   _central_tendency(counts["reply_count"]),
    }


# =============================================================================
# 3. User activity span
#    - active_days_span : days between first and last post
#    - active_days_count: number of distinct days with at least one post
# =============================================================================

def user_activity_span(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = df[DATE_COL].dt.date

    span = (
        df.groupby(POSTER_COL)[DATE_COL]
        .agg(first_post="min", last_post="max")
        .reset_index()
    )
    span["active_days_span"] = (
        span["last_post"] - span["first_post"]
    ).dt.days

    day_counts = (
        df.groupby(POSTER_COL)["date"]
        .nunique()
        .reset_index(name="active_days_count")
    )

    result = span.merge(day_counts, on=POSTER_COL)
    return result.sort_values("active_days_span", ascending=False)


def user_activity_span_stats(df: pd.DataFrame) -> dict:
    span = user_activity_span(df)
    return {
        "active_days_span":  _central_tendency(span["active_days_span"]),
        "active_days_count": _central_tendency(span["active_days_count"]),
    }


# =============================================================================
# 4. Posts relative to time active (posting rate)
#    posts_per_active_day = total posts / active_days_span (floored to 1)
# =============================================================================

def user_posting_rate(df: pd.DataFrame) -> pd.DataFrame:
    counts  = posts_per_user(df)
    spans   = user_activity_span(df)[[POSTER_COL, "active_days_span", "active_days_count"]]
    merged  = counts.merge(spans, on=POSTER_COL)

    # Avoid division by zero for users with only one day of activity
    merged["posts_per_span_day"]   = (
        merged["post_count"] / merged["active_days_span"].clip(lower=1)
    ).round(3)
    merged["posts_per_active_day"] = (
        merged["post_count"] / merged["active_days_count"].clip(lower=1)
    ).round(3)

    return merged.sort_values("posts_per_active_day", ascending=False)


# =============================================================================
# 5. Activity over time
#    - posts per calendar day (for time series chart)
# =============================================================================

def activity_per_day(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = df[DATE_COL].dt.date
    return (
        df.groupby("date")
        .size()
        .reset_index(name="post_count")
        .sort_values("date")
    )


# =============================================================================
# 6. Popular times: hour of day, day of week, month
# =============================================================================

def posts_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[DATE_COL].dt.hour
        .value_counts()
        .sort_index()
        .rename_axis("hour")
        .reset_index(name="post_count")
    )


def posts_by_day_of_week(df: pd.DataFrame) -> pd.DataFrame:
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    counts = (
        df[DATE_COL].dt.day_name()
        .value_counts()
        .reindex(day_order)
        .rename_axis("day_of_week")
        .reset_index(name="post_count")
    )
    return counts


def posts_by_month(df: pd.DataFrame) -> pd.DataFrame:
    month_order = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    counts = (
        df[DATE_COL].dt.month_name()
        .value_counts()
        .reindex(month_order)
        .rename_axis("month")
        .reset_index(name="post_count")
    )
    return counts


# =============================================================================
# 7. Top 10 most active users
# =============================================================================

# AFTER
def top_users(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    counts  = posts_per_user(df).head(n)
    spans   = user_activity_span(df)[[POSTER_COL, "active_days_span", "active_days_count"]]
    rates   = user_posting_rate(df)[[POSTER_COL, "posts_per_active_day"]]

    # Word counts for top users
    top_ids = set(counts[POSTER_COL])
    subset  = df[df[POSTER_COL].isin(top_ids)].copy()
    subset["ik_count"]   = subset[TEXT_COL].apply(lambda t: _count_word(t, "ik"))
    subset["mijn_count"] = subset[TEXT_COL].apply(lambda t: _count_word(t, "mijn"))
    subset["word_count"] = subset[TEXT_COL].fillna("").apply(lambda t: len(str(t).split()))

    word_counts = (
        subset.groupby(POSTER_COL)
        .agg(
            ik_count   =("ik_count",   "sum"),
            mijn_count =("mijn_count", "sum"),
            word_count =("word_count", "sum"),
        )
        .reset_index()
    )
    word_counts["ik_pct"]   = (word_counts["ik_count"]   / word_counts["word_count"].clip(lower=1) * 100).round(2)
    word_counts["mijn_pct"] = (word_counts["mijn_count"] / word_counts["word_count"].clip(lower=1) * 100).round(2)

    return (
        counts
        .merge(spans,       on=POSTER_COL, how="left")
        .merge(rates,       on=POSTER_COL, how="left")
        .merge(word_counts, on=POSTER_COL, how="left")
    )


# =============================================================================
# 8. "ik" usage
#    - total count across all messages
#    - count per user
#    - count per user as % of their total word count
# =============================================================================

def _count_word(text: str, word: str = "ik") -> int:
    """Case-insensitive whole-word count of a given word in a string."""
    return len(re.findall(rf"\b{word}\b", str(text), flags=re.IGNORECASE))


def _tokenize_nl(text: str, min_len: int = 3) -> list[str]:
    """Lowercase alphabetic tokens with Dutch stopwords and short words removed."""
    return [
        w for w in re.findall(r"[a-zA-ZÀ-ÿ]+", str(text).lower())
        if w not in STOPWORDS and len(w) >= min_len
    ]


def ik_statistics(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["ik_count"]    = df[TEXT_COL].apply(lambda t: _count_word(t, "ik"))
    df["mijn_count"]  = df[TEXT_COL].apply(lambda t: _count_word(t, "mijn"))
    df["word_count"]  = df[TEXT_COL].fillna("").apply(lambda t: len(str(t).split()))

    # Total across all messages
    total_ik    = int(df["ik_count"].sum())
    total_words = int(df["word_count"].sum())
    total_mijn  = int(df["mijn_count"].sum())

    # Per user
    per_user = (
        df.groupby(POSTER_COL)
        .agg(
            ik_count   =("ik_count",   "sum"),
            mijn_count =("mijn_count", "sum"),
            word_count =("word_count", "sum"),
        )
        .reset_index()
    )
    per_user["ik_pct"] = (
        per_user["ik_count"] / per_user["word_count"].clip(lower=1) * 100
    ).round(3)
    per_user["mijn_pct"] = (
        per_user["mijn_count"] / per_user["word_count"].clip(lower=1) * 100
    ).round(3)
    per_user = per_user.sort_values("ik_count", ascending=False)

    return {
        "total_ik_count":     total_ik,
        "total_mijn_count":   total_mijn,
        "total_word_count":   total_words,
        "overall_ik_pct":     round(total_ik   / max(total_words, 1) * 100, 3),
        "overall_mijn_pct":   round(total_mijn / max(total_words, 1) * 100, 3),
        "per_user":           per_user,
        "per_user_stats":     _central_tendency(per_user["ik_count"]),
        "per_user_pct_stats": _central_tendency(per_user["ik_pct"]),
        "per_user_mijn_stats":     _central_tendency(per_user["mijn_count"]),
        "per_user_mijn_pct_stats": _central_tendency(per_user["mijn_pct"]),
    }


# =============================================================================
# 9. Word frequencies by role (post vs reply)
# =============================================================================

def word_frequencies_by_role(df: pd.DataFrame, top_n: int = 50) -> dict[str, pd.DataFrame]:
    """Top-N content words for initial posts and replies."""
    df = label_roles(df)
    result = {}
    for role in ("post", "reply"):
        tokens: list[str] = []
        for text in df.loc[df["role"] == role, TEXT_COL].fillna(""):
            tokens.extend(_tokenize_nl(text))
        total = max(len(tokens), 1)
        freq_df = pd.DataFrame(Counter(tokens).most_common(top_n), columns=["word", "count"])
        freq_df["pct"] = (freq_df["count"] / total * 100).round(3)
        result[role] = freq_df
    return result


def word_frequency_ratio(df: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
    """Words ranked by log2 divergence: positive = post-heavy, negative = reply-heavy."""
    df = label_roles(df)
    role_counts: dict[str, Counter] = {}
    role_totals: dict[str, int] = {}
    for role in ("post", "reply"):
        tokens: list[str] = []
        for text in df.loc[df["role"] == role, TEXT_COL].fillna(""):
            tokens.extend(_tokenize_nl(text))
        role_counts[role] = Counter(tokens)
        role_totals[role] = max(len(tokens), 1)

    min_count = 5
    rows = []
    for w in set(role_counts["post"]) | set(role_counts["reply"]):
        c_post  = role_counts["post"].get(w, 0)
        c_reply = role_counts["reply"].get(w, 0)
        if c_post + c_reply < min_count:
            continue
        p = c_post  / role_totals["post"]  * 100
        r = c_reply / role_totals["reply"] * 100
        rows.append({
            "word":       w,
            "post_pct":   round(p, 4),
            "reply_pct":  round(r, 4),
            "log2_ratio": round(np.log2((p + 0.001) / (r + 0.001)), 4),
        })

    ratio_df = pd.DataFrame(rows).sort_values("log2_ratio", ascending=False).reset_index(drop=True)
    half = top_n // 2
    return (
        pd.concat([ratio_df.head(half), ratio_df.tail(half)])
        .drop_duplicates("word")
        .sort_values("log2_ratio", ascending=False)
        .reset_index(drop=True)
    )


# =============================================================================
# 10. Word count per message by role
# =============================================================================

def word_count_by_role(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Word count distribution split by initial posts and replies."""
    df = label_roles(df)
    df = df.copy()
    df["word_count"] = df[TEXT_COL].fillna("").apply(lambda t: len(str(t).split()))
    return {
        role: df.loc[df["role"] == role, "word_count"].reset_index(drop=True)
        for role in ("post", "reply")
    }


# =============================================================================
# 11. Emoji use by role
# =============================================================================

def emoji_use_by_role(df: pd.DataFrame) -> pd.DataFrame:
    """Emoji presence and count per message, split by role."""
    df = label_roles(df)
    df = df.copy()
    df["emoji_count"] = df[TEXT_COL].fillna("").apply(emoji_lib.emoji_count)
    df["has_emoji"] = df["emoji_count"] > 0
    rows = []
    for role in ("post", "reply"):
        sub = df[df["role"] == role]
        rows.append({
            "role":             role,
            "n_messages":       len(sub),
            "n_with_emoji":     int(sub["has_emoji"].sum()),
            "pct_with_emoji":   round(float(sub["has_emoji"].mean()) * 100, 2),
            "mean_emoji_count": round(float(sub["emoji_count"].mean()), 3),
            "total_emoji":      int(sub["emoji_count"].sum()),
        })
    return pd.DataFrame(rows)


def top_emojis_by_role(df: pd.DataFrame, top_n: int = 20) -> dict[str, pd.DataFrame]:
    """Top-N most frequent individual emojis for initial posts and replies."""
    df = label_roles(df)
    result = {}
    for role in ("post", "reply"):
        counts: Counter = Counter()
        for text in df.loc[df["role"] == role, TEXT_COL].fillna(""):
            for token in emoji_lib.analyze(str(text)):
                counts[token.chars] += 1
        total = max(sum(counts.values()), 1)
        rows = [
            {"emoji": ch, "count": cnt, "pct": round(cnt / total * 100, 2)}
            for ch, cnt in counts.most_common(top_n)
        ]
        result[role] = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["emoji", "count", "pct"])
    return result


# =============================================================================
# 12. Sentence structure by role
# =============================================================================

def sentence_structure_by_role(df: pd.DataFrame) -> dict:
    """Sentence-level structural metrics split by initial posts and replies."""
    df = label_roles(df)
    result = {}
    for role in ("post", "reply"):
        texts  = df.loc[df["role"] == role, TEXT_COL].fillna("").tolist()
        n_msgs = len(texts)
        n_sents_list, wps_list, cps_list = [], [], []
        n_q = n_ex = n_ell = 0
        for text in texts:
            text = str(text)
            if "?" in text:
                n_q += 1
            if "!" in text:
                n_ex += 1
            if "..." in text:
                n_ell += 1
            sents = [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s.strip()]
            ns    = max(len(sents), 1)
            n_sents_list.append(ns)
            wps_list.append(len(text.split()) / ns)
            cps_list.append(len(text) / ns)
        result[role] = {
            "n_messages":             n_msgs,
            "avg_sentences":          round(float(np.mean(n_sents_list)), 2) if n_sents_list else 0,
            "avg_words_per_sentence": round(float(np.mean(wps_list)),     2) if wps_list     else 0,
            "avg_chars_per_sentence": round(float(np.mean(cps_list)),     2) if cps_list     else 0,
            "pct_with_question":      round(n_q   / max(n_msgs, 1) * 100, 2),
            "pct_with_exclaim":       round(n_ex  / max(n_msgs, 1) * 100, 2),
            "pct_with_ellipsis":      round(n_ell / max(n_msgs, 1) * 100, 2),
        }
    return result


# =============================================================================
# Terminal summary
# =============================================================================

def print_all_statistics(df: pd.DataFrame):
    sep = "\n" + "─" * 60

    print(sep)
    print("POSTS PER USER")
    stats = posts_per_user_stats(df)
    print(f"  Mean:   {stats['mean']}")
    print(f"  Median: {stats['median']}")
    print(f"  Mode:   {stats['mode']}")

    print(sep)
    print("POSTS PER THREAD")
    stats = posts_per_thread_stats(df)
    print("  Total messages (incl. opening post):")
    print(f"    Mean:   {stats['total_messages']['mean']}")
    print(f"    Median: {stats['total_messages']['median']}")
    print(f"    Mode:   {stats['total_messages']['mode']}")
    print("  Replies only:")
    print(f"    Mean:   {stats['replies_only']['mean']}")
    print(f"    Median: {stats['replies_only']['median']}")
    print(f"    Mode:   {stats['replies_only']['mode']}")

    print(sep)
    print("USER ACTIVITY SPAN")
    stats = user_activity_span_stats(df)
    print("  Days between first and last post:")
    print(f"    Mean:   {stats['active_days_span']['mean']}")
    print(f"    Median: {stats['active_days_span']['median']}")
    print(f"    Mode:   {stats['active_days_span']['mode']}")
    print("  Distinct days with at least one post:")
    print(f"    Mean:   {stats['active_days_count']['mean']}")
    print(f"    Median: {stats['active_days_count']['median']}")
    print(f"    Mode:   {stats['active_days_count']['mode']}")

    print(sep)
    print("TOP 10 MOST ACTIVE USERS")
    print(top_users(df).to_string(index=False))

    print(sep)
    print("POPULAR HOURS OF DAY")
    print(posts_by_hour(df).to_string(index=False))

    print(sep)
    print("POPULAR DAYS OF WEEK")
    print(posts_by_day_of_week(df).to_string(index=False))

    print(sep)
    print("POPULAR MONTHS")
    print(posts_by_month(df).to_string(index=False))

    print(sep)
    print("'IK' AND 'MIJN' USAGE")
    ik = ik_statistics(df)
    print(f"  Total 'ik' count:        {ik['total_ik_count']}  ({ik['overall_ik_pct']}% of all words)")
    print(f"  Total 'mijn' count:      {ik['total_mijn_count']}  ({ik['overall_mijn_pct']}% of all words)")
    print(f"  Total word count:        {ik['total_word_count']}")
    print("\n  Per-user 'ik'  (mean/median/mode):", ik['per_user_stats'])
    print("  Per-user 'ik'% (mean/median/mode):", ik['per_user_pct_stats'])
    print("\n  Per-user 'mijn'  (mean/median/mode):", ik['per_user_mijn_stats'])
    print("  Per-user 'mijn'% (mean/median/mode):", ik['per_user_mijn_pct_stats'])
    print("\n  Top 10 users by 'ik' count:")
    print(ik["per_user"].head(10).to_string(index=False))

    print(sep)
    print("TOP 10 MOST ACTIVE USERS — 'IK' AND 'MIJN' BREAKDOWN")
    print(top_users(df).to_string(index=False))


# =============================================================================
# PDF report (merged from eda_report.py)
# =============================================================================

_INPUT_PATH     = "output/messages_structured.csv"
_OUTPUT_DIR     = "output"
_PDF_ALL        = os.path.join(_OUTPUT_DIR, "eda_report_all_users.pdf")
_PDF_MULTI      = os.path.join(_OUTPUT_DIR, "eda_report_multi_posters.pdf")
_FILTERED_PATH  = os.path.join(_OUTPUT_DIR, "messages_multi_posters.csv")

PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"

STOPWORDS = {
    "de", "het", "een", "en", "van", "in", "is", "ik", "dat", "op",
    "te", "met", "voor", "zijn", "er", "maar", "ook", "als", "aan",
    "niet", "ze", "je", "me", "hij", "we", "bij", "zo", "dan", "nog",
    "wel", "om", "die", "wat", "mij", "dit", "al", "nu", "heb", "was",
    "kan", "meer", "heeft", "hem", "haar", "dit", "hun", "uit", "door"
}


def _central_tendency_full(series: pd.Series, label: str) -> dict:
    """Central tendency with label, min, and max — used for the PDF stats table."""
    mode_vals = series.mode()
    return {
        "label":  label,
        "mean":   round(series.mean(), 2),
        "median": round(series.median(), 2),
        "mode":   round(float(mode_vals.iloc[0]), 2) if not mode_vals.empty else None,
        "min":    round(series.min(), 2),
        "max":    round(series.max(), 2),
    }


def _style_ax_report(ax, title: str, xlabel: str, ylabel: str = "Frequency"):
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
    fig, ax = plt.subplots(figsize=(10, 4))
    cap = series.quantile(cap_pct)
    clipped = series.clip(upper=cap)
    ax.hist(clipped, bins=bins, color=PRIMARY, edgecolor="white", linewidth=0.5)
    _style_ax_report(ax, title, xlabel)
    stats = _central_tendency_full(series, title)
    _add_stats_text(ax, stats)
    if cap < series.max():
        ax.set_xlabel(
            f"{xlabel}  (top {int((1 - cap_pct) * 100)}% capped for readability)",
            fontsize=10, color="#333333"
        )
    fig.tight_layout()
    return fig


def _bar(labels, values, title: str, xlabel: str,
         ylabel: str = "Post count", rotate: bool = False) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    bars = ax.bar(labels, values, color=PRIMARY, edgecolor="white", linewidth=0.5)
    max_idx = int(np.argmax(values))
    bars[max_idx].set_color(ACCENT)
    _style_ax_report(ax, title, xlabel, ylabel)
    if rotate:
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    fig.tight_layout()
    return fig


def _cover_page_report(title: str, subtitle: str) -> plt.Figure:
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


def _stats_table_fig(rows: list) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, max(2, len(rows) * 0.55 + 1)))
    ax.axis("off")
    col_labels = ["Metric", "Mean", "Median", "Mode", "Min", "Max"]
    cell_data = [
        [r["label"], r["mean"], r["median"], r["mode"], r["min"], r["max"]]
        for r in rows
    ]
    tbl = ax.table(cellText=cell_data, colLabels=col_labels,
                   cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cell_data) + 1):
        for j in range(len(col_labels)):
            if i % 2 == 0:
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title("Summary Statistics", fontsize=13, fontweight="bold",
                 color=PRIMARY, pad=10)
    fig.tight_layout()
    return fig


def _section_page(title: str):
    fig, ax = plt.subplots(figsize=(10, 1.8))
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=14, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def _diverging_bar(ratio_df: pd.DataFrame, title: str):
    """Horizontal diverging bar chart for word log2 ratios (post vs reply)."""
    ordered = ratio_df.sort_values("log2_ratio")
    colors = [PRIMARY if v >= 0 else ACCENT for v in ordered["log2_ratio"]]
    fig, ax = plt.subplots(figsize=(10, max(6, len(ordered) * 0.42)))
    ax.barh(ordered["word"], ordered["log2_ratio"], color=colors, edgecolor="white", linewidth=0.4)
    ax.axvline(0, color="#444444", linewidth=0.8, linestyle="--")
    ax.set_xlabel("log₂(post frequency / reply frequency)", fontsize=10, color="#333333")
    ax.set_title(title, fontsize=13, fontweight="bold", color=PRIMARY, pad=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CCCCCC")
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.tick_params(colors="#555555", labelsize=8)
    ax.text(0.02, 0.01, "← more in replies", transform=ax.transAxes,
            ha="left", fontsize=8, color=ACCENT)
    ax.text(0.98, 0.01, "more in posts →", transform=ax.transAxes,
            ha="right", fontsize=8, color=PRIMARY)
    fig.tight_layout()
    return fig


def _role_histograms(post_s: pd.Series, reply_s: pd.Series,
                     title: str, xlabel: str, cap_pct: float = 0.99):
    """Side-by-side histograms comparing a metric for posts vs replies."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, series, label, color in [
        (axes[0], post_s,  "Initial Posts", PRIMARY),
        (axes[1], reply_s, "Replies",       ACCENT),
    ]:
        cap = series.quantile(cap_pct)
        ax.hist(series.clip(upper=cap), bins=30, color=color, edgecolor="white", linewidth=0.5)
        _style_ax_report(ax, label, xlabel)
        _add_stats_text(ax, _central_tendency_full(series, label))
    fig.suptitle(title, fontsize=13, fontweight="bold", color=PRIMARY, y=1.02)
    fig.tight_layout()
    return fig


def _emoji_label(ch: str) -> str:
    """Convert an emoji character to a readable name for PDF rendering.

    PDF fonts (DejaVu) don't cover emoji codepoints, so we replace each glyph
    with its CLDR short name, e.g. 😊 → 'smiling face'.
    """
    if not ch:
        return ch
    name = emoji_lib.demojize(ch, language="en")
    return name.strip(":").replace("_", " ")


def _emoji_freq_table(role_freqs: dict[str, pd.DataFrame], top_n: int = 20):
    """Side-by-side table of the most frequent emojis per role."""
    post_df  = role_freqs.get("post",  pd.DataFrame(columns=["emoji", "count", "pct"]))
    reply_df = role_freqs.get("reply", pd.DataFrame(columns=["emoji", "count", "pct"]))
    n = min(top_n, max(len(post_df), len(reply_df)))
    if n == 0:
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.axis("off")
        ax.text(0.5, 0.5, "No emoji found in corpus.", transform=ax.transAxes,
                ha="center", fontsize=11, color="#555555")
        return fig

    def _pad(frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.head(top_n).copy()
        while len(frame) < n:
            frame = pd.concat([frame, pd.DataFrame([{"emoji": "", "count": "", "pct": ""}])],
                              ignore_index=True)
        return frame

    p = _pad(post_df)
    r = _pad(reply_df)

    col_labels = ["#", "Posts emoji", "Count", "%", "Replies emoji", "Count", "%"]
    cell_data = [
        [
            i + 1,
            _emoji_label(p.iloc[i]["emoji"]),  str(p.iloc[i]["count"]),  str(p.iloc[i]["pct"]),
            _emoji_label(r.iloc[i]["emoji"]),  str(r.iloc[i]["count"]),  str(r.iloc[i]["pct"]),
        ]
        for i in range(n)
    ]

    fig, ax = plt.subplots(figsize=(11, max(3, n * 0.52 + 1.2)))
    ax.axis("off")
    tbl = ax.table(cellText=cell_data, colLabels=col_labels,
                   cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.55)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, n + 1):
        for j in range(len(col_labels)):
            if i % 2 == 0:
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title(f"Top {top_n} Most Frequent Emojis — Posts vs Replies",
                 fontsize=13, fontweight="bold", color=PRIMARY, pad=12)
    fig.tight_layout()
    return fig


def _emoji_bars(emoji_df: pd.DataFrame):
    """Two side-by-side bars: % messages with emoji, and mean emoji per message."""
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    labels = emoji_df["role"].tolist()
    colors = [PRIMARY, ACCENT]

    axes[0].bar(labels, emoji_df["pct_with_emoji"], color=colors, edgecolor="white")
    _style_ax_report(axes[0], "% Messages Containing Emoji", "Role", "% of messages")
    for i, val in enumerate(emoji_df["pct_with_emoji"]):
        axes[0].text(i, val + 0.05, f"{val:.1f}%", ha="center", fontsize=9)

    axes[1].bar(labels, emoji_df["mean_emoji_count"], color=colors, edgecolor="white")
    _style_ax_report(axes[1], "Mean Emoji Count per Message", "Role", "Mean count")
    for i, val in enumerate(emoji_df["mean_emoji_count"]):
        axes[1].text(i, val + 0.001, f"{val:.3f}", ha="center", fontsize=9)

    fig.suptitle("Emoji Use by Message Role", fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def _top_words_table(role_freqs: dict, top_n: int = 20) -> plt.Figure:
    """Side-by-side table of the most frequent content words per role."""
    post_df  = role_freqs.get("post",  pd.DataFrame(columns=["word", "count", "pct"]))
    reply_df = role_freqs.get("reply", pd.DataFrame(columns=["word", "count", "pct"]))
    n = min(top_n, max(len(post_df), len(reply_df)))
    if n == 0:
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.axis("off")
        ax.text(0.5, 0.5, "No words found.", transform=ax.transAxes,
                ha="center", fontsize=11, color="#555555")
        return fig

    def _pad(frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.head(top_n).copy()
        while len(frame) < n:
            frame = pd.concat(
                [frame, pd.DataFrame([{"word": "", "count": "", "pct": ""}])],
                ignore_index=True,
            )
        return frame

    p, r = _pad(post_df), _pad(reply_df)
    col_labels = ["#", "Post word", "Count", "%", "Reply word", "Count", "%"]
    cell_data  = [
        [i + 1,
         p.iloc[i]["word"], str(p.iloc[i]["count"]), str(p.iloc[i]["pct"]),
         r.iloc[i]["word"], str(r.iloc[i]["count"]), str(r.iloc[i]["pct"])]
        for i in range(n)
    ]
    fig, ax = plt.subplots(figsize=(11, max(3, n * 0.5 + 1.2)))
    ax.axis("off")
    tbl = ax.table(cellText=cell_data, colLabels=col_labels, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.55)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, n + 1):
        for j in range(len(col_labels)):
            if i % 2 == 0:
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title(f"Top {top_n} Most Frequent Words — Posts vs Replies",
                 fontsize=13, fontweight="bold", color=PRIMARY, pad=12)
    fig.tight_layout()
    return fig


def _sentence_structure_bars(struct: dict) -> plt.Figure:
    """Bar charts comparing sentence-level structural metrics for posts vs replies."""
    metrics = [
        ("avg_sentences",          "Avg Sentences per Message"),
        ("avg_words_per_sentence", "Avg Words per Sentence"),
        ("avg_chars_per_sentence", "Avg Chars per Sentence"),
        ("pct_with_question",      "% Messages with '?'"),
        ("pct_with_exclaim",       "% Messages with '!'"),
        ("pct_with_ellipsis",      "% Messages with '...'"),
    ]
    roles  = list(struct.keys())
    colors = [PRIMARY, ACCENT]
    cols   = 3
    rows   = (len(metrics) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(13, rows * 3.5))
    axes_flat = axes.flatten()
    for i, (key, label) in enumerate(metrics):
        ax   = axes_flat[i]
        vals = [struct[role][key] for role in roles]
        bs   = ax.bar(roles, vals, color=colors[: len(roles)], edgecolor="white")
        for b, v in zip(bs, vals):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() * 1.02,
                    f"{v:.1f}", ha="center", va="bottom", fontsize=9)
        _style_ax_report(ax, label, "Role", "Value")
    for j in range(len(metrics), len(axes_flat)):
        axes_flat[j].set_visible(False)
    fig.suptitle("Sentence Structure — Initial Posts vs Replies",
                 fontsize=13, fontweight="bold", color=PRIMARY)
    fig.tight_layout()
    return fig


def compute_stats(df: pd.DataFrame) -> dict:
    print("Computing statistics…")

    posts_per_user_s = df.groupby(POSTER_COL).size()
    thread_counts = df.groupby(TOPIC_COL).size()

    df_sorted = df.sort_values(DATE_COL)
    first_idx = df_sorted.groupby(TOPIC_COL)[DATE_COL].idxmin()
    df_sorted = df_sorted.copy()
    df_sorted["role"] = "reply"
    df_sorted.loc[first_idx, "role"] = "post"
    replies_per_thread = (
        df_sorted[df_sorted["role"] == "reply"].groupby(TOPIC_COL).size()
    )

    span = df.groupby(POSTER_COL)[DATE_COL].agg(first_post="min", last_post="max")
    span["span_days"] = (span["last_post"] - span["first_post"]).dt.days
    df_copy = df.copy()
    df_copy["date"] = df_copy[DATE_COL].dt.date
    active_days = df_copy.groupby(POSTER_COL)["date"].nunique()

    df_copy["word_count"] = df_copy[TEXT_COL].fillna("").apply(
        lambda t: len(str(t).split())
    )
    words_per_user = df_copy.groupby(POSTER_COL)["word_count"].sum()
    words_per_post = df_copy["word_count"]

    df_copy["ik_count"]   = df_copy[TEXT_COL].apply(lambda t: _count_word(t, "ik"))
    df_copy["mijn_count"] = df_copy[TEXT_COL].apply(lambda t: _count_word(t, "mijn"))
    ik_per_user   = df_copy.groupby(POSTER_COL)["ik_count"].sum()
    mijn_per_user = df_copy.groupby(POSTER_COL)["mijn_count"].sum()
    ik_pct_per_user   = (ik_per_user   / words_per_user.clip(lower=1) * 100).round(3)
    mijn_pct_per_user = (mijn_per_user / words_per_user.clip(lower=1) * 100).round(3)

    day_order   = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_order = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    hours  = df[DATE_COL].dt.hour.value_counts().sort_index()
    days   = df[DATE_COL].dt.day_name().value_counts().reindex(day_order)
    months = df[DATE_COL].dt.month_name().value_counts().reindex(month_order)

    return {
        "posts_per_user":      posts_per_user_s,
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
    }


def build_pdf(stats: dict, pdf_path: str | None = None, subtitle: str = "All Users",
              df: pd.DataFrame | None = None, pdf=None, include_cover: bool = True):
    summary_rows = [
        _central_tendency_full(stats["posts_per_user"],     "Posts per user"),
        _central_tendency_full(stats["thread_counts"],      "Messages per thread (total)"),
        _central_tendency_full(stats["replies_per_thread"], "Replies per thread"),
        _central_tendency_full(stats["span_days"],          "Activity span (days)"),
        _central_tendency_full(stats["active_days"],        "Active days per user"),
        _central_tendency_full(stats["words_per_post"],     "Words per post"),
        _central_tendency_full(stats["words_per_user"],     "Words per user (total)"),
        _central_tendency_full(stats["ik_per_user"],        "'ik' count per user"),
        _central_tendency_full(stats["ik_pct_per_user"],    "'ik' % per user"),
        _central_tendency_full(stats["mijn_per_user"],      "'mijn' count per user"),
        _central_tendency_full(stats["mijn_pct_per_user"],  "'mijn' % per user"),
    ]

    def _write(writer):
        def save(fig):
            writer.savefig(fig, bbox_inches="tight")
            plt.close("all")

        if include_cover:
            save(_cover_page_report("Depression Connect Forum",
                                    f"Exploratory Data Analysis Report — {subtitle}"))
        else:
            save(_section_page(f"Exploratory Data Analysis — {subtitle}"))

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
        save(_bar([f"{i:02d}:00" for i in h.index], h.values,
                  "Popular Hours of Day", "Hour", rotate=True))

        d = stats["days"]
        save(_bar(d.index.tolist(), d.values, "Popular Days of Week", "Day"))

        m = stats["months"]
        save(_bar(m.index.tolist(), m.values, "Popular Months", "Month", rotate=True))

        if df is not None:
            save(_section_page("Role-Based Analysis — Posts vs Replies"))

            ratio_df = word_frequency_ratio(df, top_n=30)
            if not ratio_df.empty:
                save(_diverging_bar(ratio_df, "Word Choice: Initial Posts vs Replies"))

            wc = word_count_by_role(df)
            save(_role_histograms(wc["post"], wc["reply"],
                                  "Words per Message: Initial Posts vs Replies",
                                  "Word count"))

            emoji_df = emoji_use_by_role(df)
            save(_emoji_bars(emoji_df))
            role_freqs = top_emojis_by_role(df, top_n=20)
            save(_emoji_freq_table(role_freqs, top_n=20))

            freq_data = word_frequencies_by_role(df, top_n=20)
            save(_top_words_table(freq_data, top_n=20))

            struct = sentence_structure_by_role(df)
            save(_sentence_structure_bars(struct))

    if pdf is not None:
        _write(pdf)
    else:
        if pdf_path is None:
            pdf_path = _PDF_ALL
        print(f"Building PDF → {pdf_path}")
        with pdf_backend.PdfPages(pdf_path) as writer:
            _write(writer)
        print(f"  PDF saved → {pdf_path}")


def save_filtered(df: pd.DataFrame):
    posts_per_user_s = df.groupby(POSTER_COL).size()
    single_posters = set(posts_per_user_s[posts_per_user_s == 1].index)
    filtered = df[~df[POSTER_COL].isin(single_posters)].copy()

    print("\nFiltered dataset:")
    print(f"  Single-post users removed: {len(single_posters)}")
    print(f"  Remaining users:  {filtered[POSTER_COL].nunique()}")
    print(f"  Remaining messages: {len(filtered)}")

    os.makedirs(_OUTPUT_DIR, exist_ok=True)
    filtered.to_csv(_FILTERED_PATH, index=False)
    print(f"  Saved → {_FILTERED_PATH}")
    return filtered


def generate_report(path: str | None = None, dataset: str | None = None):
    """Generate EDA PDF reports and filtered CSV from preprocessed community messages."""
    os.makedirs(_OUTPUT_DIR, exist_ok=True)
    ds        = dataset or "combined"
    if path is None:
        path = structured_path(_OUTPUT_DIR, ds)
    pdf_all   = variant_path(_OUTPUT_DIR, "eda_report_all_users.pdf",    ds)
    pdf_multi = variant_path(_OUTPUT_DIR, "eda_report_multi_posters.pdf", ds)
    filt_path = variant_path(_OUTPUT_DIR, "messages_multi_posters.csv",  ds)

    print("\n=== Report 1: All users ===")
    df = pd.read_csv(path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL])
    print(f"Loaded {len(df)} messages from {df[POSTER_COL].nunique()} posters.")

    stats = compute_stats(df)
    build_pdf(stats, pdf_path=pdf_all, subtitle="All Users", df=df)

    # save filtered
    posts_per_user_s = df.groupby(POSTER_COL).size()
    single_posters   = set(posts_per_user_s[posts_per_user_s == 1].index)
    filtered         = df[~df[POSTER_COL].isin(single_posters)].copy()
    filtered.to_csv(filt_path, index=False)
    print(f"\nFiltered: {len(single_posters)} single-post users removed → {filt_path}")

    print("\n=== Report 2: Multi-posters only ===")
    df_multi = filtered.copy()
    df_multi[DATE_COL] = pd.to_datetime(df_multi[DATE_COL], errors="coerce")
    df_multi = df_multi.dropna(subset=[DATE_COL])
    print(f"Loaded {len(df_multi)} messages from {df_multi[POSTER_COL].nunique()} posters.")
    stats_multi = compute_stats(df_multi)
    build_pdf(stats_multi, pdf_path=pdf_multi, subtitle="Multi-Posters Only", df=df_multi)

    print("\n✓ Done.")
    print(f"  {pdf_all}")
    print(f"  {pdf_multi}")
    print(f"  {filt_path}")


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="EDA report generator")
    add_dataset_arg(ap)
    ap.add_argument("--stats", action="store_true",
                    help="Print terminal statistics instead of building PDFs")
    args = ap.parse_args()
    if args.stats:
        df = load_messages(structured_path(_OUTPUT_DIR, args.dataset))
        print_all_statistics(df)
    else:
        generate_report(dataset=args.dataset)
