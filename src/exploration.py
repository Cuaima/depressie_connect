# =============================================================================
# exploration.py  –  descriptive statistics for the community forum data
#
# All functions return a DataFrame or dict that can be:
#   - printed to terminal via print_all_statistics()
#   - passed directly to Streamlit widgets in app.py
#
# Assumes input is messages_community.csv (output of preprocess.py)
# =============================================================================

import pandas as pd
import numpy as np
from collections import Counter
import re

# ── Constants ─────────────────────────────────────────────────────────────────

POSTER_COL  = "PosterID"
TEXT_COL    = "MessageText"
DATE_COL    = "PostDate"
TOPIC_COL   = "ForumTopicID"


# ── Loader ────────────────────────────────────────────────────────────────────

def load_messages(path: str = "output/messages_community.csv") -> pd.DataFrame:
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


def _label_thread_roles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'role' column:
      'topic'  – the first message in a thread (original poster)
      'reply'  – all subsequent messages, even from the same user
    """
    df = df.copy().sort_values(DATE_COL)
    first_idx = df.groupby(TOPIC_COL)[DATE_COL].idxmin()
    df["role"] = "reply"
    df.loc[first_idx, "role"] = "topic"
    return df


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
    df = _label_thread_roles(df)

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
# Entry point
# =============================================================================

if __name__ == "__main__":
    df = load_messages()
    print_all_statistics(df)
