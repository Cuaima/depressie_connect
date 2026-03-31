# =============================================================================
# exploration.py  –  cross-account participation analysis
#
# Two main analyses:
#   topic_account_interactions()  – which account types post in the same topic?
#   posters_across_account_types()– which posters appear in multiple accounts?
#
# Designed to run on CLEANED data (output of preprocess.run_pipeline()).
# If cleaned files are absent, falls back to raw data.
# =============================================================================

import os
import pandas as pd
from collections import defaultdict

from config import (
    DATA_DIR, OUTPUT_DIR,
    COMMUNITY_ACCOUNT_IDS,
    ID_COLUMN,
)

CHUNK_SIZE = 100_000


# ── Shared helper ─────────────────────────────────────────────────────────────

def _load_topic_account_map(use_cleaned: bool = True) -> dict:
    """Returns {ForumTopicID: AccountID}."""
    base = OUTPUT_DIR if use_cleaned else DATA_DIR

    topics_path = os.path.join(base, "topics_cleaned.csv" if use_cleaned else "topics.csv")
    groups_path = os.path.join(base, "groups_cleaned.csv" if use_cleaned else "groups.csv")

    # Fall back to raw if cleaned not found
    if not os.path.exists(topics_path):
        topics_path = os.path.join(DATA_DIR, "topics.csv")
    if not os.path.exists(groups_path):
        groups_path = os.path.join(DATA_DIR, "groups.csv")

    topics = pd.read_csv(topics_path, usecols=["ForumTopicID", "ForumGroupID"])
    groups = pd.read_csv(groups_path, usecols=["ForumGroupID", "AccountID"])

    return (
        topics
        .merge(groups, on="ForumGroupID", how="left")
        .dropna(subset=["AccountID"])
        .drop_duplicates("ForumTopicID")
        .set_index("ForumTopicID")["AccountID"]
        .astype(int)
        .to_dict()
    )


def _messages_path(use_cleaned: bool = True) -> str:
    p = os.path.join(OUTPUT_DIR, "messages_cleaned.csv")
    return p if (use_cleaned and os.path.exists(p)) else os.path.join(DATA_DIR, "messages.csv")


# ── Analysis 1: topic-level account interactions ──────────────────────────────

def topic_account_interactions(
    account_types: tuple = tuple(COMMUNITY_ACCOUNT_IDS),
    use_cleaned: bool = True,
) -> tuple[pd.DataFrame, float, pd.DataFrame]:
    """
    For each forum topic, counts how many distinct account types posted in it.

    Returns:
        interactions    : (ForumTopicID, AccountID, NumMessages, NumPosters)
        pct_mixed       : % of topics with more than one account type
        cross_topics    : topics where all requested account_types are present
    """
    topic_to_account = _load_topic_account_map(use_cleaned)

    topic_message_counts: dict = defaultdict(lambda: defaultdict(int))
    topic_poster_sets:    dict = defaultdict(lambda: defaultdict(set))

    for chunk in pd.read_csv(
        _messages_path(use_cleaned),
        usecols=["ForumTopicID", ID_COLUMN],
        chunksize=CHUNK_SIZE,
    ):
        chunk["AccountID"] = chunk["ForumTopicID"].map(topic_to_account)
        chunk = chunk.dropna(subset=["AccountID"])
        chunk["AccountID"] = chunk["AccountID"].astype(int)

        for _, row in chunk.iterrows():
            t, a, p = row["ForumTopicID"], row["AccountID"], row[ID_COLUMN]
            topic_message_counts[t][a] += 1
            topic_poster_sets[t][a].add(p)

    rows = [
        {
            "ForumTopicID": t,
            "AccountID":    a,
            "NumMessages":  topic_message_counts[t][a],
            "NumPosters":   len(topic_poster_sets[t][a]),
        }
        for t, accs in topic_poster_sets.items()
        for a in accs
    ]
    interactions = pd.DataFrame(rows)

    topic_account_counts = interactions.groupby("ForumTopicID")["AccountID"].nunique()
    pct_mixed = (topic_account_counts > 1).mean() * 100

    cross_topics = (
        interactions[interactions["AccountID"].isin(account_types)]
        .groupby("ForumTopicID")["AccountID"]
        .nunique()
        .reset_index(name="NumAccountTypes")
        .query("NumAccountTypes == @(len(account_types))")
        .merge(interactions, on="ForumTopicID")
        .sort_values("ForumTopicID")
    )

    print(f"Mixed-account topics: {pct_mixed:.2f}%")
    print(
        f"Topics with all of account types {account_types}: "
        f"{cross_topics['ForumTopicID'].nunique()}"
    )

    return interactions, pct_mixed, cross_topics


# ── Analysis 2: cross-posting users ──────────────────────────────────────────

def posters_across_account_types(
    account_types: tuple = tuple(COMMUNITY_ACCOUNT_IDS),
    use_cleaned: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Identifies posters active in multiple account types.

    Returns:
        poster_accounts_df : (PosterID, AccountTypes, NumAccountTypes)
        cross_posters_df   : subset posting in ALL requested account types
        summary            : dict with counts / percentages
    """
    topic_to_account = _load_topic_account_map(use_cleaned)
    poster_accounts: dict = defaultdict(set)

    for chunk in pd.read_csv(
        _messages_path(use_cleaned),
        usecols=["ForumTopicID", ID_COLUMN],
        chunksize=CHUNK_SIZE,
    ):
        chunk["AccountID"] = chunk["ForumTopicID"].map(topic_to_account)
        chunk = chunk.dropna(subset=["AccountID"])
        chunk["AccountID"] = chunk["AccountID"].astype(int)

        for _, row in chunk.iterrows():
            poster_accounts[row[ID_COLUMN]].add(row["AccountID"])

    poster_accounts_df = (
        pd.DataFrame([
            {
                "PosterID":       poster,
                "AccountTypes":   sorted(accs),
                "NumAccountTypes": len(accs),
            }
            for poster, accs in poster_accounts.items()
        ])
        .sort_values("NumAccountTypes", ascending=False)
        .reset_index(drop=True)
    )

    target = set(account_types)
    cross_posters_df = poster_accounts_df[
        poster_accounts_df["AccountTypes"].apply(lambda a: target.issubset(a))
    ]

    summary = {
        "total_posters":    len(poster_accounts_df),
        "cross_posters":    len(cross_posters_df),
        "pct_cross_posters": (
            len(cross_posters_df) / len(poster_accounts_df) * 100
            if len(poster_accounts_df) else 0
        ),
    }

    print(
        f"Cross-posters ({account_types}): "
        f"{summary['cross_posters']} / {summary['total_posters']} "
        f"({summary['pct_cross_posters']:.1f}%)"
    )

    return poster_accounts_df, cross_posters_df, summary


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    interactions, pct_mixed, cross_topics = topic_account_interactions()
    print("\nInteractions sample:")
    print(interactions.head())

    poster_df, cross_df, summary = posters_across_account_types()
    print("\nCross-poster summary:", summary)
