import os
import pandas as pd
from collections import defaultdict

DATA_DIR = "data"
CHUNK_SIZE = 100_000


def posters_across_account_types(account_types=(2, 3)):
    """
    Identify posters who participate in multiple account types.

    Returns:
        poster_accounts_df:
            PosterID → list of AccountIDs they post in

        cross_posters_df:
            Subset of posters who post in *all* account_types

        summary:
            dict with counts and percentages
    """

    # --------------------------------------------------
    # Load topic → account map
    # --------------------------------------------------

    topics = pd.read_csv(
        os.path.join(DATA_DIR, "topics.csv"),
        usecols=["ForumTopicID", "ForumGroupID"],
    )

    groups = pd.read_csv(
        os.path.join(DATA_DIR, "groups.csv"),
        usecols=["ForumGroupID", "AccountID"],
    )

    topic_to_account = (
        topics
        .merge(groups, on="ForumGroupID", how="left")
        .dropna(subset=["AccountID"])
        .drop_duplicates("ForumTopicID")
        .set_index("ForumTopicID")["AccountID"]
        .to_dict()
    )

    # --------------------------------------------------
    # Aggregate poster → account participation
    # --------------------------------------------------

    poster_accounts = defaultdict(set)

    messages_path = os.path.join(DATA_DIR, "messages.csv")

    for chunk in pd.read_csv(
        messages_path,
        usecols=["ForumTopicID", "PosterID"],
        chunksize=CHUNK_SIZE,
    ):
        chunk["AccountID"] = chunk["ForumTopicID"].map(topic_to_account)
        chunk = chunk.dropna(subset=["AccountID"])

        for _, row in chunk.iterrows():
            poster_accounts[row["PosterID"]].add(int(row["AccountID"]))

    # --------------------------------------------------
    # Build DataFrames
    # --------------------------------------------------

    poster_accounts_df = (
        pd.DataFrame(
            [
                {
                    "PosterID": poster,
                    "AccountTypes": sorted(accounts),
                    "NumAccountTypes": len(accounts),
                }
                for poster, accounts in poster_accounts.items()
            ]
        )
        .sort_values("NumAccountTypes", ascending=False)
        .reset_index(drop=True)
    )

    cross_posters_df = poster_accounts_df[
        poster_accounts_df["AccountTypes"].apply(
            lambda accs: set(account_types).issubset(accs)
        )
    ]

    # --------------------------------------------------
    # Summary
    # --------------------------------------------------

    summary = {
        "total_posters": len(poster_accounts_df),
        "cross_posters": len(cross_posters_df),
        "pct_cross_posters": (
            len(cross_posters_df) / len(poster_accounts_df) * 100
            if len(poster_accounts_df) > 0 else 0
        ),
    }

    return poster_accounts_df, cross_posters_df, summary

print(posters_across_account_types())