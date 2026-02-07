import os
import pandas as pd
from collections import defaultdict

DATA_DIR = "data"
CHUNK_SIZE = 100_000  # adjust if needed


# --------------------------------------------------
# Load small reference tables
# --------------------------------------------------

def load_topic_account_map():
    topics = pd.read_csv(
        os.path.join(DATA_DIR, "topics.csv"),
        usecols=["ForumTopicID", "ForumGroupID"],
    )

    groups = pd.read_csv(
        os.path.join(DATA_DIR, "groups.csv"),
        usecols=["ForumGroupID", "AccountID"],
    )

    topics = topics.merge(groups, on="ForumGroupID", how="left")

    # ENFORCE uniqueness: one row per topic
    topics = (
        topics
        .dropna(subset=["AccountID"])
        .drop_duplicates(subset=["ForumTopicID"])
    )

    return dict(
        zip(topics["ForumTopicID"], topics["AccountID"])
    )


# --------------------------------------------------
# Core analysis (chunked)
# --------------------------------------------------

def topic_account_interactions(account_types=(2, 3)):
    topic_to_account = load_topic_account_map()

    # Aggregation structures
    topic_accounts = defaultdict(set)
    topic_message_counts = defaultdict(lambda: defaultdict(int))
    topic_poster_sets = defaultdict(lambda: defaultdict(set))

    # Stream messages
    messages_path = os.path.join(DATA_DIR, "messages.csv")

    for chunk in pd.read_csv(
        messages_path,
        usecols=["ForumMessageID", "ForumTopicID", "PosterID"],
        chunksize=CHUNK_SIZE,
    ):
        # Attach AccountID
        chunk["AccountID"] = chunk["ForumTopicID"].map(topic_to_account)
        chunk = chunk.dropna(subset=["AccountID"])

        for _, row in chunk.iterrows():
            topic = row["ForumTopicID"]
            acc = row["AccountID"]
            poster = row["PosterID"]

            topic_accounts[topic].add(acc)
            topic_message_counts[topic][acc] += 1
            topic_poster_sets[topic][acc].add(poster)

    # --------------------------------------------------
    # Build interaction dataframe
    # --------------------------------------------------

    rows = []
    for topic, accounts in topic_accounts.items():
        for acc in accounts:
            rows.append(
                {
                    "ForumTopicID": topic,
                    "AccountID": acc,
                    "NumMessages": topic_message_counts[topic][acc],
                    "NumPosters": len(topic_poster_sets[topic][acc]),
                }
            )

    interactions = pd.DataFrame(rows)

    # --------------------------------------------------
    # Mixed-topic analysis
    # --------------------------------------------------

    topic_account_counts = (
        interactions
        .groupby("ForumTopicID")["AccountID"]
        .nunique()
    )

    pct_mixed = (topic_account_counts > 1).mean() * 100

    cross_topics = (
        interactions
        .query("AccountID in @account_types")
        .groupby("ForumTopicID")["AccountID"]
        .nunique()
        .reset_index(name="NumAccountTypes")
        .query("NumAccountTypes == 2")
        .merge(interactions, on="ForumTopicID")
        .sort_values("ForumTopicID")
    )

    print(f"Percentage of mixed-account topics: {pct_mixed:.2f}%")
    print(
        f"Topics where account types {account_types} interact: "
        f"{cross_topics['ForumTopicID'].nunique()}"
    )

    return interactions, pct_mixed, cross_topics


# --------------------------------------------------
# Entry point
# --------------------------------------------------

if __name__ == "__main__":
    interactions, pct_mixed, cross_topics = topic_account_interactions()

    print("\nInteractions head():")
    print(interactions.head())

    print("\nCross-account topics head():")
    print(cross_topics.head())
