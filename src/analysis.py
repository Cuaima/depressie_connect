import pandas as pd
from collections import Counter
import re


def posts_per_topic(dfs):
    if "topics" not in dfs or "messages" not in dfs:
        return

    topics_df = dfs["topics"]
    messages_df = dfs["messages"]

    if "ForumTopicID" not in topics_df.columns:
        return

    topic_post_counts = messages_df["ForumTopicID"].value_counts().reset_index()
    topic_post_counts.columns = ["ForumTopicID", "PostCount"]

    merged = pd.merge(topics_df, topic_post_counts, on="ForumTopicID", how="left")
    merged["PostCount"] = merged["PostCount"].fillna(0).astype(int)
    merged = merged.sort_values("PostCount", ascending=False)

    merged.to_csv("topics_with_post_counts.csv", index=False)
    merged.head(100).to_csv("top_100_topics_by_post_count.csv", index=False)

    return merged


def word_count_message_text(dfs):
    if "messages" not in dfs:
        return

    messages_df = dfs["messages"]
    if "MessageText" not in messages_df.columns:
        return

    all_words = []
    for text in messages_df["MessageText"].dropna().astype(str):
        words = text.split()
        words = [re.sub(r"[^\w\s]", "", w).lower() for w in words if w]
        all_words.extend(words)

    word_counts = Counter(all_words)
    df = pd.DataFrame(word_counts.items(), columns=["Word", "Count"]).sort_values(
        "Count", ascending=False
    )

    df.to_csv("word_count_message_text.csv", index=False)
    return df


def sort_groups_by_account_type(dfs):
    if "groups" not in dfs:
        return

    groups_df = dfs["groups"]
    if "AccountID" not in groups_df.columns:
        return

    result = {}
    for account_type in groups_df["AccountID"].unique():
        df = groups_df[groups_df["AccountID"] == account_type]
        df.to_csv(f"groups_account_type_{account_type}.csv", index=False)
        result[account_type] = df

    return result


def sort_topics_by_account_type(groups_by_account_type, dfs):
    if "topics" not in dfs:
        return

    topics_df = dfs["topics"]
    if "ForumGroupID" not in topics_df.columns:
        return

    result = {}
    for account_type, group_df in groups_by_account_type.items():
        ids = group_df["ForumGroupID"].unique()
        df = topics_df[topics_df["ForumGroupID"].isin(ids)]
        df.to_csv(f"topics_account_type_{account_type}.csv", index=False)
        result[account_type] = df

    return result


def sort_messages_by_account_type(topics_by_account_type, dfs):
    if "messages" not in dfs:
        return

    messages_df = dfs["messages"]
    if "ForumTopicID" not in messages_df.columns:
        return

    result = {}
    for account_type, topic_df in topics_by_account_type.items():
        ids = topic_df["ForumTopicID"].unique()
        df = messages_df[messages_df["ForumTopicID"].isin(ids)]
        df.to_csv(f"messages_account_type_{account_type}.csv", index=False)
        result[account_type] = df

    return result


def top_posters_by_account_type(messages_by_account_type, top_n=100):
    for account_type, df in messages_by_account_type.items():
        if "PosterID" not in df.columns:
            continue

        counts = df["PosterID"].value_counts().head(top_n).reset_index()
        counts.columns = ["PosterID", "PostCount"]
        counts.to_csv(
            f"top_{top_n}_posters_account_type_{account_type}.csv", index=False
        )

    return counts