import os
import re
import pandas as pd
from collections import Counter
from processor import write_csv, count_ids

# ----------------------------------------------------------------------
# Sanity Check
# ----------------------------------------------------------------------

print("Analysis module loaded.")

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

OUTPUT_DIR = "output"
ID_COLUMN = "PosterID"

CSV_FILES = ["messages", "topics"]

# ----------------------------------------------------------------------
# Exploratory data
# ----------------------------------------------------------------------



# ----------------------------------------------------------------------
# Loading utilities
# ----------------------------------------------------------------------
    

def load_df(name: str) -> pd.DataFrame:
    """
    Load a cleaned & anonymized dataframe from output/.
    """
    path = os.path.join(OUTPUT_DIR, f"{name}_account_type_2.0.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _add_word_and_char_counts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["word_count"] = (
        df["MessageText"]
        .fillna("")
        .astype(str)
        .str.split()
        .str.len()
    )
    df["char_count"] = (
        df["MessageText"]
        .fillna("")
        .astype(str)
        .str.len()
    )
    return df


# ----------------------------------------------------------------------
# Core metrics
# ----------------------------------------------------------------------

def words_and_chars_per_user() -> pd.DataFrame:
    messages = load_df("messages")
    messages = _add_word_and_char_counts(messages)

    return (
        messages
        .groupby("PosterID")[["word_count", "char_count"]]
        .sum()
        .reset_index()
        .sort_values("word_count", ascending=False)
    )


def words_and_chars_per_topic() -> pd.DataFrame:
    messages = load_df("messages")
    messages = _add_word_and_char_counts(messages)

    return (
        messages
        .groupby("ForumTopicID")[["word_count", "char_count"]]
        .sum()
        .reset_index()
        .sort_values("word_count", ascending=False)
    )


def messages_per_topic() -> pd.DataFrame:
    messages = load_df("messages")

    return (
        messages
        .groupby("ForumTopicID")
        .size()
        .reset_index(name="message_count")
        .sort_values("message_count", ascending=False)
    )


def topics_per_user() -> pd.DataFrame:
    messages = load_df("messages")

    return (
        messages
        .groupby("PosterID")["ForumTopicID"]
        .nunique()
        .reset_index(name="topic_count")
        .sort_values("topic_count", ascending=False)
    )


def words_per_user_per_month() -> pd.DataFrame:
    messages = load_df("messages")

    if "PostDate" not in messages.columns:
        raise ValueError("PostDate column not found in messages")

    messages["PostDate"] = pd.to_datetime(
        messages["PostDate"], errors="coerce"
    )

    messages = messages.dropna(subset=["PostDate"])
    messages = _add_word_and_char_counts(messages)

    messages["year_month"] = messages["PostDate"].dt.to_period("M").astype(str)

    return (
        messages
        .groupby(["PosterID", "year_month"])["word_count"]
        .sum()
        .reset_index()
        .sort_values(["PosterID", "year_month"])
    )

# ----------------------------------------------------------------------
# Older Analytics
# ----------------------------------------------------------------------

def top(dfs, id_column=ID_COLUMN):
    counts = count_ids(dfs, id_column)
    top_100 = counts.most_common(100)
    top_100_df = pd.DataFrame(top_100, columns=["AnonymizedID", "Count"])
    return top_100_df


def posts_per_topic(dfs):
    if "topics" not in dfs or "messages" not in dfs:
        return None

    topics = dfs["topics"]
    messages = dfs["messages"]

    if "ForumTopicID" not in topics.columns or "ForumTopicID" not in messages.columns:
        return None

    counts = messages["ForumTopicID"].value_counts().reset_index()
    counts.columns = ["ForumTopicID", "PostCount"]

    merged = topics.merge(counts, on="ForumTopicID", how="left")
    merged["PostCount"] = merged["PostCount"].fillna(0).astype(int)
    merged = merged.sort_values("PostCount", ascending=False)

    return merged


def word_frequency(dfs):
    messages = dfs.get("messages")
    if messages is None or "MessageText" not in messages.columns:
        return None

    words = []
    for text in messages["MessageText"].dropna().astype(str):
        tokens = text.split()
        tokens = [re.sub(r"[^\w\s]", "", w).lower() for w in tokens if w]
        words.extend(tokens)

    counter = Counter(words)
    freq_df = pd.DataFrame(counter.items(), columns=["Word", "Count"])
    freq_df = freq_df.sort_values("Count", ascending=False)

    return freq_df


def split_by_account_type(df, column, prefix):
    result = {}
    for value in df[column].dropna().unique():
        subset = df[df[column] == value]
        write_csv(subset, f"{prefix}_{value}.csv")
        result[value] = subset
    return result


def split_topics_by_groups(groups_by_type, topics_df):
    result = {}
    for acc_type, gdf in groups_by_type.items():
        group_ids = gdf["ForumGroupID"].unique()
        subset = topics_df[topics_df["ForumGroupID"].isin(group_ids)]
        write_csv(subset, f"topics_account_type_{acc_type}.csv")
        result[acc_type] = subset
    return result


def split_messages_by_topics(topics_by_type, messages_df):
    result = {}
    for acc_type, tdf in topics_by_type.items():
        topic_ids = tdf["ForumTopicID"].unique()
        subset = messages_df[messages_df["ForumTopicID"].isin(topic_ids)]
        write_csv(subset, f"messages_account_type_{acc_type}.csv")
        result[acc_type] = subset
    return result


def top_posters(message_groups, n=100):
    for acc_type, df in message_groups.items():
        if "PosterID" not in df.columns:
            continue
        top_n = df["PosterID"].value_counts().head(n).reset_index()
        top_n.columns = ["PosterID", "PostCount"]
        write_csv(top_n, f"top_{n}_posters_account_type_{acc_type}.csv")
    return None

# This is the code that used to live under Main to run the older Analytics:

    # posts_per_topic(dfs)
    # word_frequency(dfs)

    # groups = dfs.get("groups")
    # topics = dfs.get("topics")
    # messages = dfs.get("messages")

    # if groups is None or topics is None or messages is None:
    #     return

    # groups_by_type = split_by_account_type(groups, "AccountID", "groups_account_type")
    # topics_by_type = split_topics_by_groups(groups_by_type, topics)
    # messages_by_type = split_messages_by_topics(topics_by_type, messages)

    # top_posters(messages_by_type)

# ----------------------------------------------------------------------
# Batch runner
# ----------------------------------------------------------------------

def run_all(save: bool = True):
    results = {
        "words_chars_per_user.csv": words_and_chars_per_user(),
        "words_chars_per_topic.csv": words_and_chars_per_topic(),
        "messages_per_topic.csv": messages_per_topic(),
        "topics_per_user.csv": topics_per_user(),
        "words_per_user_per_month.csv": words_per_user_per_month(),
    }

    if save:
        for name, df in results.items():
            write_csv(df, name)

    return results


if __name__ == "__main__":
    run_all()
