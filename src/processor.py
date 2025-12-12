import os
import re
import warnings
import pandas as pd
from collections import Counter
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from text_anonymizer import anonymize as ta_anonymize, deanonymize as ta_deanonymize


# ----------------------------------------------------------------------
# Sanity Check
# ----------------------------------------------------------------------

print("Processor module loaded.")


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

DATA_DIR = "data"
OUTPUT_DIR = "output"

CSV_FILES = ["accounts", "groups", "messages", "topics"]

ID_COLUMN = "PosterID"
DATE_COLUMNS = ["PostedDate", "StartDate"]


# ----------------------------------------------------------------------
# Utility functions
# ----------------------------------------------------------------------

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def read_csv_file(path):
    try:
        return pd.read_csv(path, on_bad_lines="warn")
    except Exception as e:
        raise RuntimeError(f"Error reading {path}: {e}")


def write_csv(df, name):
    df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)


def anonymize_message_text(df):
    """
    Apply text_anonymizer to MessageText column.
    Stores both anonymized text and mapping for later use.
    """
    if "MessageText" not in df.columns:
        return df, None

    anonymized_texts = []
    mapping_store = []

    for text in df["MessageText"].fillna("").astype(str):
        anonymized, mapping = ta_anonymize(text)
        anonymized_texts.append(anonymized)
        mapping_store.append(mapping)

    df = df.copy()
    df["MessageText"] = anonymized_texts
    df["_MessageText_AnonymizationMap"] = mapping_store

    return df, mapping_store


# ----------------------------------------------------------------------
# Cleaning
# ----------------------------------------------------------------------

def parse_html(text):
    return BeautifulSoup(str(text), "html.parser").get_text()


def convert_dates(df):
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def clean_dataframe(df):
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(how="all").reset_index(drop=True)

    if "MessageText" in df.columns:
        df["MessageText"] = df["MessageText"].astype(str).apply(parse_html)

    df = convert_dates(df)
    return df


# ----------------------------------------------------------------------
# Loading
# ----------------------------------------------------------------------

def load_data(file_list):
    dfs = {}
    for name in file_list:
        path = os.path.join(DATA_DIR, f"{name}.csv")
        df = read_csv_file(path)
        dfs[name] = clean_dataframe(df)
    return dfs


# ----------------------------------------------------------------------
# Anonymization
# ----------------------------------------------------------------------

def build_anonymization_map(dfs, id_column):
    all_ids = set()

    for df in dfs.values():
        if id_column in df.columns:
            all_ids.update(df[id_column].dropna().unique())

    mapping = {uid: f"user_{i+1}" for i, uid in enumerate(sorted(all_ids))}
    return mapping


def apply_mapping(dfs, id_column, mapping):
    for name, df in dfs.items():
        if id_column in df.columns:
            df[id_column] = df[id_column].map(mapping)
    return dfs


def count_ids(dfs, id_column):
    counter = Counter()
    for df in dfs.values():
        if id_column in df.columns:
            counter.update(df[id_column].dropna())
    return counter


def anonymize(dfs, id_column=ID_COLUMN):
    mapping = build_anonymization_map(dfs, id_column)
    apply_mapping(dfs, id_column, mapping)
    counts = count_ids(dfs, id_column)

    mapping_df = pd.DataFrame(mapping.items(), columns=["OriginalID", "AnonymizedID"])
    write_csv(mapping_df, "anonymization_mapping.csv")

    top_100 = counts.most_common(100)
    top_100_df = pd.DataFrame(top_100, columns=["AnonymizedID", "Count"])
    write_csv(top_100_df, "anonymized_top_100.csv")

    return mapping, top_100



# ----------------------------------------------------------------------
# Analytics
# ----------------------------------------------------------------------

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

    write_csv(merged, "topics_with_post_counts.csv")
    write_csv(merged.head(100), "top_100_topics_by_post_count.csv")

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

    write_csv(freq_df, "word_count_message_text.csv")
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


# ----------------------------------------------------------------------
# Main pipeline
# ----------------------------------------------------------------------

def main():
    ensure_output_dir()

    dfs = load_data(CSV_FILES)
    anonymize(dfs)

    posts_per_topic(dfs)
    word_frequency(dfs)

    groups = dfs.get("groups")
    topics = dfs.get("topics")
    messages = dfs.get("messages")

    if groups is None or topics is None or messages is None:
        return

    groups_by_type = split_by_account_type(groups, "AccountID", "groups_account_type")
    topics_by_type = split_topics_by_groups(groups_by_type, topics)
    messages_by_type = split_messages_by_topics(topics_by_type, messages)

    top_posters(messages_by_type)


if __name__ == "__main__":
    main()
