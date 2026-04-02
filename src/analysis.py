import pandas as pd
from collections import Counter
import re

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def add_word_and_char_counts(messages: pd.DataFrame) -> pd.DataFrame:
    """
    Adds word_count and char_count columns.
    """
    df = messages.copy()

    text = df["MessageText"].fillna("").astype(str)
    df["word_count"] = text.str.split().str.len()
    df["char_count"] = text.str.len()

    return df


# --------------------------------------------------
# Descriptive analytics (Streamlit-friendly)
# --------------------------------------------------

def words_and_chars_per_user(messages: pd.DataFrame) -> pd.DataFrame:
    messages = add_word_and_char_counts(messages)

    return (
        messages
        .groupby("PosterID")[["word_count", "char_count"]]
        .sum()
        .reset_index()
        .sort_values("word_count", ascending=False)
    )


def words_and_chars_per_topic(messages: pd.DataFrame) -> pd.DataFrame:
    messages = add_word_and_char_counts(messages)

    return (
        messages
        .groupby("ForumTopicID")[["word_count", "char_count"]]
        .sum()
        .reset_index()
        .sort_values("word_count", ascending=False)
    )


def messages_per_topic(messages: pd.DataFrame) -> pd.DataFrame:
    return (
        messages
        .groupby("ForumTopicID")
        .size()
        .reset_index(name="message_count")
        .sort_values("message_count", ascending=False)
    )


def topics_per_user(messages: pd.DataFrame) -> pd.DataFrame:
    return (
        messages
        .groupby("PosterID")["ForumTopicID"]
        .nunique()
        .reset_index(name="topic_count")
        .sort_values("topic_count", ascending=False)
    )


def words_per_user_per_month(messages: pd.DataFrame) -> pd.DataFrame:
    if "PostDate" not in messages.columns:
        raise ValueError("PostDate column missing")

    df = messages.copy()
    df["PostDate"] = pd.to_datetime(df["PostDate"], errors="coerce")
    df = df.dropna(subset=["PostDate"])

    df = add_word_and_char_counts(df)
    df["year_month"] = df["PostDate"].dt.to_period("M").astype(str)

    return (
        df
        .groupby(["PosterID", "year_month"])["word_count"]
        .sum()
        .reset_index()
        .sort_values(["PosterID", "year_month"])
    )


def add_rolling_average(
    df: pd.DataFrame,
    group_cols: list[str],
    time_col: str,
    value_col: str,
    window: int = 3,
) -> pd.DataFrame:
    """
    Adds a rolling average column to a time series dataframe.
    """
    out = []

    for _, g in df.groupby(group_cols):
        g = g.sort_values(time_col)
        g[f"{value_col}_rolling_{window}"] = (
            g[value_col]
            .rolling(window=window, min_periods=1)
            .mean()
        )
        out.append(g)

    return pd.concat(out, ignore_index=True)


# ------------------------------------------------------
# text features
# ------------------------------------------------------
def most_common_words(
    messages: pd.DataFrame,
    top_n: int = 50,
    min_length: int = 2,
    lowercase: bool = True,
    remove_stopwords: bool = False,
    stopwords: set[str] = None,
) -> pd.DataFrame:
    """
    Returns the top N most common words in the messages.

    Args:
        messages: DataFrame with a 'MessageText' column.
        top_n: Number of words to return.
        min_length: Minimum word length to include.
        lowercase: Convert words to lowercase.
        remove_stopwords: Whether to remove common stopwords.
        stopwords: Set of stopwords to remove if remove_stopwords=True.
    """
    if "MessageText" not in messages.columns:
        raise ValueError("MessageText column not found")

    words = []

    for text in messages["MessageText"].dropna().astype(str):
        # Remove punctuation
        tokens = re.findall(r"\b\w+\b", text)
        if lowercase:
            tokens = [t.lower() for t in tokens]
        # Filter by length
        tokens = [t for t in tokens if len(t) >= min_length]
        # Remove stopwords if needed
        if remove_stopwords and stopwords is not None:
            tokens = [t for t in tokens if t not in stopwords]
        words.extend(tokens)

    counter = Counter(words)
    top_words = counter.most_common(top_n)

    return pd.DataFrame(top_words, columns=["Word", "Count"])


# --------------------------------------------------
# Aggregated analytics bundle (for Streamlit)
# --------------------------------------------------

def compute_all_metrics(messages: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Returns a dictionary of analytics tables.
    """
    return {
        "words_chars_per_user": words_and_chars_per_user(messages),
        "words_chars_per_topic": words_and_chars_per_topic(messages),
        "messages_per_topic": messages_per_topic(messages),
        "topics_per_user": topics_per_user(messages),
        "words_per_user_per_month": words_per_user_per_month(messages),
    }
