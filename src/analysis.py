import os
import pandas as pd

OUTPUT_DIR = "output"

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


def save_df(df: pd.DataFrame, filename: str):
    """
    Save a dataframe to output/.
    """
    df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)


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
            save_df(df, name)

    return results


if __name__ == "__main__":
    run_all()
