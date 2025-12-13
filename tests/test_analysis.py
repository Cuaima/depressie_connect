import pandas as pd
import pytest
from src.analysis import (
    _add_word_and_char_counts,
    words_and_chars_per_user,
    topics_per_user,
)

@pytest.fixture
def sample_messages():
    return pd.DataFrame({
        "PosterID": ["user_1", "user_1", "user_2"],
        "ForumTopicID": [1, 2, 1],
        "MessageText": [
            "hello world",
            "another message here",
            "short"
        ],
        "PostedDate": [
            "2023-01-15",
            "2023-01-20",
            "2023-02-01"
        ]
    })


def test_add_word_and_char_counts(sample_messages):
    df = _add_word_and_char_counts(sample_messages)

    assert df.loc[0, "word_count"] == 2
    assert df.loc[1, "word_count"] == 3
    assert df.loc[2, "char_count"] == len("short")


def test_words_and_chars_per_user(monkeypatch, sample_messages):
    def mock_load_df(name):
        return sample_messages

    monkeypatch.setattr(
        "src.analysis.load_df",
        mock_load_df
    )

    result = words_and_chars_per_user()

    user_1 = result[result["PosterID"] == "user_1"].iloc[0]
    assert user_1["word_count"] == 5


def test_topics_per_user(monkeypatch, sample_messages):
    def mock_load_df(name):
        return sample_messages

    monkeypatch.setattr(
        "src.analysis.load_df",
        mock_load_df
    )

    result = topics_per_user()

    user_1 = result[result["PosterID"] == "user_1"].iloc[0]
    user_2 = result[result["PosterID"] == "user_2"].iloc[0]

    assert user_1["topic_count"] == 2
    assert user_2["topic_count"] == 1
