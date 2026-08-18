# =============================================================================
# thread_utils.py  –  shared thread-structure and text-metric helpers
# =============================================================================

from __future__ import annotations

import re
import pandas as pd

POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
DATE_COL   = "PostDate"
TOPIC_COL  = "ForumTopicID"


def label_roles(
    df: pd.DataFrame,
    topic_col: str = TOPIC_COL,
    date_col: str = DATE_COL,
) -> pd.DataFrame:
    """Labels the first message in each thread as 'post', all others as 'reply'."""
    df = df.copy().sort_values(date_col)
    first_idx = df.groupby(topic_col)[date_col].idxmin().dropna()
    df["role"] = "reply"
    df.loc[first_idx, "role"] = "post"
    return df


# ── Date parsing ──────────────────────────────────────────────────────────────

def parse_post_dates(series: pd.Series) -> pd.Series:
    """
    Parse PostDate-style columns tolerantly across the two export formats.

    The old export carries millisecond timestamps ('...:49.867'); the new
    export mixes '.000' and second-precision values. Bare pd.to_datetime
    infers a strict format from the FIRST element (pandas >= 2.0), so on a
    mixed column it silently coerces the minority format to NaT — in the
    combined variant this destroyed the dates of ~21k new-export rows.
    format='ISO8601' parses all shapes uniformly.
    """
    return pd.to_datetime(series, format="ISO8601", errors="coerce")


# ── Anonymization placeholder stripping ──────────────────────────────────────

# NER anonymization (preprocess.py) replaces entities with placeholders like
# [ENTITY_PERSON_1] or [ENTITY_WORK_OF_ART_2]. These must stay in the stored
# text (they ARE the anonymization) but must never reach analysis: tokenizers
# split them into words like "of", "work", "art" that pollute LIWC function-
# word and content scores, word frequencies, and word counts.
_ENTITY_PLACEHOLDER_RE = re.compile(r"\[entity_[a-z_]+_\d+\]", re.IGNORECASE)


def strip_entity_placeholders(text: str) -> str:
    """Remove [ENTITY_*_N] anonymization placeholders; collapse leftover spaces."""
    text = _ENTITY_PLACEHOLDER_RE.sub(" ", str(text))
    return re.sub(r" {2,}", " ", text).strip()


def strip_entity_placeholders_col(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Apply strip_entity_placeholders to one text column of a DataFrame."""
    df = df.copy()
    df[column] = df[column].fillna("").map(strip_entity_placeholders)
    return df


# ── Word-level helpers ────────────────────────────────────────────────────────

_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)  # letters only, no digits/underscore


def tokenize_words(text: str) -> list[str]:
    return _WORD_RE.findall(str(text).lower())


def word_count(text: str) -> int:
    return len(str(text).split())


# ── Sentence-level helpers ────────────────────────────────────────────────────

_SENTENCE_SPLIT_RE = re.compile(r"[.!?…]+(?:\s+|$)")


def split_sentences(text: str) -> list[str]:
    """
    Lightweight sentence splitter using .!?… as boundaries.
    Good enough for descriptive stats on forum text; not linguistically rigorous.
    Dutch abbreviations like "dhr.", "bijv." will occasionally over-split.
    """
    text = str(text).strip()
    if not text:
        return []
    return [s.strip() for s in _SENTENCE_SPLIT_RE.split(text) if s.strip()]


def sentence_stats(text: str) -> dict:
    """Per-message sentence-structure metrics."""
    text = str(text)
    sentences = split_sentences(text)
    n_sentences = len(sentences) if sentences else (1 if text.strip() else 0)
    words = tokenize_words(text)
    n_words = len(words)

    return {
        "n_sentences":       n_sentences,
        "n_words":           n_words,
        "words_per_sentence": (n_words / n_sentences) if n_sentences else 0.0,
        "avg_word_length":   (sum(len(w) for w in words) / n_words) if n_words else 0.0,
        "n_questions":       text.count("?"),
        "n_exclamations":    text.count("!"),
        "n_commas":          text.count(","),
        "ends_in_question":  text.strip().endswith("?") if text.strip() else False,
    }


# ── Emoji helper ──────────────────────────────────────────────────────────────

# Broad emoji Unicode ranges; no external dependency needed.
_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0001F1E6-\U0001F1FF"
    "\U00002190-\U000021FF"
    "\U00002B00-\U00002BFF"
    "]",
    flags=re.UNICODE,
)


def extract_emojis(text: str) -> list[str]:
    return _EMOJI_RE.findall(str(text))


def count_emojis(text: str) -> int:
    return len(extract_emojis(text))
