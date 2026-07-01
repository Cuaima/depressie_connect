from __future__ import annotations
import re

# Dutch absolutist words — translated from the 19-word English set in
# Al-Mosaiwi & Johnstone (2018). REVIEW with a native Dutch speaker before
# treating results as scientifically reliable.
ABSOLUTIST_WORDS_NL: list[str] = [
    "altijd",       # always
    "nooit",        # never
    "iedereen",     # everyone
    "niemand",      # nobody / no one
    "alles",        # everything
    "niets",        # nothing
    "constant",     # constant(ly)
    "voortdurend",  # constantly / continually
    "steeds",       # constantly / ever
    "volledig",     # completely / fully
    "totaal",       # totally
    "absoluut",     # absolutely
    "zeker",        # certainly / definitely
    "definitief",   # definitely
    "heel",         # whole / entirely
    "geheel",       # entirely / wholly
    "moeten",       # must (infinitive)
    "moet",         # must (3rd person)
    "ooit",         # ever
]

_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def score_absolutist(text: str, wordlist: list[str] = ABSOLUTIST_WORDS_NL) -> int:
    """Count absolutist word occurrences in text (whole-word, case-insensitive)."""
    word_set = set(wordlist)
    return sum(1 for t in _WORD_RE.findall(text.lower()) if t in word_set)


def absolutist_rate(text: str, wordlist: list[str] = ABSOLUTIST_WORDS_NL) -> float:
    """Return absolutist words as % of all words; 0.0 if text is empty."""
    tokens = _WORD_RE.findall(text.lower())
    if not tokens:
        return 0.0
    word_set = set(wordlist)
    return round(sum(1 for t in tokens if t in word_set) / len(tokens) * 100, 3)
