"""Tests for src/liwc_analysis.py — pure functions only (no NLP model, no file I/O)."""

import pandas as pd
import pytest

from liwc_analysis import (
    _tokenize,
    _match_term,
    score_text,
    load_liwc_dic,
    load_liwc_csv,
    score_messages,
    per_user_summary,
)


# ---------------------------------------------------------------------------
# _tokenize
# ---------------------------------------------------------------------------

class TestTokenize:
    def test_returns_lowercase_words(self):
        assert _tokenize("Hello World") == ["hello", "world"]

    def test_strips_punctuation(self):
        tokens = _tokenize("Hello, world!")
        assert "hello," not in tokens
        assert "world!" not in tokens
        assert "hello" in tokens

    def test_returns_empty_list_for_empty_string(self):
        assert _tokenize("") == []

    def test_handles_numbers(self):
        tokens = _tokenize("I have 3 cats")
        assert "3" in tokens

    def test_splits_on_whitespace(self):
        assert len(_tokenize("one two three")) == 3


# ---------------------------------------------------------------------------
# _match_term
# ---------------------------------------------------------------------------

class TestMatchTerm:
    def test_exact_match(self):
        assert _match_term("happy", "happy") is True

    def test_exact_no_match(self):
        assert _match_term("happy", "sad") is False

    def test_wildcard_prefix_match(self):
        assert _match_term("happiness", "happ*") is True
        assert _match_term("happy", "happ*") is True
        assert _match_term("happily", "happ*") is True

    def test_wildcard_no_match(self):
        assert _match_term("sad", "happ*") is False

    def test_wildcard_requires_prefix(self):
        assert _match_term("unhappy", "happ*") is False

    def test_exact_term_does_not_use_prefix_logic(self):
        assert _match_term("happiness", "happy") is False


# ---------------------------------------------------------------------------
# score_text
# ---------------------------------------------------------------------------

class TestScoreText:
    def test_counts_matching_terms(self):
        term_to_cats = {"happy": ["posemo"], "sad": ["negemo"]}
        cats = ["posemo", "negemo"]
        result = score_text("I am happy and sad", term_to_cats, cats)
        assert result["posemo"] == 1
        assert result["negemo"] == 1

    def test_wildcard_counts_multiple_tokens(self):
        term_to_cats = {"happ*": ["posemo"]}
        cats = ["posemo"]
        result = score_text("happiness and happily", term_to_cats, cats)
        assert result["posemo"] == 2

    def test_zero_for_no_match(self):
        term_to_cats = {"happy": ["posemo"]}
        cats = ["posemo", "negemo"]
        result = score_text("nothing relevant here", term_to_cats, cats)
        assert result["posemo"] == 0

    def test_all_categories_initialized_to_zero(self):
        result = score_text("some text", {}, ["posemo", "negemo", "affect"])
        assert all(v == 0 for v in result.values())

    def test_term_matched_at_most_once_per_token(self):
        """First matching term wins; a token counted once even if it matches multiple terms."""
        # "happy" matches "happ*" and "happy" — should be counted once
        term_to_cats = {"happ*": ["posemo"], "happy": ["affect"]}
        cats = ["posemo", "affect"]
        result = score_text("happy", term_to_cats, cats)
        total = sum(result.values())
        assert total == 1

    def test_multi_category_term(self):
        term_to_cats = {"happy": ["posemo", "affect"]}
        cats = ["posemo", "affect"]
        result = score_text("happy", term_to_cats, cats)
        assert result["posemo"] == 1
        assert result["affect"] == 1


# ---------------------------------------------------------------------------
# load_liwc_dic
# ---------------------------------------------------------------------------

class TestLoadLiwcDic:
    def _write_dic(self, tmp_path, content: str):
        f = tmp_path / "test.dic"
        f.write_text(content, encoding="utf-8")
        return str(f)

    def test_parses_categories_and_terms(self, tmp_path):
        content = "%\n1\taffect\n2\tposemo\n%\nhappy\t1\t2\nsad\t1\n"
        path = self._write_dic(tmp_path, content)
        terms, cats = load_liwc_dic(path)
        assert "happy" in terms
        assert "affect" in terms["happy"]
        assert "posemo" in terms["happy"]
        assert "sad" in terms
        assert "affect" in terms["sad"]
        assert "posemo" not in terms["sad"]

    def test_wildcard_term_preserved(self, tmp_path):
        content = "%\n1\taffect\n%\nhapp*\t1\n"
        path = self._write_dic(tmp_path, content)
        terms, _ = load_liwc_dic(path)
        assert "happ*" in terms

    def test_category_map_populated(self, tmp_path):
        content = "%\n1\tfunction\n2\tpronoun\n%\nI\t1\t2\n"
        path = self._write_dic(tmp_path, content)
        _, cats = load_liwc_dic(path)
        assert cats["1"] == "function"
        assert cats["2"] == "pronoun"

    def test_raises_on_missing_percent_markers(self, tmp_path):
        content = "1\taffect\nhappy\t1\n"
        path = self._write_dic(tmp_path, content)
        with pytest.raises(ValueError, match="%"):
            load_liwc_dic(path)


# ---------------------------------------------------------------------------
# load_liwc_csv
# ---------------------------------------------------------------------------

class TestLoadLiwcCsv:
    def _write_csv(self, tmp_path, content: str):
        f = tmp_path / "test.csv"
        f.write_text(content, encoding="utf-8")
        return str(f)

    def test_parses_term_and_category_columns(self, tmp_path):
        content = "term,category\nhappy,posemo\nsad,negemo\n"
        path = self._write_csv(tmp_path, content)
        terms, cats = load_liwc_csv(path)
        assert "happy" in terms
        assert "posemo" in terms["happy"]
        assert "sad" in terms

    def test_accepts_categories_column_name(self, tmp_path):
        # "categories" is an accepted alias for "category"
        content = "term,categories\nhappy,posemo\nsad,negemo\n"
        path = self._write_csv(tmp_path, content)
        terms, _ = load_liwc_csv(path)
        assert "happy" in terms

    def test_raises_on_missing_term_column(self, tmp_path):
        content = "word,category\nhappy,posemo\n"
        path = self._write_csv(tmp_path, content)
        with pytest.raises(ValueError, match="term"):
            load_liwc_csv(path)

    def test_raises_on_missing_category_column(self, tmp_path):
        content = "term,label\nhappy,posemo\n"
        path = self._write_csv(tmp_path, content)
        with pytest.raises(ValueError):
            load_liwc_csv(path)


# ---------------------------------------------------------------------------
# score_messages
# ---------------------------------------------------------------------------

class TestScoreMessages:
    def _make_df(self):
        return pd.DataFrame({
            "PosterID": ["u1", "u2"],
            "MessageText": ["I am happy today", "feeling very sad"],
            "PostDate": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        })

    def test_adds_prefixed_category_columns(self):
        term_to_cats = {"happy": ["posemo"], "sad": ["negemo"]}
        cats = ["posemo", "negemo"]
        df = self._make_df()
        result, liwc_cols = score_messages(df, term_to_cats, cats)
        assert "liwc_posemo" in result.columns
        assert "liwc_negemo" in result.columns

    def test_adds_word_count_column(self):
        df = self._make_df()
        result, _ = score_messages(df, {}, [])
        assert "word_count" in result.columns
        assert result["word_count"].iloc[0] == 4   # "I am happy today"

    def test_adds_percentage_columns(self):
        term_to_cats = {"happy": ["posemo"]}
        cats = ["posemo"]
        df = self._make_df()
        result, _ = score_messages(df, term_to_cats, cats)
        assert "liwc_posemo_pct" in result.columns

    def test_percentage_calculation(self):
        term_to_cats = {"happy": ["posemo"]}
        cats = ["posemo"]
        df = pd.DataFrame({"PosterID": ["u1"], "MessageText": ["happy happy sad"]})
        result, _ = score_messages(df, term_to_cats, cats)
        # 2 matches out of 3 words = 66.667%
        assert abs(result["liwc_posemo_pct"].iloc[0] - 66.667) < 0.1


# ---------------------------------------------------------------------------
# per_user_summary
# ---------------------------------------------------------------------------

class TestPerUserSummary:
    def test_aggregates_by_user(self):
        df = pd.DataFrame({
            "PosterID": ["u1", "u1", "u2"],
            "MessageText": ["a", "b", "c"],
            "word_count": [3, 4, 5],
            "liwc_posemo": [1, 0, 1],
        })
        result = per_user_summary(df, liwc_cols=["liwc_posemo"])
        assert len(result) == 2

    def test_sums_word_count_per_user(self):
        df = pd.DataFrame({
            "PosterID": ["u1", "u1"],
            "MessageText": ["a", "b"],
            "word_count": [3, 4],
            "liwc_posemo": [0, 0],
        })
        result = per_user_summary(df, liwc_cols=["liwc_posemo"])
        u1 = result[result["PosterID"] == "u1"].iloc[0]
        assert u1["word_count"] == 7

    def test_adds_message_count_column(self):
        df = pd.DataFrame({
            "PosterID": ["u1", "u1", "u2"],
            "MessageText": ["a", "b", "c"],
            "word_count": [1, 1, 1],
            "liwc_posemo": [0, 0, 0],
        })
        result = per_user_summary(df, liwc_cols=["liwc_posemo"])
        u1 = result[result["PosterID"] == "u1"].iloc[0]
        assert u1["message_count"] == 2
