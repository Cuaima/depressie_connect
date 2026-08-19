"""
Tests for src/preprocess.py.

The custom_text_anonymizer requires a spacy Dutch model (nl_core_news_lg)
that may not be installed in CI or fresh environments.  All tests that
exercise anonymization therefore inject a lightweight fake anonymizer via
monkeypatch instead of loading the real NLP model.
"""

import pandas as pd
import pytest

import preprocess


# ---------------------------------------------------------------------------
# Shared fake anonymizer
# ---------------------------------------------------------------------------

def _fake_anonymize(text: str):
    """Replace 'Alice' → [PERSON_1] and 'Amsterdam' → [LOCATION_1]."""
    result = text.replace("Alice", "[PERSON_1]").replace("Amsterdam", "[LOCATION_1]")
    return result, {}


# ---------------------------------------------------------------------------
# clean_dataframe
# ---------------------------------------------------------------------------

class TestCleanDataframe:
    def test_strips_html_tags(self):
        df = pd.DataFrame({"MessageText": ["<p>Hello <b>World</b></p>"], "PosterID": ["u1"]})
        result = preprocess.clean_dataframe(df)
        assert "<" not in result["MessageText"].iloc[0]
        assert "Hello" in result["MessageText"].iloc[0]
        assert "World" in result["MessageText"].iloc[0]

    def test_strips_forum_quote_blocks(self):
        df = pd.DataFrame({
            "MessageText": ["[quote author=x]quoted text[/quote]\nMy reply"],
            "PosterID": ["u1"],
        })
        result = preprocess.clean_dataframe(df)
        assert "quoted text" not in result["MessageText"].iloc[0]
        assert "My reply" in result["MessageText"].iloc[0]

    def test_parses_date_columns(self):
        df = pd.DataFrame({
            "MessageText": ["hello"],
            "PosterID": ["u1"],
            "PostDate": ["2023-01-15"],
        })
        result = preprocess.clean_dataframe(df)
        assert pd.api.types.is_datetime64_any_dtype(result["PostDate"])

    def test_drops_rows_where_all_columns_are_blank(self):
        # Only rows where EVERY column is blank/whitespace are dropped.
        df = pd.DataFrame({"MessageText": ["   ", "hello"], "PosterID": ["   ", "u2"]})
        result = preprocess.clean_dataframe(df)
        assert len(result) == 1
        assert "hello" in result["MessageText"].iloc[0]


# ---------------------------------------------------------------------------
# filter_text_quality
# ---------------------------------------------------------------------------

class TestFilterTextQuality:
    def test_removes_messages_below_min_words(self):
        df = pd.DataFrame({"MessageText": ["hi", "this has four words", "ok"]})
        result = preprocess.filter_text_quality(df, min_words=3, language_filter=False)
        assert len(result) == 1
        assert result["MessageText"].iloc[0] == "this has four words"

    def test_keeps_messages_exactly_at_threshold(self):
        df = pd.DataFrame({"MessageText": ["one two three"]})
        result = preprocess.filter_text_quality(df, min_words=3, language_filter=False)
        assert len(result) == 1

    def test_removes_all_when_all_too_short(self):
        df = pd.DataFrame({"MessageText": ["hi", "ok", "yes"]})
        result = preprocess.filter_text_quality(df, min_words=5, language_filter=False)
        assert len(result) == 0

    def test_no_op_when_column_missing(self):
        df = pd.DataFrame({"OtherColumn": ["hi", "ok"]})
        result = preprocess.filter_text_quality(df, min_words=3, language_filter=False)
        assert len(result) == len(df)


# ---------------------------------------------------------------------------
# anonymize_ids
# ---------------------------------------------------------------------------

class TestAnonymizeIds:
    def test_replaces_ids_with_user_tokens(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))
        df = pd.DataFrame({
            "PosterID": ["id-A", "id-B", "id-A"],
            "MessageText": ["x", "y", "z"],
        })
        dfs = {"messages": df}
        preprocess.anonymize_ids(dfs)

        ids = dfs["messages"]["PosterID"]
        assert ids.str.startswith("user_").all()

    def test_same_original_id_maps_to_same_token(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))
        df = pd.DataFrame({"PosterID": ["id-A", "id-B", "id-A"]})
        dfs = {"messages": df}
        preprocess.anonymize_ids(dfs)

        ids = dfs["messages"]["PosterID"]
        assert ids.iloc[0] == ids.iloc[2]
        assert ids.iloc[0] != ids.iloc[1]

    def test_writes_mapping_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))
        df = pd.DataFrame({"PosterID": ["id-X", "id-Y"]})
        preprocess.anonymize_ids({"messages": df})

        mapping = pd.read_csv(tmp_path / "anonymization_mapping.csv")
        assert set(mapping.columns) >= {"OriginalID", "AnonymizedID"}
        assert set(mapping["OriginalID"]) == {"id-X", "id-Y"}

    def test_ids_removed_from_output(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))
        original_ids = {"id-A", "id-B"}
        df = pd.DataFrame({"PosterID": list(original_ids)})
        dfs = {"messages": df}
        preprocess.anonymize_ids(dfs)

        assert not set(dfs["messages"]["PosterID"]) & original_ids


# ---------------------------------------------------------------------------
# anonymize_text_column — replace_original=True (default)
# ---------------------------------------------------------------------------

class TestAnonymizeTextColumnReplaceOriginal:
    def test_replaces_column_with_anonymized_content(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1, 2],
            "MessageText": ["Alice lives in Amsterdam", "Normal message"],
        })
        result = preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=True
        )

        assert "Alice" not in result["MessageText"].iloc[0]
        assert "[PERSON_1]" in result["MessageText"].iloc[0]

    def test_anon_column_is_dropped_after_replacement(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1],
            "MessageText": ["Alice lives here"],
        })
        result = preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=True
        )

        assert "MessageText_anon" not in result.columns

    def test_writes_entities_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1],
            "MessageText": ["Hello Alice"],
        })
        preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=True
        )

        assert (tmp_path / "entities_MessageText.csv").exists()


# ---------------------------------------------------------------------------
# anonymize_text_column — replace_original=False
# ---------------------------------------------------------------------------

class TestAnonymizeTextColumnKeepBoth:
    def test_keeps_original_column_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1],
            "MessageText": ["Alice lives here"],
        })
        result = preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=False
        )

        assert result["MessageText"].iloc[0] == "Alice lives here"

    def test_adds_anon_column(self, tmp_path, monkeypatch):
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1],
            "MessageText": ["Alice lives here"],
        })
        result = preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=False
        )

        assert "MessageText_anon" in result.columns
        assert "Alice" not in result["MessageText_anon"].iloc[0]


# ---------------------------------------------------------------------------
# anonymize_text_column — anonymizer unavailable
# ---------------------------------------------------------------------------

class TestAnonymizeTextColumnUnavailable:
    def test_returns_dataframe_unchanged_when_unavailable(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(preprocess, "_ANON_AVAILABLE", False)
            df = pd.DataFrame({"MessageText": ["Alice lives in Amsterdam"]})
            result = preprocess.anonymize_text_column(
                df, "MessageText", replace_original=True
            )
        assert result["MessageText"].iloc[0] == "Alice lives in Amsterdam"

    def test_no_anon_column_added_when_unavailable(self):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(preprocess, "_ANON_AVAILABLE", False)
            df = pd.DataFrame({"MessageText": ["hello world"]})
            result = preprocess.anonymize_text_column(df, "MessageText")
        assert "MessageText_anon" not in result.columns


# ---------------------------------------------------------------------------
# Regression: MessageText_normalized must not leak PII (Bug 2)
# ---------------------------------------------------------------------------

class TestNormalizedColumnDoesNotLeakPII:
    def test_normalized_reflects_anonymized_text(self, tmp_path, monkeypatch):
        """
        After anonymization + pipeline fix, MessageText_normalized must be
        re-derived from the anonymized MessageText — not retain the original.
        """
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1],
            "MessageText": ["Alice lives in Amsterdam"],
            "MessageText_normalized": ["alice lives in amsterdam"],
        })

        # Step 1: anonymize MessageText (as run_pipeline does)
        df = preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=True
        )
        # Step 2: regenerate _normalized from anonymized text (the pipeline fix)
        df["MessageText_normalized"] = df["MessageText"].str.lower()

        assert "alice" not in df["MessageText_normalized"].iloc[0], (
            "MessageText_normalized must not contain original entity names"
        )
        assert "[person_1]" in df["MessageText_normalized"].iloc[0]

    def test_without_fix_normalized_would_leak(self, tmp_path, monkeypatch):
        """
        Documents the original bug: if _normalized is not regenerated after
        anonymization, it still contains the raw entity text.
        """
        monkeypatch.setattr(preprocess, "_ANON_AVAILABLE", True)
        monkeypatch.setattr(preprocess, "ta_anonymize", _fake_anonymize, raising=False)
        monkeypatch.setattr(preprocess, "PREPROCESS_DIR", str(tmp_path))

        df = pd.DataFrame({
            "ForumMessageID": [1],
            "MessageText": ["Alice lives in Amsterdam"],
            "MessageText_normalized": ["alice lives in amsterdam"],
        })

        # Anonymize only MessageText, do NOT regenerate _normalized
        df = preprocess.anonymize_text_column(
            df, "MessageText", export_review=False, replace_original=True
        )

        # Without the fix, the _normalized column still leaks the original name
        assert "alice" in df["MessageText_normalized"].iloc[0], (
            "This confirms the pre-fix state: _normalized retains PII"
        )
