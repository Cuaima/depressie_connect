"""Tests for src/postprocess.py."""

import pandas as pd
import pytest

import postprocess


def _thread_df(*rows):
    """Build a minimal messages DataFrame from (topic_id, date, text) tuples."""
    return pd.DataFrame(
        rows, columns=["ForumTopicID", "PostDate", "MessageText"]
    ).assign(PostDate=lambda df: pd.to_datetime(df["PostDate"]))


# ---------------------------------------------------------------------------
# filter_intro_groups
# ---------------------------------------------------------------------------

class TestFilterIntroGroups:
    def test_removes_threads_with_matching_keyword(self):
        df = pd.DataFrame({
            "ForumTopicID": [1, 1, 2, 2],
            "GroupName": ["welkom", "welkom", "Steun", "Steun"],
            "MessageText": ["a", "b", "c", "d"],
        })
        result = postprocess.filter_intro_groups(df, keywords={"welkom"})
        assert 1 not in result["ForumTopicID"].values
        assert set(result["ForumTopicID"]) == {2}

    def test_keeps_threads_without_matching_keyword(self):
        df = pd.DataFrame({
            "ForumTopicID": [1, 2],
            "GroupName": ["Steun", "Ervaringen"],
            "MessageText": ["a", "b"],
        })
        result = postprocess.filter_intro_groups(df, keywords={"welkom"})
        assert len(result) == 2

    def test_case_insensitive_matching(self):
        df = pd.DataFrame({
            "ForumTopicID": [1],
            "GroupName": ["WELKOM"],
            "MessageText": ["hello"],
        })
        result = postprocess.filter_intro_groups(df, keywords={"welkom"})
        assert len(result) == 0

    def test_skips_gracefully_when_group_name_missing(self):
        df = pd.DataFrame({"ForumTopicID": [1, 2], "MessageText": ["a", "b"]})
        result = postprocess.filter_intro_groups(df)
        assert len(result) == 2

    def test_removes_all_messages_in_matched_thread(self):
        """Every message in a matched thread should be removed, not just the first."""
        df = pd.DataFrame({
            "ForumTopicID": [1, 1, 1, 2],
            "GroupName": ["welkom", "welkom", "welkom", "Steun"],
            "MessageText": ["a", "b", "c", "d"],
        })
        result = postprocess.filter_intro_groups(df, keywords={"welkom"})
        assert len(result) == 1

    def test_partial_keyword_match_in_group_name(self):
        """A group name containing the keyword (not exact) should also be filtered."""
        df = pd.DataFrame({
            "ForumTopicID": [1],
            "GroupName": ["off-topic discussion"],
            "MessageText": ["a"],
        })
        result = postprocess.filter_intro_groups(df, keywords={"off-topic"})
        assert len(result) == 0


# ---------------------------------------------------------------------------
# build_thread_structure
# ---------------------------------------------------------------------------

class TestBuildThreadStructure:
    def _make_df(self):
        return _thread_df(
            (1, "2023-01-01", "first"),
            (1, "2023-01-02", "second"),
            (1, "2023-01-03", "third"),
            (2, "2023-02-01", "alpha"),
            (2, "2023-02-02", "beta"),
        )

    def test_first_message_per_thread_is_initial_post(self):
        result = postprocess.build_thread_structure(self._make_df())
        initial = result[result["is_initial_post"]]
        assert len(initial) == 2
        assert set(initial["ForumTopicID"]) == {1, 2}

    def test_reply_index_starts_at_zero(self):
        result = postprocess.build_thread_structure(self._make_df())
        assert result.groupby("ForumTopicID")["reply_index"].min().eq(0).all()

    def test_reply_index_increments_sequentially(self):
        result = postprocess.build_thread_structure(self._make_df())
        t1 = result[result["ForumTopicID"] == 1].sort_values("PostDate")
        assert list(t1["reply_index"]) == [0, 1, 2]

    def test_single_message_thread_is_initial_post(self):
        df = _thread_df((99, "2023-01-01", "only message"))
        result = postprocess.build_thread_structure(df)
        assert result["is_initial_post"].all()
        assert result["reply_index"].iloc[0] == 0

    def test_raises_when_date_column_missing(self):
        df = pd.DataFrame({"ForumTopicID": [1, 2], "MessageText": ["a", "b"]})
        with pytest.raises(ValueError, match="PostDate"):
            postprocess.build_thread_structure(df)


# ---------------------------------------------------------------------------
# label_thread_success
# ---------------------------------------------------------------------------

class TestLabelThreadSuccess:
    def _structured_df(self):
        df = _thread_df(
            (1, "2023-01-01", "post1"),
            (1, "2023-01-02", "reply1"),
            (2, "2023-02-01", "alone"),
        )
        return postprocess.build_thread_structure(df)

    def test_thread_with_replies_has_flag_true(self):
        result = postprocess.label_thread_success(self._structured_df())
        t1 = result[result["ForumTopicID"] == 1]
        assert t1["thread_has_replies"].all()

    def test_thread_without_replies_has_flag_false(self):
        result = postprocess.label_thread_success(self._structured_df())
        t2 = result[result["ForumTopicID"] == 2]
        assert not t2["thread_has_replies"].any()

    def test_reply_count_reflects_number_of_replies(self):
        result = postprocess.label_thread_success(self._structured_df())
        t1_initial = result[
            (result["ForumTopicID"] == 1) & result["is_initial_post"]
        ]
        assert t1_initial["reply_count"].iloc[0] == 1

    def test_solo_thread_has_reply_count_zero(self):
        result = postprocess.label_thread_success(self._structured_df())
        t2_initial = result[
            (result["ForumTopicID"] == 2) & result["is_initial_post"]
        ]
        assert t2_initial["reply_count"].iloc[0] == 0


# ---------------------------------------------------------------------------
# _normalize_dutch_text
# ---------------------------------------------------------------------------

class TestNormalizeDutchText:
    def test_lowercases_text(self):
        assert postprocess._normalize_dutch_text("Hello World") == "hello world"

    def test_collapses_four_or_more_repeated_chars_to_two(self):
        result = postprocess._normalize_dutch_text("heeeeeel goed")
        assert "heeee" not in result
        assert result.startswith("he")

    def test_three_repeated_chars_kept_unchanged(self):
        result = postprocess._normalize_dutch_text("nee nee nee")
        assert "nee" in result

    def test_collapses_multiple_spaces(self):
        result = postprocess._normalize_dutch_text("too   many   spaces")
        assert "  " not in result

    def test_strips_leading_and_trailing_whitespace(self):
        assert postprocess._normalize_dutch_text("  hello  ") == "hello"

    def test_collapses_excess_newlines_to_two(self):
        result = postprocess._normalize_dutch_text("line1\n\n\n\nline2")
        assert "\n\n\n" not in result

    def test_strips_entity_placeholders(self):
        result = postprocess._normalize_dutch_text(
            "ik las [ENTITY_WORK_OF_ART_1] van [ENTITY_PERSON_2] gisteren"
        )
        assert "entity" not in result
        assert result == "ik las van gisteren"

    def test_entity_placeholder_multiword_type(self):
        result = postprocess._normalize_dutch_text("[ENTITY_WORK_OF_ART_12]")
        assert result == ""


# ---------------------------------------------------------------------------
# normalize_text
# ---------------------------------------------------------------------------

class TestNormalizeText:
    def test_adds_text_normalized_column(self):
        df = pd.DataFrame({"MessageText": ["Hello World"]})
        result = postprocess.normalize_text(df)
        assert "text_normalized" in result.columns

    def test_normalized_column_is_lowercase(self):
        df = pd.DataFrame({"MessageText": ["Hello WORLD"]})
        result = postprocess.normalize_text(df)
        assert result["text_normalized"].iloc[0] == "hello world"

    def test_original_column_is_unchanged(self):
        df = pd.DataFrame({"MessageText": ["Hello WORLD"]})
        result = postprocess.normalize_text(df)
        assert result["MessageText"].iloc[0] == "Hello WORLD"

    def test_skips_when_column_missing(self):
        df = pd.DataFrame({"OtherColumn": ["hello"]})
        result = postprocess.normalize_text(df)
        assert "text_normalized" not in result.columns


# ---------------------------------------------------------------------------
# get_input_path / get_output_name
# ---------------------------------------------------------------------------

class TestPathHelpers:
    def test_default_input_path(self):
        path = postprocess.get_input_path(None)
        assert path.endswith("messages_community.csv")

    def test_old_dataset_input_path(self):
        path = postprocess.get_input_path("old")
        assert "_old" in path

    def test_combined_dataset_no_suffix(self):
        path = postprocess.get_input_path("combined")
        assert "_combined" not in path

    def test_default_output_name(self):
        assert postprocess.get_output_name(None) == "messages_structured.csv"

    def test_old_dataset_output_name(self):
        assert postprocess.get_output_name("old") == "messages_structured_old.csv"

    def test_new_only_dataset_output_name(self):
        assert postprocess.get_output_name("new_only") == "messages_structured_new_only.csv"
