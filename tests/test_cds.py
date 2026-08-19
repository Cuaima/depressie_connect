"""
Tests for:
  - utils/CDS.py          — CDS phrase matching and dataset processing
  - utils/thread_utils.py — thread role labelling
  - cds_prevalence.py     — time columns and category ranking
"""

import pandas as pd
import pytest

from utils.CDS import find_CDS_in_text, load_CDS, process_dataset
from utils.thread_utils import label_roles
from cds_prevalence import add_time_columns, compute_category_ranking


# ---------------------------------------------------------------------------
# find_CDS_in_text
# ---------------------------------------------------------------------------

class TestFindCDSInText:
    # NOTE: find_CDS_in_text lowercases the CDS pattern but does NOT lowercase
    # the input text.  In the pipeline, process_dataset pre-lowercases all text
    # before calling this function.  Tests must follow the same convention.

    def test_finds_exact_phrase(self):
        assert find_CDS_in_text("I never", [], "i never do anything right") == 1

    def test_returns_zero_when_phrase_absent(self):
        assert find_CDS_in_text("I never", [], "i always succeed") == 0

    def test_finds_variant(self):
        assert find_CDS_in_text("I am a", ["I'm a"], "i'm a failure") == 1

    def test_returns_zero_when_variant_also_absent(self):
        assert find_CDS_in_text("I am a", ["I'm a"], "everything is fine") == 0

    def test_word_boundary_respected(self):
        assert find_CDS_in_text("I never", [], "i never give up") == 1
        assert find_CDS_in_text("failure", [], "this is not a failure at all") == 1

    def test_pattern_is_lowercased(self):
        # Uppercase CDS key is lowercased by the function before matching
        assert find_CDS_in_text("I never", [], "i never win") == 1


# ---------------------------------------------------------------------------
# load_CDS
# ---------------------------------------------------------------------------

class TestLoadCDS:
    def test_loads_english_dictionary(self):
        cds = load_CDS(language="EN")
        assert len(cds) > 0
        assert "categories" in cds.columns
        assert "variants" in cds.columns

    def test_loads_dutch_dictionary(self):
        cds = load_CDS(language="NL")
        assert len(cds) > 0

    def test_raises_for_unsupported_language(self):
        with pytest.raises(NotImplementedError):
            load_CDS(language="XX")

    def test_variants_are_lists(self):
        cds = load_CDS(language="EN")
        assert all(isinstance(v, list) for v in cds["variants"])


# ---------------------------------------------------------------------------
# process_dataset
# ---------------------------------------------------------------------------

class TestProcessDataset:
    def _make_tweets(self, texts):
        return pd.DataFrame({"text": texts})

    def test_per_tweet_returns_cds_flag(self):
        df = self._make_tweets(["I never do anything right", "hello world"])
        result = process_dataset(df, output="per_tweet", language="EN")
        assert "CDS" in result.columns
        assert result["CDS"].dtype in (int, bool, "int64", "bool")

    def test_per_category_returns_category_columns(self):
        df = self._make_tweets(["I never do anything right", "hello world"])
        result = process_dataset(df, output="per_category", language="EN")
        assert len(result.columns) > 1

    def test_per_phrase_returns_phrase_columns(self):
        df = self._make_tweets(["I never do anything right"])
        result = process_dataset(df, output="per_phrase", language="EN")
        assert "I never" in result.columns

    def test_all_variants_returns_three_objects(self):
        df = self._make_tweets(["some text here"])
        phrases, cats, per_tweet = process_dataset(df, output="all_variants", language="EN")
        assert len(phrases) == 1
        assert len(cats) == 1
        assert len(per_tweet) == 1

    def test_raises_on_missing_text_column(self):
        df = pd.DataFrame({"content": ["hello"]})
        with pytest.raises(ValueError, match="text"):
            process_dataset(df)

    def test_raises_on_invalid_output_type(self):
        df = self._make_tweets(["hello"])
        with pytest.raises(NotImplementedError):
            process_dataset(df, output="invalid_type")

    def test_english_phrase_detected(self):
        df = self._make_tweets(["i am a failure and i never succeed"])
        result = process_dataset(df, output="per_tweet", language="EN")
        assert result["CDS"].iloc[0] == 1

    def test_no_cds_in_neutral_text(self):
        df = self._make_tweets(["the weather is nice today"])
        result = process_dataset(df, output="per_tweet", language="EN")
        assert result["CDS"].iloc[0] == 0


# ---------------------------------------------------------------------------
# label_roles  (utils/thread_utils.py)
# ---------------------------------------------------------------------------

class TestLabelRoles:
    def _make_df(self, rows):
        return pd.DataFrame(rows, columns=["ForumTopicID", "PostDate", "MessageText"]
            ).assign(PostDate=lambda d: pd.to_datetime(d["PostDate"]))

    def test_first_message_per_thread_is_labeled_post(self):
        df = self._make_df([(1, "2023-01-01", "first"), (1, "2023-01-02", "second")])
        result = label_roles(df)
        first = result[result["MessageText"] == "first"]
        assert first["role"].iloc[0] == "post"

    def test_subsequent_messages_labeled_reply(self):
        df = self._make_df([
            (1, "2023-01-01", "first"),
            (1, "2023-01-02", "second"),
            (1, "2023-01-03", "third"),
        ])
        result = label_roles(df)
        assert (result["role"] == "reply").sum() == 2

    def test_each_thread_has_exactly_one_post(self):
        df = self._make_df([
            (1, "2023-01-01", "a"), (1, "2023-01-02", "b"),
            (2, "2023-02-01", "c"), (2, "2023-02-02", "d"),
            (3, "2023-03-01", "e"),
        ])
        result = label_roles(df)
        posts_per_thread = result[result["role"] == "post"].groupby("ForumTopicID").size()
        assert (posts_per_thread == 1).all()

    def test_single_message_thread_labeled_post(self):
        df = self._make_df([(99, "2023-01-01", "only")])
        result = label_roles(df)
        assert result["role"].iloc[0] == "post"

    def test_role_column_added(self):
        df = self._make_df([(1, "2023-01-01", "msg")])
        result = label_roles(df)
        assert "role" in result.columns

    def test_nat_dates_do_not_raise(self):
        """Threads where all dates are NaT (e.g. after CSV round-trip) must not raise KeyError."""
        df = pd.DataFrame({
            "ForumTopicID": [1, 2],
            "PostDate": pd.to_datetime([pd.NaT, "2023-01-01"]),
            "MessageText": ["a", "b"],
        })
        result = label_roles(df)
        assert "role" in result.columns


# ---------------------------------------------------------------------------
# strip_entity_placeholders  (utils/thread_utils.py)
# ---------------------------------------------------------------------------

from utils.thread_utils import strip_entity_placeholders, strip_entity_placeholders_col


class TestStripEntityPlaceholders:
    def test_removes_simple_placeholder(self):
        assert strip_entity_placeholders("hoi [ENTITY_PERSON_1] hoe gaat het") == \
            "hoi hoe gaat het"

    def test_removes_multiword_entity_type(self):
        # 'of' inside WORK_OF_ART must not leak into the text as a Dutch word
        result = strip_entity_placeholders("ik las [ENTITY_WORK_OF_ART_3]")
        assert result == "ik las"
        assert "of" not in result.split()

    def test_case_insensitive(self):
        assert strip_entity_placeholders("[entity_person_1]") == ""

    def test_plain_text_unchanged(self):
        assert strip_entity_placeholders("gewone tekst zonder entiteiten") == \
            "gewone tekst zonder entiteiten"

    def test_bracketed_non_placeholder_kept(self):
        assert strip_entity_placeholders("[quote] blijft staan") == "[quote] blijft staan"

    def test_col_helper_handles_nan(self):
        df = pd.DataFrame({"MessageText": ["[ENTITY_GPE_1] daar", None]})
        result = strip_entity_placeholders_col(df, "MessageText")
        assert result["MessageText"].tolist() == ["daar", ""]


# ---------------------------------------------------------------------------
# parse_post_dates  (utils/thread_utils.py)
# ---------------------------------------------------------------------------

from utils.thread_utils import parse_post_dates


class TestParsePostDates:
    def test_mixed_precision_formats_all_parse(self):
        """The combined export mixes millisecond ('.867'), '.000', and bare
        second-precision timestamps. All must parse — a strict format inferred
        from the first row silently NaT'd the minority format (the combined
        LIWC date-loss bug)."""
        s = pd.Series([
            "2019-06-19 00:26:49.867",   # old export, milliseconds
            "2019-09-02 11:40:52.000",   # new export, .000
            "2022-12-04 13:10:02",       # new export, seconds
        ])
        out = parse_post_dates(s)
        assert out.notna().all()
        assert out.iloc[2].year == 2022 and out.iloc[2].hour == 13

    def test_ms_first_then_seconds_does_not_drop_seconds(self):
        # Direct regression: ms value first, second-precision after.
        out = parse_post_dates(pd.Series(["2019-06-19 00:26:49.867",
                                          "2022-12-04 13:10:02"]))
        assert out.notna().all()

    def test_empty_and_bad_values_become_nat(self):
        out = parse_post_dates(pd.Series(["", "not a date", "2020-01-01 00:00:00"]))
        assert out.isna().tolist() == [True, True, False]


# ---------------------------------------------------------------------------
# add_time_columns  (cds_prevalence.py)
# ---------------------------------------------------------------------------

class TestAddTimeColumns:
    def test_adds_year_column(self):
        df = pd.DataFrame({"PostDate": pd.to_datetime(["2021-06-15", "2022-11-30"])})
        result = add_time_columns(df)
        assert "year" in result.columns
        assert result["year"].iloc[0] == 2021
        assert result["year"].iloc[1] == 2022

    def test_adds_month_dt_column(self):
        df = pd.DataFrame({"PostDate": pd.to_datetime(["2021-06-15"])})
        result = add_time_columns(df)
        assert "month_dt" in result.columns
        assert result["month_dt"].iloc[0].month == 6
        assert result["month_dt"].iloc[0].year == 2021

    def test_original_post_date_unchanged(self):
        dates = pd.to_datetime(["2021-06-15"])
        df = pd.DataFrame({"PostDate": dates})
        result = add_time_columns(df)
        assert result["PostDate"].iloc[0] == dates[0]


# ---------------------------------------------------------------------------
# compute_category_ranking  (cds_prevalence.py)
# ---------------------------------------------------------------------------

class TestComputeCategoryRanking:
    def _make_df(self):
        """DataFrame with two CDS category columns, role, and per-user IDs
        (required since the switch to per-user Mann-Whitney U)."""
        return pd.DataFrame({
            "PosterID": ["u1", "u2", "u3", "u4", "u5"],
            "role": ["post", "reply", "post", "reply", "post"],
            "Catastrophizing": [1, 0, 1, 1, 0],
            "Mindreading":     [0, 1, 0, 0, 1],
        })

    def test_returns_one_row_per_category(self):
        result = compute_category_ranking(self._make_df())
        assert set(result["category"]) == {"Catastrophizing", "Mindreading"}

    def test_overall_prevalence_is_correct(self):
        result = compute_category_ranking(self._make_df())
        row = result[result["category"] == "Catastrophizing"].iloc[0]
        # 3 out of 5 messages → 60%
        assert row["prevalence_pct"] == 60.0

    def test_sorted_descending_by_prevalence(self):
        result = compute_category_ranking(self._make_df())
        assert result["prevalence_pct"].is_monotonic_decreasing

    def test_posts_prevalence_computed_separately(self):
        result = compute_category_ranking(self._make_df())
        row = result[result["category"] == "Catastrophizing"].iloc[0]
        # Posts: [1,1,0] → 2/3 ≈ 66.67%
        assert abs(row["prevalence_posts_pct"] - 66.67) < 0.1

    def test_raises_when_no_cds_columns_present(self):
        df = pd.DataFrame({"role": ["post", "reply"], "MessageText": ["a", "b"]})
        with pytest.raises(ValueError, match="CDS category columns"):
            compute_category_ranking(df)

    def test_match_counts_in_output(self):
        result = compute_category_ranking(self._make_df())
        row = result[result["category"] == "Catastrophizing"].iloc[0]
        assert row["n_matches_total"] == 3
