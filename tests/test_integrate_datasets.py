"""Tests for src/integrate_datasets.py."""

import pandas as pd
import pytest

import integrate_datasets as ids


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_lowercases_text(self):
        assert ids._normalize("Hello World") == "hello world"

    def test_strips_html_tags(self):
        result = ids._normalize("<p>Hello <b>World</b></p>")
        assert "<" not in result
        assert "hello" in result

    def test_decodes_html_entities(self):
        result = ids._normalize("cats &amp; dogs")
        assert "&amp;" not in result
        assert "amp" not in result

    def test_removes_punctuation(self):
        result = ids._normalize("Hello, World!")
        assert "," not in result
        assert "!" not in result

    def test_collapses_whitespace(self):
        result = ids._normalize("hello   world")
        assert "  " not in result

    def test_strips_leading_trailing_whitespace(self):
        result = ids._normalize("  hello  ")
        assert result == result.strip()


# ---------------------------------------------------------------------------
# remove_duplicates
# ---------------------------------------------------------------------------

class TestRemoveDuplicates:
    LONG = "This is a sufficiently long message to be considered for deduplication purposes"

    def test_removes_new_post_matching_old(self):
        old = pd.DataFrame({"MessageText": [self.LONG]})
        new = pd.DataFrame({"MessageText": [self.LONG, "brand new content not in old"]})
        result = ids.remove_duplicates(old, new)
        assert len(result) == 1
        assert "brand new content not in old" in result["MessageText"].values

    def test_keeps_all_new_when_no_overlap(self):
        old = pd.DataFrame({"MessageText": ["completely different content in old"]})
        new = pd.DataFrame({"MessageText": ["new content first message", "new content second one"]})
        result = ids.remove_duplicates(old, new)
        assert len(result) == 2

    def test_short_texts_never_deduplicated(self):
        """Messages that normalize to ≤20 chars are not candidates for dedup."""
        old = pd.DataFrame({"MessageText": ["short"]})
        new = pd.DataFrame({"MessageText": ["short"]})
        result = ids.remove_duplicates(old, new)
        assert len(result) == 1

    def test_preserves_all_columns_in_new(self):
        old = pd.DataFrame({"MessageText": [self.LONG]})
        new = pd.DataFrame({
            "MessageText": ["only new content here not matching anything"],
            "PosterID": ["user_1"],
        })
        result = ids.remove_duplicates(old, new)
        assert "PosterID" in result.columns


# ---------------------------------------------------------------------------
# build_id_bridge
# ---------------------------------------------------------------------------

class TestBuildIdBridge:
    LONG_TEXT = "This is a sufficiently long message used to match between old and new datasets"

    def test_high_confidence_one_to_one_mapping(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        old = pd.DataFrame({
            "PosterID": ["uuid-1"],
            "PostDate": pd.to_datetime(["2020-01-01"]),
            "ForumTopicID": [1],
            "MessageText": [self.LONG_TEXT],
        })
        new = pd.DataFrame({
            "PosterID": ["int-1"],
            "PostDate": pd.to_datetime(["2020-01-01"]),
            "ForumTopicID": [1],
            "MessageText": [self.LONG_TEXT],
        })
        bridge = ids.build_id_bridge(old, new)
        assert len(bridge) == 1
        assert bridge.iloc[0]["confidence"] == "HIGH"
        assert bridge.iloc[0]["PosterID_old"] == "uuid-1"
        assert bridge.iloc[0]["PosterID_new"] == "int-1"

    def test_writes_id_bridge_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        old = pd.DataFrame({
            "PosterID": ["uuid-1"],
            "PostDate": pd.to_datetime(["2020-01-01"]),
            "ForumTopicID": [1],
            "MessageText": [self.LONG_TEXT],
        })
        new = pd.DataFrame({
            "PosterID": ["int-1"],
            "PostDate": pd.to_datetime(["2020-01-01"]),
            "ForumTopicID": [1],
            "MessageText": [self.LONG_TEXT],
        })
        ids.build_id_bridge(old, new)
        assert (tmp_path / "id_bridge.csv").exists()

    def test_collision_when_one_old_id_maps_to_multiple_new_ids(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        text_a = "First long message that will be matched between the old and new datasets"
        text_b = "Second long message also matched between old and new datasets here"
        old = pd.DataFrame({
            "PosterID": ["uuid-A", "uuid-A"],
            "PostDate": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "ForumTopicID": [1, 1],
            "MessageText": [text_a, text_b],
        })
        new = pd.DataFrame({
            "PosterID": ["int-1", "int-2"],
            "PostDate": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "ForumTopicID": [1, 1],
            "MessageText": [text_a, text_b],
        })
        bridge = ids.build_id_bridge(old, new)
        assert "COLLISION" in bridge["confidence"].values

    def test_no_bridge_when_no_matching_texts(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        old = pd.DataFrame({
            "PosterID": ["uuid-1"],
            "PostDate": pd.to_datetime(["2020-01-01"]),
            "ForumTopicID": [1],
            "MessageText": ["this old message has no match at all in new data"],
        })
        new = pd.DataFrame({
            "PosterID": ["int-1"],
            "PostDate": pd.to_datetime(["2020-01-01"]),
            "ForumTopicID": [1],
            "MessageText": ["this new message has no match at all in old data"],
        })
        bridge = ids.build_id_bridge(old, new)
        assert len(bridge) == 0


# ---------------------------------------------------------------------------
# detect_new_superusers
# ---------------------------------------------------------------------------

class TestDetectNewSuperusers:
    def _make_df(self, rows):
        df = pd.DataFrame(rows)
        df["PostDate"] = pd.to_datetime(df["PostDate"])
        return df

    def test_high_thread_start_pct_flagged(self, tmp_path, monkeypatch):
        """A user who starts >50% of threads and has >10 posts should be flagged."""
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        rows = (
            # superuser starts 15 threads (100% thread start pct, 15 posts)
            [{"PosterID": "super", "ForumTopicID": i, "PostDate": "2020-01-01",
              "MessageText": f"message {i}"} for i in range(15)]
            # normal user replies to those threads
            + [{"PosterID": "normal", "ForumTopicID": i, "PostDate": "2020-01-02",
                "MessageText": f"reply {i}"} for i in range(15)]
        )
        flagged = ids.detect_new_superusers(self._make_df(rows))
        assert "super" in flagged

    def test_low_volume_user_not_flagged(self, tmp_path, monkeypatch):
        """A user with fewer posts than the minimum threshold should never be flagged."""
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        rows = [
            {"PosterID": "user1", "ForumTopicID": i % 3,
             "PostDate": f"2020-01-{(i % 28) + 1:02d}",
             "MessageText": f"unique words every single time message number {i}"}
            for i in range(5)   # well below all minimum thresholds
        ]
        flagged = ids.detect_new_superusers(self._make_df(rows))
        assert "user1" not in flagged

    def test_writes_signals_csv(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ids, "OUTPUT_DIR", str(tmp_path))
        rows = [{"PosterID": "u", "ForumTopicID": 1,
                 "PostDate": "2020-01-01", "MessageText": "hello"}]
        ids.detect_new_superusers(self._make_df(rows))
        assert (tmp_path / "new_superuser_signals.csv").exists()


# ---------------------------------------------------------------------------
# filter_new_data
# ---------------------------------------------------------------------------

class TestFilterNewData:
    def test_removes_behavioral_superusers(self):
        new = pd.DataFrame({
            "PosterID": ["super", "normal", "normal"],
            "MessageText": ["a", "b", "c"],
        })
        bridge = pd.DataFrame(columns=["PosterID_new", "confidence"])
        result = ids.filter_new_data(new, superuser_ids={"super"}, bridge=bridge)
        assert "super" not in result["PosterID"].values
        assert len(result) == 2

    def test_removes_shared_account_ids_from_bridge(self):
        new = pd.DataFrame({
            "PosterID": ["shared", "regular"],
            "MessageText": ["a", "b"],
        })
        bridge = pd.DataFrame({
            "PosterID_new": ["shared"],
            "confidence": ["SHARED"],
        })
        result = ids.filter_new_data(new, superuser_ids=set(), bridge=bridge)
        assert "shared" not in result["PosterID"].values

    def test_keeps_clean_users(self):
        new = pd.DataFrame({"PosterID": ["u1", "u2"], "MessageText": ["a", "b"]})
        bridge = pd.DataFrame(columns=["PosterID_new", "confidence"])
        result = ids.filter_new_data(new, superuser_ids=set(), bridge=bridge)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# harmonize_schemas
# ---------------------------------------------------------------------------

class TestHarmonizeSchemas:
    def _old_df(self):
        return pd.DataFrame({
            "PosterID": ["uuid-1", "uuid-2"],
            "ForumTopicID": [1, 2],
            "ForumGroupID": [10, 20],
            "MessageText": ["hello old", "world old"],
            "PostDate": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "PostModifiedDate": [pd.NaT, pd.NaT],
            "source": ["old", "old"],
        })

    def _new_df(self):
        return pd.DataFrame({
            "PosterID": ["int-99"],
            "ForumTopicID": [3],
            "ForumGroupID": [30],
            "MessageText": ["new message"],
            "PostDate": pd.to_datetime(["2020-01-03"]),
            "PostModifiedDate": [pd.NaT],
            "source": ["new"],
            "ForumMessageID": [101],
            "post_type": ["Reply"],
            "source_file": ["file.csv"],
            "Topic_title": [pd.NA],
        })

    def test_remaps_high_confidence_uuid_to_integer_id(self):
        bridge = pd.DataFrame({
            "PosterID_old": ["uuid-1"],
            "PosterID_new": ["int-1"],
            "confidence": ["HIGH"],
        })
        old_out, _, _ = ids.harmonize_schemas(self._old_df(), self._new_df(), bridge)
        assert "uuid-1" not in old_out["PosterID"].values
        assert "int-1" in old_out["PosterID"].values

    def test_keeps_unmapped_uuid_unchanged(self):
        bridge = pd.DataFrame({
            "PosterID_old": ["uuid-1"],
            "PosterID_new": ["int-1"],
            "confidence": ["HIGH"],
        })
        old_out, _, _ = ids.harmonize_schemas(self._old_df(), self._new_df(), bridge)
        # uuid-2 has no mapping → should remain as-is
        assert "uuid-2" in old_out["PosterID"].values

    def test_collision_mapping_not_applied(self):
        bridge = pd.DataFrame({
            "PosterID_old": ["uuid-1"],
            "PosterID_new": ["int-1"],
            "confidence": ["COLLISION"],
        })
        old_out, _, _ = ids.harmonize_schemas(self._old_df(), self._new_df(), bridge)
        # COLLISION mappings must not remap IDs
        assert "uuid-1" in old_out["PosterID"].values

    def test_extracts_new_topics(self):
        new = self._new_df().copy()
        new["post_type"] = "Topic"
        new["Topic_title"] = "My thread title"
        bridge = pd.DataFrame(columns=["PosterID_old", "PosterID_new", "confidence"])
        _, _, topics = ids.harmonize_schemas(self._old_df(), new, bridge)
        assert len(topics) == 1
        assert "Topic_title" in topics.columns
