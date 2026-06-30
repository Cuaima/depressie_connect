# =============================================================================
# integrate_datasets.py  –  build ID bridge + merge old and new forum data
#
# Execution order:
#   1. load_old_raw()          – load raw old messages + group/account structure
#   2. load_new_raw()          – load and normalize new CSV exports
#   3. build_id_bridge()       – map old UUIDs → new integer IDs with confidence
#   4. detect_new_superusers() – recompute behavioral signals on new data
#   5. filter_old_data()       – apply account type 1/4 filter to old data
#   6. filter_new_data()       – apply behavioral superuser filter to new data
#   7. remove_duplicates()     – drop overlapping posts using the ID bridge
#   8. harmonize_schemas()     – align column names across both datasets
#   9. combine_and_save()      – write integrated output files
#
# Output files:
#   output/integrated_messages.csv      – combined cleaned messages
#   output/integrated_topics.csv        – topic titles from new data for review
#   output/id_bridge.csv                – UUID → integer ID mapping with confidence
#   output/new_superuser_exclusions.csv – behavioral superuser exclusion list
#
# Run with:  python src/integrate_datasets.py
# =============================================================================

from __future__ import annotations

import os
import re
import pandas as pd
import numpy as np
from difflib import SequenceMatcher

# ── Directories ───────────────────────────────────────────────────────────────
DATA_DIR     = "data"
NEW_DATA_DIR = "data/new"
OUTPUT_DIR   = "output"

# ── Old data column names ─────────────────────────────────────────────────────
OLD_ID_COL   = "PosterID"
OLD_TEXT_COL = "MessageText"
OLD_DATE_COL = "PostDate"
OLD_TOPIC_COL = "ForumTopicID"

# ── New data column mapping ───────────────────────────────────────────────────
NEW_COL_MAP = {
    "Content":           "MessageText",
    "AuthorID":          "PosterID",
    "PostDate":          "PostDate",
    "PostModifiedDate":  "PostModifiedDate",
    "ForumTopicID":      "ForumTopicID",
    "ForumGroupID":      "ForumGroupID",
    "ForumMessageID":    "ForumMessageID",
    "post_type":         "post_type",
    "Topic_title":       "Topic_title",
}

# ── Account types to exclude from old data ────────────────────────────────────
SUPERUSER_ACCOUNT_IDS = {1, 4}

# ── Behavioral superuser thresholds (new data only) ───────────────────────────
# These are recomputed each run — adjust if needed after reviewing outputs
MIN_POSTS_FOR_DIVERSITY_SIGNAL  = 50   # lexical diversity only meaningful at scale
MIN_POSTS_FOR_RATE_SIGNAL       = 20   # hour_std only meaningful at scale
MIN_POSTS_FOR_THREAD_SIGNAL     = 10   # threads_started_pct only meaningful at scale
OVERLAP_COUNT_THRESHOLD         = 10   # confirmed overlap with known shared accounts
LEXICAL_DIVERSITY_THRESHOLD     = 0.10 # below this = suspiciously repetitive
HOUR_STD_THRESHOLD              = 1.0  # below this = suspiciously regular posting
THREAD_START_PCT_THRESHOLD      = 50   # above this = seeding behavior

# ── ID bridge confidence thresholds ──────────────────────────────────────────
# One old UUID → exactly one new ID = HIGH confidence
# One old UUID → multiple new IDs  = COLLISION, do not link
# One new ID   → many old UUIDs    = SHARED account, exclude if above this count
SHARED_ACCOUNT_THRESHOLD = 15  # new IDs mapping to more old UUIDs than this are excluded


# =============================================================================
# Helpers
# =============================================================================

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_csv(df: pd.DataFrame, filename: str):
    df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)
    print(f"  Wrote {len(df)} rows → {filename}")


def _normalize(text: str) -> str:
    """Normalize text for comparison — strip HTML, punctuation, whitespace."""
    text = str(text).lower().strip()
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&amp;", "&").replace("&nbsp;", " ").replace("&quot;", '"')
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


# =============================================================================
# Step 1: Load old raw data
# =============================================================================

def load_old_raw() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw old messages, topics, and groups."""
    print("\n[1] Loading old raw data…")

    messages = pd.read_csv(
        os.path.join(DATA_DIR, "messages.csv"), on_bad_lines="skip"
    )
    topics = pd.read_csv(
        os.path.join(DATA_DIR, "topics.csv"), on_bad_lines="skip"
    )
    groups = pd.read_csv(
        os.path.join(DATA_DIR, "groups.csv"), on_bad_lines="skip"
    )

    messages[OLD_DATE_COL] = pd.to_datetime(messages[OLD_DATE_COL], errors="coerce")
    messages["source"] = "old"
    messages["PostModifiedDate"] = pd.NaT  # old data has no modified date

    print(f"  Old messages: {len(messages)}, posters: {messages[OLD_ID_COL].nunique()}")
    return messages, topics, groups


# =============================================================================
# Step 2: Load new raw data
# =============================================================================

def load_new_raw() -> pd.DataFrame:
    """Load and normalize all new CSV exports."""
    print("\n[2] Loading new raw data…")

    parts = []
    for fname in sorted(os.listdir(NEW_DATA_DIR)):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(NEW_DATA_DIR, fname)
        df = pd.read_csv(path, sep=";", quotechar='"', on_bad_lines="skip")
        df["source_file"] = fname
        parts.append(df)
        print(f"  Loaded {path}: {len(df)} rows")

    if not parts:
        raise FileNotFoundError(f"No CSV files found in {NEW_DATA_DIR}")

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.rename(columns=NEW_COL_MAP)

    # Drop Group metadata rows
    before = len(combined)
    combined = combined[combined["post_type"] != "Group"].copy()
    print(f"  Dropped {before - len(combined)} 'Group' metadata rows.")

    combined["PostDate"] = pd.to_datetime(combined["PostDate"], errors="coerce")
    combined["PostModifiedDate"] = pd.to_datetime(
        combined["PostModifiedDate"], errors="coerce"
    )
    combined["source"] = "new"
    combined["PosterID"] = combined["PosterID"].astype(str)

    print(f"  New messages: {len(combined)}, posters: {combined['PosterID'].nunique()}")
    return combined


# =============================================================================
# Step 3: Build ID bridge
# =============================================================================

def build_id_bridge(
    old_messages: pd.DataFrame,
    new_messages: pd.DataFrame,
) -> pd.DataFrame:
    """
    Builds a mapping table between old UUIDs and new integer IDs.

    Confidence levels:
      HIGH      – one-to-one mapping, safe to link
      COLLISION – one old UUID maps to multiple new IDs (corrupted old UUID)
      SHARED    – one new ID maps to many old UUIDs (shared/admin account)
    """
    print("\n[3] Building ID bridge…")

    old = old_messages.copy()
    new = new_messages.copy()
    old["_norm"] = old[OLD_TEXT_COL].fillna("").apply(_normalize)
    new["_norm"] = new["MessageText"].fillna("").apply(_normalize)

    # Only match on non-trivial messages (>20 chars normalized)
    old_texts = old[old["_norm"].str.len() > 20][[
        OLD_ID_COL, OLD_DATE_COL, OLD_TOPIC_COL, "_norm"
    ]].rename(columns={OLD_ID_COL: "PosterID_old", OLD_DATE_COL: "PostDate_old",
                        OLD_TOPIC_COL: "ForumTopicID_old"})

    new_texts = new[new["_norm"].str.len() > 20][[
        "PosterID", "PostDate", "ForumTopicID", "_norm"
    ]].rename(columns={"PosterID": "PosterID_new", "PostDate": "PostDate_new",
                        "ForumTopicID": "ForumTopicID_new"})

    exact = old_texts.merge(new_texts, on="_norm", how="inner")
    exact = exact.drop(columns=["_norm"])

    print(f"  Found {len(exact)} exact text matches across datasets.")

    # Build raw mapping
    mapping = exact[["PosterID_old", "PosterID_new"]].drop_duplicates()

    # Count how many new IDs each old UUID maps to
    old_to_new_count = mapping.groupby("PosterID_old")["PosterID_new"].nunique()
    # Count how many old UUIDs each new ID maps to
    new_to_old_count = mapping.groupby("PosterID_new")["PosterID_old"].nunique()

    # Classify each mapping
    def classify(row):
        old_count = old_to_new_count.get(row["PosterID_old"], 1)
        new_count = new_to_old_count.get(row["PosterID_new"], 1)
        if new_count >= SHARED_ACCOUNT_THRESHOLD:
            return "SHARED"
        if old_count > 1:
            return "COLLISION"
        return "HIGH"

    mapping["confidence"] = mapping.apply(classify, axis=1)

    # Summary
    conf_counts = mapping["confidence"].value_counts()
    print(f"  HIGH confidence mappings:  {conf_counts.get('HIGH', 0)}")
    print(f"  COLLISION mappings:        {conf_counts.get('COLLISION', 0)}")
    print(f"  SHARED account mappings:   {conf_counts.get('SHARED', 0)}")

    # Add overlap counts for reference
    overlap_counts = (
        exact.groupby(["PosterID_old", "PosterID_new"])
        .size()
        .reset_index(name="overlap_count")
    )
    mapping = mapping.merge(overlap_counts, on=["PosterID_old", "PosterID_new"], how="left")

    write_csv(mapping, "id_bridge.csv")
    return mapping


# =============================================================================
# Step 4: Detect superusers in new data (behavioral, recomputed each run)
# =============================================================================

def detect_new_superusers(new_messages: pd.DataFrame) -> set:
    """
    Recomputes behavioral superuser signals on new data.
    Returns the set of PosterIDs to exclude.
    """
    print("\n[4] Detecting superusers in new data…")

    new = new_messages.copy()
    new_sorted = new.sort_values("PostDate")
    first_posts = (
        new_sorted.groupby("ForumTopicID")["PosterID"]
        .first()
        .reset_index()
    )
    first_posts.columns = ["ForumTopicID", "thread_starter"]
    thread_starter_counts = first_posts["thread_starter"].value_counts()

    signals = []
    for author in new["PosterID"].dropna().unique():
        author_posts = new[new["PosterID"] == author]["MessageText"].fillna("").astype(str)
        all_words    = " ".join(author_posts).lower().split()

        post_count        = len(author_posts)
        unique_words      = len(set(all_words))
        total_words       = len(all_words)
        lexical_diversity = round(unique_words / max(total_words, 1), 3)
        threads_started   = thread_starter_counts.get(author, 0)

        author_dates = new[new["PosterID"] == author]["PostDate"].dropna()
        hour_std = round(author_dates.dt.hour.std(), 2) if len(author_dates) > 5 else None

        signals.append({
            "PosterID":             author,
            "post_count":           post_count,
            "threads_started":      threads_started,
            "threads_started_pct":  round(threads_started / max(post_count, 1) * 100, 1),
            "lexical_diversity":    lexical_diversity,
            "hour_std":             hour_std,
            "total_words":          total_words,
        })

    signals_df = pd.DataFrame(signals)

    # Recomputed flag — all conditions require minimum post counts to avoid
    # false positives on low-volume users
    signals_df["superuser_flag"] = (
        (
            (signals_df["threads_started_pct"] > THREAD_START_PCT_THRESHOLD) &
            (signals_df["post_count"] > MIN_POSTS_FOR_THREAD_SIGNAL)
        ) |
        (
            (signals_df["lexical_diversity"] < LEXICAL_DIVERSITY_THRESHOLD) &
            (signals_df["post_count"] > MIN_POSTS_FOR_DIVERSITY_SIGNAL)
        ) |
        (
            (signals_df["hour_std"].astype("float64").fillna(99) < HOUR_STD_THRESHOLD) &
            (signals_df["post_count"] > MIN_POSTS_FOR_RATE_SIGNAL)
        )
    )

    flagged = signals_df[signals_df["superuser_flag"]]
    print(f"  Flagged {len(flagged)} / {len(signals_df)} authors as likely superusers.")

    write_csv(signals_df.sort_values("post_count", ascending=False),
              "new_superuser_signals.csv")
    write_csv(flagged, "new_superuser_exclusions.csv")

    return set(flagged["PosterID"].astype(str))


# =============================================================================
# Step 5: Filter old data (account type 1/4)
# =============================================================================

def filter_old_data(
    old_messages: pd.DataFrame,
    topics: pd.DataFrame,
    groups: pd.DataFrame,
) -> pd.DataFrame:
    """Remove posts from account type 1 and 4 forums from old data."""
    print("\n[5] Filtering old data by account type…")

    topic_group = topics[["ForumTopicID", "ForumGroupID"]].merge(
        groups[["ForumGroupID", "AccountID"]], on="ForumGroupID", how="left"
    )
    topic_to_account = dict(zip(
        topic_group["ForumTopicID"].astype(str),
        topic_group["AccountID"]
    ))

    old_messages = old_messages.copy()
    old_messages["_AccountID"] = (
        old_messages["ForumTopicID"].astype(str).map(topic_to_account)
    )

    # Find superuser posters (ever posted in account 1 or 4)
    superuser_posters = set(
        old_messages.loc[
            old_messages["_AccountID"].isin(SUPERUSER_ACCOUNT_IDS),
            OLD_ID_COL
        ].dropna()
    )
    print(f"  Old superuser posters identified: {len(superuser_posters)}")

    before = len(old_messages)
    old_messages = old_messages[
        ~old_messages[OLD_ID_COL].isin(superuser_posters)
    ].drop(columns=["_AccountID"])
    print(f"  Old data after superuser removal: {before} → {len(old_messages)}")

    return old_messages


# =============================================================================
# Step 6: Filter new data (behavioral superusers)
# =============================================================================

def filter_new_data(
    new_messages: pd.DataFrame,
    superuser_ids: set,
    bridge: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove behavioral superusers and shared account IDs from new data.
    Also removes posts that are exact duplicates of old data
    (kept in old data, removed from new to avoid double-counting).
    """
    print("\n[6] Filtering new data…")

    # Also exclude shared account IDs identified in the bridge
    shared_ids = set(
        bridge[bridge["confidence"] == "SHARED"]["PosterID_new"].astype(str)
    )
    all_excluded = superuser_ids | shared_ids
    print(f"  Excluding {len(superuser_ids)} behavioral superusers "
          f"+ {len(shared_ids)} shared accounts = {len(all_excluded)} total.")

    before = len(new_messages)
    new_messages = new_messages[
        ~new_messages["PosterID"].astype(str).isin(all_excluded)
    ].copy()
    print(f"  New data after exclusions: {before} → {len(new_messages)}")

    return new_messages


# =============================================================================
# Step 7: Remove duplicate posts
# =============================================================================

def remove_duplicates(
    old_messages: pd.DataFrame,
    new_messages: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove exact duplicate posts from new data that already exist in old data.
    Old data takes precedence — keep old, drop new duplicates.
    """
    print("\n[7] Removing duplicate posts…")

    old_norms = set(
        old_messages[OLD_TEXT_COL].fillna("").apply(_normalize)
        .loc[lambda s: s.str.len() > 20]
    )

    new_messages = new_messages.copy()
    new_messages["_norm"] = new_messages["MessageText"].fillna("").apply(_normalize)

    before = len(new_messages)
    new_messages = new_messages[
        ~(
            (new_messages["_norm"].isin(old_norms)) &
            (new_messages["_norm"].str.len() > 20)
        )
    ].drop(columns=["_norm"])

    print(f"  Removed {before - len(new_messages)} duplicate posts from new data.")
    return new_messages


# =============================================================================
# Step 8: Harmonize schemas
# =============================================================================

def harmonize_schemas(
    old_messages: pd.DataFrame,
    new_messages: pd.DataFrame,
    bridge: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    - Remap old UUIDs to new integer IDs where confidence=HIGH
    - Align column names
    - Tag each row with its source
    """
    print("\n[8] Harmonizing schemas…")

    # Build UUID → integer ID map (HIGH confidence only)
    high_conf = bridge[bridge["confidence"] == "HIGH"][
        ["PosterID_old", "PosterID_new"]
    ].drop_duplicates("PosterID_old")
    uuid_to_int = dict(zip(
        high_conf["PosterID_old"].astype(str),
        high_conf["PosterID_new"].astype(str)
    ))

    # Remap old UUIDs where we have a HIGH confidence match
    old_messages = old_messages.copy()
    old_messages[OLD_ID_COL] = old_messages[OLD_ID_COL].astype(str).map(
        lambda x: uuid_to_int.get(x, x)  # keep original if no mapping
    )
    print(f"  Remapped {len(uuid_to_int)} old UUIDs to new integer IDs.")

    # Ensure both DataFrames have the same columns
    shared_cols = [
        "PosterID", "ForumTopicID", "ForumGroupID",
        "MessageText", "PostDate", "PostModifiedDate",
        "source"
    ]

    # Old data may not have ForumGroupID directly — add if missing
    if "ForumGroupID" not in old_messages.columns:
        old_messages["ForumGroupID"] = pd.NA

    # New data: add ForumMessageID to old for reference if available
    old_out = old_messages.reindex(
        columns=shared_cols + ["ForumMessageID"] if "ForumMessageID" in old_messages.columns
        else shared_cols
    )
    new_out = new_messages.reindex(
        columns=shared_cols + ["ForumMessageID", "post_type", "source_file"],
        fill_value=pd.NA
    )

    # Extract topics from new data for separate review file
    new_topics = new_messages[
        new_messages["post_type"] == "Topic"
    ][["ForumTopicID", "PosterID", "PostDate", "Topic_title", "MessageText"]].copy()

    return old_out, new_out, new_topics


# =============================================================================
# Step 9: Combine and save
# =============================================================================

def combine_and_save(
    old_messages: pd.DataFrame,
    new_messages: pd.DataFrame,
    new_topics: pd.DataFrame,
):
    print("\n[9] Combining and saving…")

    combined = pd.concat([old_messages, new_messages], ignore_index=True)

    # Final deduplication on ForumMessageID if available
    if "ForumMessageID" in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(
            subset=["ForumMessageID"],
            keep="first"
        )
        print(f"  ForumMessageID dedup: {before} → {len(combined)}")

    combined = combined.sort_values(["ForumTopicID", "PostDate"]).reset_index(drop=True)

    print("\n  Combined dataset:")
    print(f"    Total messages:  {len(combined)}")
    print(f"    Unique posters:  {combined['PosterID'].nunique()}")
    print(f"    Date range:      {combined['PostDate'].min()} → {combined['PostDate'].max()}")
    print(f"    From old data:   {(combined['source'] == 'old').sum()}")
    print(f"    From new data:   {(combined['source'] == 'new').sum()}")

    write_csv(combined,                                    "integrated_messages.csv")
    write_csv(combined[combined["source"] == "old"],       "messages_old.csv")
    write_csv(combined[combined["source"] == "new"],       "messages_new_only.csv")
    write_csv(combined,                                    "messages_combined.csv")
    write_csv(new_topics,                                  "integrated_topics_review.csv")


# =============================================================================
# Main pipeline
# =============================================================================

def run_integration():
    ensure_output_dir()

    # 1. Load
    old_messages, topics, groups = load_old_raw()
    new_messages = load_new_raw()

    # 2. Build ID bridge
    bridge = build_id_bridge(old_messages, new_messages)

    # 3. Detect new superusers (behavioral, recomputed)
    new_superuser_ids = detect_new_superusers(new_messages)

    # 4. Filter old data by account type
    old_filtered = filter_old_data(old_messages, topics, groups)

    # 5. Filter new data by behavioral signals + shared accounts
    new_filtered = filter_new_data(new_messages, new_superuser_ids, bridge)

    # 6. Remove duplicates (keep old, drop new copies)
    new_deduped = remove_duplicates(old_filtered, new_filtered)

    # 7. Harmonize schemas
    old_harmonized, new_harmonized, new_topics = harmonize_schemas(
        old_filtered, new_deduped, bridge
    )

    # 8. Combine and save
    combine_and_save(old_harmonized, new_harmonized, new_topics)

    print("\n✓ Integration complete.")
    print("  Next step: run preprocess.py on integrated_messages.csv")


if __name__ == "__main__":
    run_integration()