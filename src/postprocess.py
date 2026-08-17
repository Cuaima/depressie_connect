# =============================================================================
# postprocess.py  –  thread structure → filter → normalize
#
# Run this AFTER preprocess.py has produced cleaned output. This script picks
# up from messages_community.csv and adds:
#
#   1. load_cleaned_data()         – load messages_community.csv
#   2. filter_intro_groups()       – drop welcome / off-topic threads
#   2b. filter_min_posts()         – drop users below MIN_POSTS_PER_USER threshold
#   3. build_thread_structure()    – flag initial posts vs replies
#   4. label_thread_success()      – threads with 0 replies = negative class
#   5. normalize_text()            – lowercase, whitespace, repeated chars
#   6. sanity_check_lengths()      – warn on suspiciously short messages
#   7. save_outputs()              – write messages_structured.csv
#
# Input:  output/preprocessed/messages_community[_dataset].csv
# Output: output/messages_structured[_dataset].csv
# =============================================================================

from __future__ import annotations

import os
import re
import pandas as pd

from config import PREPROCESS_DIR, OUTPUT_DIR, INTRO_GROUP_KEYWORDS, MIN_POSTS_PER_USER

# ── Config ────────────────────────────────────────────────────────────────────
TEXT_COLUMN = "MessageText"
DATE_COLUMN = "PostDate"


def get_input_path(dataset: str | None = None) -> str:
    suffix = f"_{dataset}" if dataset and dataset != "combined" else ""
    return os.path.join(PREPROCESS_DIR, f"messages_community{suffix}.csv")


def get_output_name(dataset: str | None = None) -> str:
    suffix = f"_{dataset}" if dataset and dataset != "combined" else ""
    return f"messages_structured{suffix}.csv"


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"  Saved: {path}")


# ── Step 1: Load cleaned data ─────────────────────────────────────────────────

def load_cleaned_data(dataset: str | None = None) -> pd.DataFrame:
    input_file = get_input_path(dataset)
    print(f"\n[1] Loading cleaned data from {input_file}...")
    df = pd.read_csv(input_file)
    print(f"  Loaded {len(df)} messages, {df['ForumTopicID'].nunique()} threads.")

    if DATE_COLUMN in df.columns:
        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce")

    return df


# ── Step 2: Filter intro / welcome / off-topic groups ────────────────────────

def filter_intro_groups(
    messages: pd.DataFrame,
    keywords: set = INTRO_GROUP_KEYWORDS,
) -> pd.DataFrame:
    print("\n[2] Filtering intro / welcome / off-topic groups...")

    if "GroupName" not in messages.columns or messages["GroupName"].fillna("").eq("").all():
        print("  SKIP: 'GroupName' column not found or empty – re-run preprocess.py to populate it.")
        return messages

    mask = messages["GroupName"].fillna("").str.lower().apply(
        lambda g: any(kw in g for kw in keywords)
    )
    intro_thread_ids = messages.loc[mask, "ForumTopicID"].unique()

    before = messages["ForumTopicID"].nunique()
    messages = messages[~messages["ForumTopicID"].isin(intro_thread_ids)].copy()
    print(
        f"  Removed {before - messages['ForumTopicID'].nunique()} intro/welcome/off-topic threads – "
        f"{messages['ForumTopicID'].nunique()} threads remain."
    )
    return messages


# ── Step 2b: Filter low-activity users ───────────────────────────────────────

def filter_min_posts(
    messages: pd.DataFrame,
    min_posts: int = MIN_POSTS_PER_USER,
) -> pd.DataFrame:
    print(f"\n[2b] Filtering users with fewer than {min_posts} posts...")

    posts_per_user = messages.groupby("PosterID").size()
    active_users   = set(posts_per_user[posts_per_user >= min_posts].index)

    before = messages["PosterID"].nunique()
    messages = messages[messages["PosterID"].isin(active_users)].copy()
    after = messages["PosterID"].nunique()
    print(
        f"  Removed {before - after} users with fewer than {min_posts} posts – "
        f"{after} users remain ({len(messages)} messages)."
    )
    return messages


# ── Step 3: Build thread structure ───────────────────────────────────────────

def build_thread_structure(messages: pd.DataFrame) -> pd.DataFrame:
    print("\n[3] Building thread structure...")

    if DATE_COLUMN not in messages.columns:
        raise ValueError(
            f"Date column '{DATE_COLUMN}' not found. "
            f"Available columns: {list(messages.columns)}"
        )

    messages = messages.sort_values(["ForumTopicID", DATE_COLUMN]).reset_index(drop=True)
    messages["reply_index"]     = messages.groupby("ForumTopicID").cumcount()
    messages["is_initial_post"] = messages["reply_index"] == 0

    n_threads  = messages["ForumTopicID"].nunique()
    n_initial  = messages["is_initial_post"].sum()
    n_replies  = (~messages["is_initial_post"]).sum()
    print(f"  {n_threads} threads: {n_initial} initial posts, {n_replies} replies.")

    return messages


# ── Step 4: Label thread success ─────────────────────────────────────────────

def label_thread_success(messages: pd.DataFrame) -> pd.DataFrame:
    """
    Threads WITH replies  → thread_has_replies = True  (positive class candidate)
    Threads WITHOUT replies → thread_has_replies = False (negative class)

    Receiving peer replies is the forum's mechanism of support: on Depression
    Connect specifically, engagement is associated with recovery-related
    empowerment (docs/studies/Smit quant evaluatie DC.pdf); what drives
    support acquisition in online health communities is studied in
    docs/studies/lib_intr-03-2021-0189.pdf.

    The final supportiveness label is assigned after manual annotation.
    """
    print("\n[4] Labeling thread success...")

    reply_counts = (
        messages[~messages["is_initial_post"]]
        .groupby("ForumTopicID")
        .size()
        .reset_index(name="reply_count")
    )

    messages = messages.merge(reply_counts, on="ForumTopicID", how="left")
    messages["reply_count"]        = messages["reply_count"].fillna(0).astype(int)
    messages["thread_has_replies"] = messages["reply_count"] > 0

    no_reply  = (~messages["thread_has_replies"] & messages["is_initial_post"]).sum()
    has_reply = ( messages["thread_has_replies"] & messages["is_initial_post"]).sum()
    print(f"  Threads with replies:    {has_reply} (positive class candidates)")
    print(f"  Threads without replies: {no_reply} (negative class)")

    return messages


# ── Step 5: Normalize text ────────────────────────────────────────────────────

def _normalize_dutch_text(text: str) -> str:
    """
    Light normalization for Dutch NLP / LIWC feature extraction.
    Preserves punctuation and sentence boundaries; normalizes whitespace
    and pathological character repetition.
    """
    text = str(text).lower()
    text = re.sub(r"\[entity_[a-z_]+_\d+\]", " ", text)  # drop anonymization placeholders
    text = re.sub(r"(.)\1{3,}", r"\1\1", text)   # 4+ repeated chars → 2
    text = re.sub(r"[ \t]+", " ", text)           # collapse horizontal whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)        # max two consecutive newlines
    return text.strip()


def normalize_text(messages: pd.DataFrame) -> pd.DataFrame:
    print("\n[5] Normalizing text...")

    if TEXT_COLUMN not in messages.columns:
        print(f"  SKIP: column '{TEXT_COLUMN}' not found.")
        return messages

    messages = messages.copy()
    messages["text_normalized"] = (
        messages[TEXT_COLUMN].fillna("").apply(_normalize_dutch_text)
    )
    print("  Done → column 'text_normalized' added.")
    return messages


# ── Step 6: Sanity check ──────────────────────────────────────────────────────

def sanity_check_lengths(messages: pd.DataFrame) -> None:
    print("\n[6] Sanity checking message lengths...")

    wc = messages["text_normalized"].fillna("").apply(lambda x: len(x.split()))

    for label, subset_mask in [
        ("Initial posts", messages["is_initial_post"]),
        ("Replies",       ~messages["is_initial_post"]),
    ]:
        q = wc[subset_mask].quantile([0.05, 0.25, 0.5, 0.75, 0.95])
        print(
            f"  {label}: median={q[0.5]:.0f} words, "
            f"5th pct={q[0.05]:.0f}, 95th pct={q[0.95]:.0f}"
        )

    very_short = (wc < 3).sum()
    if very_short:
        print(f"  WARNING: {very_short} messages have fewer than 3 words.")


# ── Step 7: Save structured dataset ──────────────────────────────────────────

def save_outputs(messages: pd.DataFrame, dataset: str | None = None):
    print("\n[7] Saving structured dataset...")

    messages_clean = messages.drop(columns=["GroupName"], errors="ignore")
    output_name = get_output_name(dataset)
    write_csv(messages_clean, output_name)
    print(
        f"  {output_name}: {len(messages_clean)} messages, "
        f"{messages_clean['ForumTopicID'].nunique()} threads."
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dataset: str | None = None):
    """
    dataset: "old", "new_only", "combined", or None (default).
    Reads messages_community{_dataset}.csv and writes messages_structured{_dataset}.csv.
    """
    ensure_output_dir()

    messages = load_cleaned_data(dataset)
    messages = filter_intro_groups(messages)
    messages = filter_min_posts(messages)
    messages = build_thread_structure(messages)
    messages = label_thread_success(messages)
    messages = normalize_text(messages)
    sanity_check_lengths(messages)
    save_outputs(messages, dataset)

    print("\n✓ Postprocessing complete.")
    print(f"  Next step: run liwc_extractor.py on {get_output_name(dataset)}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        choices=["old", "new_only", "combined"],
        default=None,
        help="Which preprocessed dataset to process. Omit to use messages_community.csv directly.",
    )
    args = parser.parse_args()
    run(dataset=args.dataset)
