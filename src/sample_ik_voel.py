# =============================================================================
# sample_ik_voel.py  –  extract a sample from "ik voel me vandaag" groups
#
# Reads from cleaned output files (run preprocess.py first).
# Writes:  output/sample_ik_voel_me_vandaag.csv
#
# Run with:  python src/sample_ik_voel.py
# =============================================================================

import os
import pandas as pd

DATA_DIR   = "data"
OUTPUT_DIR = "output"

MATCH_PHRASE = "voel me vandaag"
EXCLUDE_ACCOUNT_IDS = {1, 4}


def main():
    # ── Load cleaned files ────────────────────────────────────────────────────
    groups  = pd.read_csv(os.path.join(OUTPUT_DIR, "groups_cleaned.csv"))
    topics  = pd.read_csv(os.path.join(OUTPUT_DIR, "topics_cleaned.csv"))
    messages = pd.read_csv(os.path.join(OUTPUT_DIR, "messages_community.csv"))

# AFTER
    # ── Normalize AccountID for filtering ─────────────────────────────────────
    groups["AccountID"] = pd.to_numeric(groups["AccountID"], errors="coerce")

    # ── Find matching groups, excluding test and demo accounts ────────────────
    matching_groups = groups[
        groups["Name"].str.lower().str.replace(r"[^\w\s]", " ", regex=True).str.contains(MATCH_PHRASE, na=False) &
        ~groups["AccountID"].isin(EXCLUDE_ACCOUNT_IDS)
    ]
    print(f"Found {len(matching_groups)} matching groups:")
    print(matching_groups[["ForumGroupID", "AccountID", "Name"]].to_string(index=False))

    # ── Get topics belonging to those groups ──────────────────────────────────
    group_ids      = set(matching_groups["ForumGroupID"].astype(str))
    matching_topics = topics[
        topics["ForumGroupID"].astype(str).isin(group_ids)
    ][["ForumTopicID", "ForumGroupID", "Name"]].copy()
    matching_topics = matching_topics.rename(columns={"Name": "topic_title"})

    print(f"\nFound {len(matching_topics)} topics in those groups.")

    topic_ids = set(matching_topics["ForumTopicID"].astype(str))

    # ── Filter messages to those topics ──────────────────────────────────────
    thread_messages = messages[
        messages["ForumTopicID"].astype(str).isin(topic_ids)
    ].copy()

    thread_messages = thread_messages.sort_values(
        ["ForumTopicID", "PostDate"]
    )

    # ── Label: first message per topic = 'post', rest = 'reply' ──────────────
    opening_idx = thread_messages.groupby("ForumTopicID")["PostDate"].idxmin()
    thread_messages["label"] = "reply"
    thread_messages.loc[opening_idx, "label"] = "post"

    print(f"Opening posts: {(thread_messages['label'] == 'post').sum()}")
    print(f"Replies:       {(thread_messages['label'] == 'reply').sum()}")

    # ── Assemble final dataset ────────────────────────────────────────────────
    dataset = thread_messages[[
        "ForumTopicID",
        "PosterID",
        "PostDate",
        "MessageText",
        "label",
    ]].rename(columns={
        "PosterID":    "poster",
        "PostDate":    "date",
        "MessageText": "text",
    })

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = os.path.join(OUTPUT_DIR, "sample_ik_voel_me_vandaag.csv")
    dataset.to_csv(out_path, index=False)
    print(f"\nSaved {len(dataset)} rows → {out_path}")


if __name__ == "__main__":
    main()