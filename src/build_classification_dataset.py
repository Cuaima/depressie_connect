# =============================================================================
# build_classification_dataset.py
#
# Builds a two-class dataset:
#   label="topic"   → the first (oldest) message of each forum thread
#   label="message" → all subsequent replies
#
# Only includes posters from COMMUNITY_ACCOUNT_IDS (2, 3).
# Superusers and intro-group posts are already excluded by the pipeline;
# call preprocess.run_pipeline() first, or pass pre-cleaned DataFrames.
# =============================================================================

import os
import pandas as pd

from config import (
    OUTPUT_DIR, DATA_DIR,
    TEXT_COLUMN, DATE_COLUMN_PRIMARY,
    COMMUNITY_ACCOUNT_IDS,
)


def build_dataset(messages: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Args:
        messages: Pre-cleaned messages DataFrame.  If None, reads
                  the per-account CSVs written by preprocess.run_pipeline().
    """

    if messages is None:
        path = os.path.join(OUTPUT_DIR, "messages_community.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found – run preprocess.run_pipeline() first with COMMUNITY_ACCOUNT_IDS.")
        messages = pd.read_csv(path)
        

    # ── Ensure types ─────────────────────────────────────────────────────────
    messages[DATE_COLUMN_PRIMARY] = pd.to_datetime(
        messages[DATE_COLUMN_PRIMARY], errors="coerce"
    )
    messages = messages.dropna(subset=[TEXT_COLUMN, DATE_COLUMN_PRIMARY]).copy()

    # ── Identify oldest message per topic → label "topic" ────────────────────
    oldest = (
        messages
        .sort_values(DATE_COLUMN_PRIMARY)
        .groupby("ForumTopicID", as_index=False)
        .first()
    )

    topic_df = pd.DataFrame({
        "text":  oldest[TEXT_COLUMN].astype(str),
        "label": "topic",
    })

    # ── Remaining messages → label "message" ─────────────────────────────────
    oldest_index = set(oldest.index)
    message_df = (
        messages
        .drop(index=oldest_index, errors="ignore")
        [[TEXT_COLUMN]]
        .rename(columns={TEXT_COLUMN: "text"})
        .assign(label="message")
    )

    # ── Combine, clean, deduplicate ───────────────────────────────────────────
    dataset = (
        pd.concat([topic_df, message_df], ignore_index=True)
        .dropna(subset=["text"])
    )
    dataset["text"] = dataset["text"].astype(str).str.strip()
    dataset = (
        dataset[dataset["text"] != ""]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # ── Report ────────────────────────────────────────────────────────────────
    print("Classification dataset shape:", dataset.shape)
    print(dataset["label"].value_counts().to_string())

    # ── Save ──────────────────────────────────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "classification_dataset.csv")
    dataset.to_csv(out_path, index=False)
    print(f"Saved → {out_path}")

    return dataset


if __name__ == "__main__":
    build_dataset()
