# =============================================================================
# export_to_excel.py  –  export anonymized forum data to Excel
#
# Produces one .xlsx file per dataset variant, each with two sheets:
#
#   Messages  –  one row per message (PosterID anonymized, text NER-cleaned)
#   Topics    –  one row per thread: opener, dates, reply count, opening text
#
# Output:
#   output/export/forum_export_old.xlsx
#   output/export/forum_export_new_only.xlsx
#   output/export/forum_export.xlsx        ← combined (no suffix)
#
# Run with:
#   python scripts/export_to_excel.py                    (combined only)
#   python scripts/export_to_excel.py --dataset old
#   python scripts/export_to_excel.py --all
#
#   Or via Make:
#   make export                (combined)
#   make export-all            (all three variants)
# =============================================================================

from __future__ import annotations

import os
import sys
import argparse

import pandas as pd

# Allow running directly without PYTHONPATH=src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dataset_io import add_dataset_arg, structured_path, variant_path, DATASET_CHOICES

OUTPUT_DIR = "output"
EXPORT_DIR = os.path.join(OUTPUT_DIR, "export")

POSTER_COL = "PosterID"
TEXT_COL   = "MessageText"
DATE_COL   = "PostDate"
TOPIC_COL  = "ForumTopicID"


# =============================================================================
# Topic summary
# =============================================================================

def build_topic_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse message-level data to one row per thread.

    Columns produced:
      ForumTopicID, first_post_date, last_post_date, opener_poster_id,
      message_count, reply_count, thread_has_replies, opening_post_text
    """
    stats = (
        df.groupby(TOPIC_COL)
        .agg(
            message_count=(TEXT_COL,  "count"),
            first_post_date=(DATE_COL, "min"),
            last_post_date=(DATE_COL,  "max"),
        )
        .reset_index()
    )

    opener_src_cols = [TOPIC_COL, POSTER_COL, TEXT_COL,
                       "thread_has_replies", "reply_count"]
    opener_src_cols = [c for c in opener_src_cols if c in df.columns]

    openers = (
        df[df["is_initial_post"]][opener_src_cols]
        .rename(columns={
            POSTER_COL: "opener_poster_id",
            TEXT_COL:   "opening_post_text",
        })
    )

    topics = stats.merge(openers, on=TOPIC_COL, how="left")

    col_order = [
        TOPIC_COL, "first_post_date", "last_post_date",
        "opener_poster_id", "message_count", "reply_count",
        "thread_has_replies", "opening_post_text",
    ]
    topics = topics[[c for c in col_order if c in topics.columns]]
    return topics.sort_values("first_post_date").reset_index(drop=True)


# =============================================================================
# Export
# =============================================================================

def export_dataset(dataset: str) -> str:
    input_path  = structured_path(OUTPUT_DIR, dataset)
    output_path = variant_path(EXPORT_DIR, "forum_export.xlsx", dataset)

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Structured messages not found: {input_path}\n"
            "Run 'make pipeline' (or 'make pipeline-all') first."
        )

    print(f"\nLoading {input_path}…")
    df = pd.read_csv(input_path)
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    print(f"  {len(df):,} messages  ·  "
          f"{df[POSTER_COL].nunique():,} users  ·  "
          f"{df[TOPIC_COL].nunique():,} threads")

    print("  Building topic summary…")
    topics = build_topic_summary(df)

    os.makedirs(EXPORT_DIR, exist_ok=True)
    print(f"  Writing → {output_path}")
    with pd.ExcelWriter(
        output_path,
        engine="openpyxl",
        datetime_format="YYYY-MM-DD HH:MM:SS",
    ) as writer:
        df.to_excel(writer, sheet_name="Messages", index=False)
        topics.to_excel(writer, sheet_name="Topics", index=False)

    size_kb = os.path.getsize(output_path) // 1024
    print(f"  ✓ Saved ({size_kb:,} KB)")
    print(f"    Sheet 'Messages' : {len(df):,} rows")
    print(f"    Sheet 'Topics'   : {len(topics):,} rows")
    return output_path


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export anonymized forum messages and thread topics to Excel. "
            "One .xlsx per dataset variant with two sheets: Messages and Topics."
        )
    )
    add_dataset_arg(parser)
    parser.add_argument(
        "--all", dest="all_variants", action="store_true",
        help="Export all three variants (old, new_only, combined) in one run.",
    )
    args = parser.parse_args()

    datasets = DATASET_CHOICES if args.all_variants else [args.dataset]
    for ds in datasets:
        export_dataset(ds)

    print("\n✓ Done.")
    print(f"  Files written to {EXPORT_DIR}/")


if __name__ == "__main__":
    main()
