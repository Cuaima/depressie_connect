"""
Inspect messages with very low word counts.

Prints short initial posts and short replies for manual review.

Run from project root:
    python scripts/inspect_short_messages.py
    python scripts/inspect_short_messages.py --input output/messages_structured_old.csv
    python scripts/inspect_short_messages.py --threshold 5
"""

import argparse
import pandas as pd

DEFAULT_INPUT = "output/messages_structured.csv"
DEFAULT_THRESHOLD = 3


def main():
    parser = argparse.ArgumentParser(description="Inspect short messages.")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help=f"Path to structured messages CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_THRESHOLD,
        help=f"Word count below which a message is considered short (default: {DEFAULT_THRESHOLD})",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    df["PostDate"] = pd.to_datetime(df["PostDate"], errors="coerce")
    df = df.sort_values(["ForumTopicID", "PostDate"]).reset_index(drop=True)

    if "is_initial_post" not in df.columns:
        df["is_initial_post"] = df.groupby("ForumTopicID").cumcount() == 0

    df["wc"] = df["MessageText"].fillna("").apply(lambda x: len(x.split()))
    short = df[df["wc"] < args.threshold].copy()

    print(f"\n=== SHORT INITIAL POSTS (< {args.threshold} words) ===")
    short_initial = (
        short[short["is_initial_post"]][["ForumTopicID", "MessageText", "wc"]]
        .sort_values("wc")
    )
    print(short_initial.to_string())

    print(f"\n=== SHORT REPLIES (< {args.threshold} words) — first 30 ===")
    short_replies = (
        short[~short["is_initial_post"]][["ForumTopicID", "MessageText", "wc"]]
        .sort_values("wc")
    )
    print(short_replies.head(30).to_string())


if __name__ == "__main__":
    main()
