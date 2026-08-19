"""
Sample messages from a specific user for manual inspection.

Usage:
    python scripts/sample_user_messages.py --user user_1035
    python scripts/sample_user_messages.py --user user_1035 --n 20
    python scripts/sample_user_messages.py --top 3          # sample from top 3 most prolific users
    python scripts/sample_user_messages.py --input output/messages_structured_old.csv --user user_42
"""

import argparse
import pandas as pd

DEFAULT_INPUT = "output/messages_structured.csv"
DATE_COLUMN = "PostDate"
TEXT_COLUMN = "MessageText"
POSTER_COL  = "PosterID"
DEFAULT_N   = 10


def sample_user(df: pd.DataFrame, user_id: str, n: int):
    user_msgs = df[df[POSTER_COL] == user_id].copy()
    if user_msgs.empty:
        print(f"  No messages found for {user_id}.")
        return

    sample = user_msgs.sample(n=min(n, len(user_msgs)), random_state=42)
    print(f"\n{'='*70}")
    print(f"User: {user_id}  |  Total messages: {len(user_msgs)}  |  Showing: {len(sample)}")
    print(f"{'='*70}")
    for _, row in sample.iterrows():
        print(f"\n--- Thread {row.get('ForumTopicID', '?')} | {row.get(DATE_COLUMN, '?')} ---")
        print(str(row[TEXT_COLUMN])[:500])
        if len(str(row[TEXT_COLUMN])) > 500:
            print("  [truncated]")


def main():
    parser = argparse.ArgumentParser(description="Sample messages from a user.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help=f"Path to messages CSV (default: {DEFAULT_INPUT})")
    parser.add_argument("--user",  type=str, help="PosterID to inspect (e.g. user_1035)")
    parser.add_argument("--top",   type=int, help="Inspect top N most prolific users instead")
    parser.add_argument("--n",     type=int, default=DEFAULT_N, help=f"Messages to sample per user (default: {DEFAULT_N})")
    args = parser.parse_args()

    if not args.user and not args.top:
        parser.error("Provide either --user or --top.")

    df = pd.read_csv(args.input)
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce")

    if args.top:
        top_users = df[POSTER_COL].value_counts().head(args.top).index.tolist()
        for user_id in top_users:
            sample_user(df, user_id, args.n)
    else:
        sample_user(df, args.user, args.n)


if __name__ == "__main__":
    main()