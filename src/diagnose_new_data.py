# =============================================================================
# diagnose_new_data.py  –  understand overlap between old and new datasets
#
# Run BEFORE any integration. This script is read-only — it writes nothing
# to your output folder, only prints findings and saves a report CSV.
#
# Run with:  python src/diagnose_new_data.py
# =============================================================================

import os
import re
import pandas as pd
from difflib import SequenceMatcher

DATA_DIR   = "data"
OUTPUT_DIR = "output"
NEW_DATA_DIR = "data/new"  # ← put your 4 new CSVs here

# ── Column name mapping for new data ─────────────────────────────────────────
# Maps new column names to internal standard names
NEW_COL_MAP = {
    "Content":        "MessageText",
    "AuthorID":       "PosterID",
    "PostDate":       "PostDate",
    "ForumTopicID":   "ForumTopicID",
    "ForumGroupID":   "ForumGroupID",
    "ForumMessageID": "ForumMessageID",
    "post_type":      "post_type",
    "Topic_title":    "Topic_title",
}


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_old_messages() -> pd.DataFrame:
    path = os.path.join(DATA_DIR, "messages.csv")  # raw file, before cleaning
    df = pd.read_csv(path, on_bad_lines="skip")
    df["PostDate"] = pd.to_datetime(df["PostDate"], errors="coerce")
    df["source"] = "old"
    return df


def load_new_data() -> pd.DataFrame:
    """Load and concatenate all 4 new CSV files."""
    parts = []
    for fname in os.listdir(NEW_DATA_DIR):
        if fname.endswith(".csv"):
            path = os.path.join(NEW_DATA_DIR, fname)
            print(f"  Loading {path}")
            df = pd.read_csv(path, sep=";", quotechar='"', on_bad_lines="warn")
            df["source_file"] = fname
            parts.append(df)

    if not parts:
        raise FileNotFoundError(f"No CSV files found in {NEW_DATA_DIR}")

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.rename(columns=NEW_COL_MAP)
    combined["PostDate"] = pd.to_datetime(combined["PostDate"], errors="coerce")
    combined["source"] = "new"
    return combined


# ── Text normalization for comparison ─────────────────────────────────────────

def _normalize(text: str) -> str:
    """Lowercase, strip whitespace and punctuation for comparison."""
    text = str(text).lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text


# ── Overlap detection ─────────────────────────────────────────────────────────

def find_exact_overlaps(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Find messages with identical normalized text in both datasets."""
    old = old.copy()
    new = new.copy()
    old["_norm"] = old["MessageText"].fillna("").apply(_normalize)
    new["_norm"] = new["MessageText"].fillna("").apply(_normalize)

    # Only compare non-empty texts
    old_texts = old[old["_norm"].str.len() > 20][["PosterID", "PostDate", "_norm", "ForumTopicID"]]
    new_texts = new[new["_norm"].str.len() > 20][["PosterID", "PostDate", "_norm", "ForumTopicID", "source_file"]]

    merged = old_texts.merge(new_texts, on="_norm", suffixes=("_old", "_new"))
    return merged.drop(columns=["_norm"])


def find_near_overlaps(
    old: pd.DataFrame,
    new: pd.DataFrame,
    sample_size: int = 500,
    threshold: float = 0.85,
) -> pd.DataFrame:
    """
    Sample-based near-duplicate detection using SequenceMatcher.
    Only runs on a sample to keep runtime manageable.
    """
    old_sample = old[old["MessageText"].fillna("").str.split().str.len() > 10].sample(
        min(sample_size, len(old)), random_state=42
    )
    new_sample = new[new["MessageText"].fillna("").str.split().str.len() > 10].sample(
        min(sample_size, len(new)), random_state=42
    )

    rows = []
    for _, o_row in old_sample.iterrows():
        o_text = _normalize(o_row["MessageText"])
        for _, n_row in new_sample.iterrows():
            n_text = _normalize(n_row["MessageText"])
            ratio = SequenceMatcher(None, o_text, n_text).ratio()
            if ratio >= threshold:
                rows.append({
                    "similarity":       round(ratio, 3),
                    "old_PosterID":     o_row["PosterID"],
                    "new_PosterID":     n_row["PosterID"],
                    "old_PostDate":     o_row["PostDate"],
                    "new_PostDate":     n_row["PostDate"],
                    "old_text_snippet": str(o_row["MessageText"])[:100],
                    "new_text_snippet": str(n_row["MessageText"])[:100],
                })
    
    if not rows:
        print("  No near-duplicates found in sample.")
        return pd.DataFrame(columns=[
            "similarity", "old_PosterID", "new_PosterID",
            "old_PostDate", "new_PostDate",
            "old_text_snippet", "new_text_snippet"
        ])

    return pd.DataFrame(rows).sort_values("similarity", ascending=False)


# ── Superuser signals in new data ─────────────────────────────────────────────

def superuser_signals(new: pd.DataFrame, exact_overlaps: pd.DataFrame) -> pd.DataFrame:
    """
    Flags authors in new data who show superuser-like behavior:
      - appear in many exact-overlap posts
      - seed disproportionate number of threads (first post in many topics)
      - very low lexical diversity
      - abnormally uniform posting times
    """
    signals = []

    authors = new["PosterID"].dropna().unique()

    # Pre-compute first posts per topic
    new_sorted = new.sort_values("PostDate")
    first_posts = new_sorted.groupby("ForumTopicID")["PosterID"].first().reset_index()
    first_posts.columns = ["ForumTopicID", "thread_starter"]
    thread_starter_counts = first_posts["thread_starter"].value_counts()

    # Authors appearing in exact overlaps
    overlap_authors = (
        exact_overlaps["PosterID_new"].value_counts()
        if "PosterID_new" in exact_overlaps.columns
        else pd.Series(dtype=int)
    )

    for author in authors:
        author_posts = new[new["PosterID"] == author]["MessageText"].fillna("").astype(str)
        all_words = " ".join(author_posts).lower().split()

        post_count       = len(author_posts)
        unique_words     = len(set(all_words))
        total_words      = len(all_words)
        lexical_diversity = round(unique_words / max(total_words, 1), 3)
        threads_started  = thread_starter_counts.get(author, 0)
        overlap_count    = overlap_authors.get(author, 0)

        # Posting hour std — low std = suspiciously regular posting times
        author_dates = new[new["PosterID"] == author]["PostDate"].dropna()
        hour_std = round(author_dates.dt.hour.std(), 2) if len(author_dates) > 5 else None

        signals.append({
            "PosterID":          author,
            "post_count":        post_count,
            "threads_started":   threads_started,
            "threads_started_pct": round(threads_started / max(post_count, 1) * 100, 1),
            "lexical_diversity": lexical_diversity,
            "hour_std":          hour_std,
            "overlap_count":     overlap_count,
        })

    df = pd.DataFrame(signals).sort_values("overlap_count", ascending=False)

    # Flag likely superusers — adjust thresholds after reviewing output
    df["superuser_flag"] = (
        (df["overlap_count"] > 5) |
        (df["threads_started_pct"] > 50) |
        (df["lexical_diversity"] < 0.1) |
        (df["hour_std"].fillna(99) < 1.0)
    )

    return df


# ── Summary report ────────────────────────────────────────────────────────────

def print_report(old, new, exact, near, signals):
    sep = "\n" + "─" * 60

    print(sep)
    print("OLD DATA SHAPE")
    print(f"  Messages: {len(old)}")
    print(f"  Unique posters: {old['PosterID'].nunique()}")
    print(f"  Date range: {old['PostDate'].min()} → {old['PostDate'].max()}")

    print(sep)
    print("NEW DATA SHAPE")
    print(f"  Messages: {len(new)}")
    print(f"  Unique authors: {new['PosterID'].nunique()}")
    print(f"  Date range: {new['PostDate'].min()} → {new['PostDate'].max()}")
    print(f"  Post types:\n{new['post_type'].value_counts().to_string()}")
    print(f"  Source files:\n{new['source_file'].value_counts().to_string()}")

    print(sep)
    print("EXACT OVERLAPS")
    print(f"  Exact duplicate messages found: {len(exact)}")
    if len(exact) > 0:
        print(f"  Unique old posters in overlaps: {exact['PosterID_old'].nunique()}")
        print(f"  Unique new posters in overlaps: {exact['PosterID_new'].nunique()}")
        print(f"\n  Sample overlaps (old poster → new poster):")
        print(exact[["PosterID_old", "PosterID_new", "PostDate_old", "PostDate_new"]].head(10).to_string(index=False))

    print(sep)
    print(f"NEAR-DUPLICATE SAMPLE (threshold=0.85, sample={500})")
    print(f"  Near-duplicates found in sample: {len(near)}")
    if len(near) > 0:
        print(near.head(5).to_string(index=False))

    print(sep)
    print("SUPERUSER SIGNALS IN NEW DATA")
    flagged = signals[signals["superuser_flag"]]
    print(f"  Flagged as likely superuser: {len(flagged)} / {len(signals)} authors")
    if len(flagged) > 0:
        print(flagged.to_string(index=False))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading data…")
    old = load_old_messages()
    new = load_new_data()

    print("\nFinding exact overlaps…")
    exact = find_exact_overlaps(old, new)

    print("\nFinding near-duplicates (sample-based, may take a moment)…")
    near = find_near_overlaps(old, new)

    print("\nAnalyzing superuser signals in new data…")
    signals = superuser_signals(new, exact)

    print_report(old, new, exact, near, signals)

    # ── Save reports for manual review ───────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    exact.to_csv(os.path.join(OUTPUT_DIR, "diag_exact_overlaps.csv"), index=False)
    near.to_csv(os.path.join(OUTPUT_DIR, "diag_near_overlaps.csv"), index=False)
    signals.to_csv(os.path.join(OUTPUT_DIR, "diag_superuser_signals.csv"), index=False)
    print(f"\nReports saved to {OUTPUT_DIR}/diag_*.csv — review these before proceeding.")


if __name__ == "__main__":
    main()
