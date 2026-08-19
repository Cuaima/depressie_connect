# =============================================================================
# preprocess.py  –  load → filter → clean → anonymize
#
# Execution order:
#   1. load_raw_data()           – read CSVs from disk
#   2. build_topic_account_map() – join topics → groups → accounts
#   3. remove_superusers()       – drop posters from test/demo forums
#   3b. remove_moderators()      – drop confirmed moderator posters
#   4. clean_dataframe()         – strip HTML, convert dates
#   5. filter_text_quality()     – min length, language
#   6. anonymize_ids()           – replace PosterID with user_N
#   7. anonymize_text_columns()  – NER-based text anonymization
#   8. save_outputs()            – attach GroupName, write community CSV
#
# Note: intro/welcome group filtering is handled in postprocess.py (step 2),
# so it can be adjusted without re-running the expensive anonymization step.
# =============================================================================

from __future__ import annotations

import os
import re
import warnings
import pandas as pd
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

from tqdm import tqdm

from config import (
    DATA_DIR, OUTPUT_DIR, PREPROCESS_DIR,
    CSV_FILES, ID_COLUMN, DATE_COLUMNS, TEXT_COLUMN,
    SUPERUSER_ACCOUNT_IDS, COMMUNITY_ACCOUNT_IDS,
    MODERATOR_POSTER_IDS,
    MIN_WORD_COUNT, LANGUAGE_FILTER, TARGET_LANGUAGE,
    ANONYMIZE_TEXT, REPLACE_ORIGINAL_TEXT, EXPORT_ENTITY_REVIEW,
    INTEGRATED_OLD_PATH, INTEGRATED_NEW_PATH, INTEGRATED_COMBINED_PATH,
)
from utils.thread_utils import parse_post_dates

_ANON_AVAILABLE = False
_LANGDETECT_AVAILABLE = False

if LANGUAGE_FILTER:
    try:
        from langdetect import detect, LangDetectException
        _LANGDETECT_AVAILABLE = True
    except ImportError:
        print("WARNING: langdetect not installed – language filter disabled.")

if ANONYMIZE_TEXT:
    try:
        from custom_text_anonymizer import anonymize as ta_anonymize
        _ANON_AVAILABLE = True
    except Exception as e:
        print(f"WARNING: custom_text_anonymizer unavailable ({type(e).__name__}: {e})")
        print("  → Run with: conda activate thesis_env && PYTHONPATH=./src python src/preprocess.py")

print("preprocess module loaded.")


# ── I/O helpers ───────────────────────────────────────────────────────────────

def ensure_output_dir():
    os.makedirs(PREPROCESS_DIR, exist_ok=True)


def read_csv(name: str, **kwargs) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, f"{name}.csv")
    print(f"  Loading {path}")
    return pd.read_csv(path, on_bad_lines="warn", **kwargs)


def write_csv(df: pd.DataFrame, filename: str):
    df.to_csv(os.path.join(PREPROCESS_DIR, filename), index=False)


# ── Step 1: Load raw data ─────────────────────────────────────────────────────

_DATASET_PATHS = {
    "old":      INTEGRATED_OLD_PATH,
    "new_only": INTEGRATED_NEW_PATH,
    "combined": INTEGRATED_COMBINED_PATH,
}


def load_raw_data(dataset: str | None = None) -> dict[str, pd.DataFrame]:
    """
    Load messages and metadata CSVs.

    dataset: one of "old", "new_only", "combined", or None.
      None  → reads data/messages.csv (single-export workflow)
      other → reads the corresponding output/messages_*.csv produced by
              integrate_datasets.py
    """
    print("\n[1] Loading raw data…")
    dfs = {}

    if dataset is not None:
        path = _DATASET_PATHS[dataset]
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Dataset file not found: {path}\n"
                "Run integrate_datasets.py first."
            )
        print(f"  Loading {dataset} dataset from {path}")
        dfs["messages"] = pd.read_csv(path, on_bad_lines="skip")
    else:
        integrated_path = os.path.join(OUTPUT_DIR, "integrated_messages.csv")
        if os.path.exists(integrated_path):
            print(f"  Loading integrated messages from {integrated_path}")
            dfs["messages"] = pd.read_csv(integrated_path, on_bad_lines="skip")
        else:
            dfs["messages"] = read_csv("messages")

    for name in ["topics", "groups", "accounts"]:
        dfs[name] = read_csv(name)

    return dfs


# ── Step 2: Build topic → account mapping ────────────────────────────────────

def build_topic_account_map(dfs: dict[str, pd.DataFrame]) -> dict:
    topics = dfs["topics"][["ForumTopicID", "ForumGroupID"]].copy()
    groups = dfs["groups"][["ForumGroupID", "AccountID", "Name"]].copy()
    groups = groups.rename(columns={"Name": "GroupName"})

    merged = topics.merge(groups, on="ForumGroupID", how="left")

    with_account = (
        merged
        .dropna(subset=["AccountID"])
        .drop_duplicates(subset=["ForumTopicID"])
    )
    topic_to_account = dict(zip(
        with_account["ForumTopicID"],
        with_account["AccountID"].astype(int),
    ))

    with_topic = (
        merged
        .dropna(subset=["ForumTopicID"])
        .drop_duplicates(subset=["ForumTopicID"])
    )
    topic_to_group = dict(zip(
        with_topic["ForumTopicID"].astype(int),
        with_topic["GroupName"].fillna(""),
    ))

    return topic_to_account, topic_to_group


# ── Step 3: Remove superuser posters ─────────────────────────────────────────

def get_superuser_ids(
    messages: pd.DataFrame,
    topic_to_account: dict,
    superuser_accounts: set = SUPERUSER_ACCOUNT_IDS,
) -> set:
    df = messages.copy()
    df["AccountID"] = df["ForumTopicID"].map(topic_to_account)
    superuser_posters = set(
        df.loc[df["AccountID"].isin(superuser_accounts), ID_COLUMN].dropna()
    )
    print(f"  Identified {len(superuser_posters)} superuser posters to exclude.")
    return superuser_posters


def remove_superusers(messages: pd.DataFrame, superuser_ids: set) -> pd.DataFrame:
    before = len(messages)
    messages = messages[~messages[ID_COLUMN].isin(superuser_ids)].copy()
    print(f"  Removed superusers: {before} → {len(messages)} messages.")
    return messages


# ── Step 3b: Remove confirmed moderator posters ───────────────────────────────

def remove_moderators(
    messages: pd.DataFrame,
    moderator_ids: set = MODERATOR_POSTER_IDS,
) -> pd.DataFrame:
    if not moderator_ids:
        print("  No moderator IDs configured – skipping.")
        return messages

    before = len(messages)
    date_col = DATE_COLUMNS[0]

    if date_col in messages.columns:
        first_posts = (
            messages.sort_values(["ForumTopicID", date_col])
            .drop_duplicates(subset=["ForumTopicID"], keep="first")
        )
        mod_threads = set(
            first_posts.loc[
                first_posts[ID_COLUMN].isin(moderator_ids), "ForumTopicID"
            ]
        )
        messages = messages[~messages["ForumTopicID"].isin(mod_threads)].copy()
        print(f"  Removed {len(mod_threads)} moderator-initiated threads.")
    else:
        print(f"  WARNING: date column '{date_col}' not found; skipping thread-level filter.")

    before_replies = len(messages)
    messages = messages[~messages[ID_COLUMN].isin(moderator_ids)].copy()
    removed_replies = before_replies - len(messages)
    if removed_replies:
        print(f"  Removed {removed_replies} individual moderator messages from other threads.")

    print(f"  Total removed: {before} → {len(messages)} messages.")
    return messages


# ── Step 4: Clean dataframes (HTML stripping, date parsing) ──────────────────

def _parse_html(text: str) -> str:
    return BeautifulSoup(str(text), "html.parser").get_text(separator=" ").strip()


def _strip_forum_quotes(text: str) -> str:
    text = re.sub(r"\[quote[^\]]*\].*?\[/quote\]", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"^>.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

    df = df.replace(r"^\s*$", pd.NA, regex=True).dropna(how="all").reset_index(drop=True)

    for col in df.columns:
        df[col] = df[col].astype(str).apply(_parse_html)

    if TEXT_COLUMN in df.columns:
        df[TEXT_COLUMN] = df[TEXT_COLUMN].apply(_strip_forum_quotes)

    for col in DATE_COLUMNS:
        if col in df.columns:
            # astype(str) above turned real NaN into the string "nan"/"<NA>";
            # those never were dates, so exclude them from the loss check.
            raw = df[col].astype(str)
            had_value = ~raw.str.lower().isin({"nan", "nat", "<na>", "none", ""})
            df[col] = parse_post_dates(df[col])
            lost = int((had_value & df[col].isna()).sum())
            if lost:
                frac = lost / max(int(had_value.sum()), 1)
                if frac > 0.005:
                    raise RuntimeError(
                        f"{lost} non-empty '{col}' values ({frac:.1%}) failed to parse — "
                        "a systemic date-format problem, not stray dirty rows. "
                        "Refusing to continue and silently drop them."
                    )
                print(f"  WARNING: {lost} non-empty '{col}' values failed to parse (kept as NaT).")

    return df


def standardize_text(df: pd.DataFrame) -> pd.DataFrame:
    if TEXT_COLUMN not in df.columns:
        return df

    df = df.copy()
    text = df[TEXT_COLUMN].fillna("").astype(str)

    text = text.apply(lambda t: re.sub(r"https?://\S+|www\.\S+", "", t))
    text = text.apply(lambda t: re.sub(r"([!?.]){2,}", r"\1", t))
    text = text.apply(lambda t: re.sub(r"[\r\n\t]+", " ", t))
    text = text.apply(lambda t: re.sub(r" {2,}", " ", t))
    text = text.str.strip()

    df[f"{TEXT_COLUMN}_normalized"] = text.str.lower()
    df[TEXT_COLUMN] = text

    return df


# ── Step 5: Text quality filters ──────────────────────────────────────────────

def _word_count(text: str) -> int:
    return len(str(text).split())


def _detect_language(text: str) -> str:
    if not _LANGDETECT_AVAILABLE:
        return TARGET_LANGUAGE
    try:
        return detect(str(text))
    except Exception:
        return "unknown"


def filter_text_quality(
    df: pd.DataFrame,
    min_words: int = MIN_WORD_COUNT,
    language_filter: bool = LANGUAGE_FILTER,
    target_lang: str = TARGET_LANGUAGE,
) -> pd.DataFrame:
    if TEXT_COLUMN not in df.columns:
        return df

    before = len(df)
    df = df[df[TEXT_COLUMN].fillna("").apply(_word_count) >= min_words].copy()
    print(f"  Min-word filter ({min_words}): {before} → {len(df)} messages.")

    if language_filter and _LANGDETECT_AVAILABLE:
        before = len(df)
        tqdm.pandas(desc="Language detection", unit="msg")
        df["_lang"] = df[TEXT_COLUMN].progress_apply(_detect_language)
        df = df[df["_lang"] == target_lang].drop(columns=["_lang"])
        print(f"  Language filter ({target_lang}): {before} → {len(df)} messages.")

    return df


# ── Step 6: ID anonymization ──────────────────────────────────────────────────

def anonymize_ids(dfs: dict[str, pd.DataFrame]) -> dict[str, str]:
    all_ids: set = set()
    for df in dfs.values():
        if ID_COLUMN in df.columns:
            all_ids.update(df[ID_COLUMN].dropna())

    mapping = {uid: f"user_{i + 1}" for i, uid in enumerate(sorted(all_ids))}

    for df in dfs.values():
        if ID_COLUMN in df.columns:
            df[ID_COLUMN] = df[ID_COLUMN].map(mapping)

    write_csv(
        pd.DataFrame(mapping.items(), columns=["OriginalID", "AnonymizedID"]),
        "anonymization_mapping.csv",
    )
    print(f"  Anonymized {len(mapping)} unique poster IDs.")
    return mapping


# ── Step 7: Text anonymization ───────────────────────────────────────────────

def _strip_at_mentions(text: str) -> str:
    return re.sub(r"@(\w+)", lambda m: m.group(1).replace("_", " "), text)


def anonymize_text_column(
    df: pd.DataFrame,
    column: str,
    export_review: bool = EXPORT_ENTITY_REVIEW,
    replace_original: bool = REPLACE_ORIGINAL_TEXT,
) -> pd.DataFrame:
    if column not in df.columns:
        print(f"  SKIP anonymization: column '{column}' not found.")
        return df
    if not _ANON_AVAILABLE:
        print("  SKIP anonymization: custom_text_anonymizer unavailable.")
        return df

    anon_texts, anon_entities = [], []
    for text in tqdm(df[column].fillna("").astype(str),
                     desc=f"Anonymising {column}", unit="msg"):
        cleaned = _strip_at_mentions(text)
        anon, entities = ta_anonymize(cleaned)
        anon_texts.append(anon)
        anon_entities.append(entities)

    df = df.copy()
    df[f"{column}_anon"] = anon_texts

    if "ForumMessageID" in df.columns:
        ref_col = "ForumMessageID"
    else:
        ref_col = df.index.name or "index"
        df = df.reset_index()

    write_csv(
        pd.DataFrame({ref_col: df[ref_col], "column": column, "entities": anon_entities}),
        f"entities_{column}.csv",
    )

    if export_review:
        write_csv(
            pd.DataFrame({
                "original_text":   df[column],
                "anonymized_text": df[f"{column}_anon"],
            }),
            f"review_anonymization_{column}.csv",
        )

    if replace_original:
        df[column] = df[f"{column}_anon"]
        df = df.drop(columns=[f"{column}_anon"])

    if ref_col == "index" and "index" in df.columns:
        df = df.drop(columns=["index"])

    return df


def anonymize_text_columns(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    if columns is None:
        columns = [TEXT_COLUMN, "Name"]
    for col in columns:
        if col in df.columns:
            df = anonymize_text_column(df, col)
    return df


# ── Step 8: Save outputs ──────────────────────────────────────────────────────

def save_outputs(
    messages: pd.DataFrame,
    topic_to_account: dict,
    topic_to_group: dict,
    dataset: str | None = None,
):
    messages = messages.copy()
    using_integrated = dataset is not None or os.path.exists(
        os.path.join(OUTPUT_DIR, "integrated_messages.csv")
    )

    def safe_topic_key(x):
        try:
            return str(int(float(x)))
        except (ValueError, TypeError):
            return None

    topic_to_account_str = {str(int(float(k))): v for k, v in topic_to_account.items()}

    messages["AccountID"] = (
        messages["ForumTopicID"]
        .apply(safe_topic_key)
        .map(topic_to_account_str)
    )

    if using_integrated:
        community = messages
    else:
        matched = messages["AccountID"].notna().sum()
        print(f"  Matched {matched}/{len(messages)} messages to an account.")
        community = messages[
            messages["AccountID"].isin(COMMUNITY_ACCOUNT_IDS)
        ].drop(columns=["AccountID"])

    community = community.copy()
    community["GroupName"] = community["ForumTopicID"].apply(
        lambda x: topic_to_group.get(safe_topic_key(x), "") if pd.notna(x) else ""
    )

    community = community.drop(columns=["AccountID"], errors="ignore")

    suffix = f"_{dataset}" if dataset and dataset != "combined" else ""
    filename = f"messages_community{suffix}.csv"
    write_csv(community, filename)
    print(f"  Wrote {len(community)} messages → {filename}")


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(dataset: str | None = None):
    """
    dataset: "old", "new_only", "combined", or None (reads data/messages.csv).
    Outputs are named messages_community.csv, messages_community_old.csv,
    or messages_community_new_only.csv accordingly.
    """
    ensure_output_dir()

    # Skip superuser/moderator removal only for "combined" (integrate_datasets.py
    # already filtered it) and for the legacy single-export path when
    # integrated_messages.csv exists on disk.  For "old" and "new_only" we always
    # run the removal because those slices are not guaranteed to be clean.
    skip_removal = dataset == "combined" or (
        dataset is None
        and os.path.exists(os.path.join(OUTPUT_DIR, "integrated_messages.csv"))
    )

    # 1. Load
    dfs = load_raw_data(dataset)

    # 2. Build maps
    print("\n[2] Building topic → account map…")
    raw_topics = pd.read_csv(os.path.join(DATA_DIR, "topics.csv"))
    raw_groups = pd.read_csv(os.path.join(DATA_DIR, "groups.csv"))
    topic_to_account, topic_to_group = build_topic_account_map({
        "topics": raw_topics,
        "groups": raw_groups,
    })

    if skip_removal:
        print("\n[3] Skipping superuser/moderator removal – already applied in integrate_datasets.py")
    else:
        # 3. Superuser removal
        print("\n[3] Identifying superusers…")
        superuser_ids = get_superuser_ids(dfs["messages"], topic_to_account)
        dfs["messages"] = remove_superusers(dfs["messages"], superuser_ids)

        # 3b. Moderator removal
        print("\n[3b] Removing moderators…")
        dfs["messages"] = remove_moderators(dfs["messages"])

    # 4. Clean all DataFrames
    print("\n[4] Cleaning dataframes (HTML, dates, quote stripping)…")
    for name in dfs:
        dfs[name] = clean_dataframe(dfs[name])

    # 4b. Standardize text
    print("\n[4b] Standardizing text…")
    dfs["messages"] = standardize_text(dfs["messages"])

    # 5. Text quality filters (messages only)
    print("\n[5] Filtering text quality…")
    dfs["messages"] = filter_text_quality(dfs["messages"])

    # 6. ID anonymization
    print("\n[6] Anonymizing poster IDs…")
    anonymize_ids(dfs)

    # 7. Text anonymization
    if ANONYMIZE_TEXT:
        print("\n[7] Anonymizing text…")
        dfs["messages"] = anonymize_text_columns(dfs["messages"], columns=[TEXT_COLUMN])
        dfs["topics"]   = anonymize_text_columns(dfs["topics"],   columns=["Name"])

        # 7b. Strip entity placeholder tokens (e.g. [ENTITY_PERSON_1]) so they
        # don't appear as words in word-frequency and LIWC analyses downstream.
        # The anonymizer review CSV and entity log are already written above.
        print("\n[7b] Stripping entity placeholder tokens…")
        _entity_re = re.compile(r"\[ENTITY_[A-Z]+_\d+\]")
        dfs["messages"][TEXT_COLUMN] = (
            dfs["messages"][TEXT_COLUMN]
            .str.replace(_entity_re, "", regex=True)
            .str.replace(r"  +", " ", regex=True)
            .str.strip()
        )
        print("  Done.")

        # Re-derive the normalized column from the now-cleaned text.
        norm_col = f"{TEXT_COLUMN}_normalized"
        if norm_col in dfs["messages"].columns:
            dfs["messages"][norm_col] = dfs["messages"][TEXT_COLUMN].str.lower()

    # 8. Write cleaned files + community output
    print("\n[8] Saving outputs…")
    for name, df in dfs.items():
        write_csv(df, f"{name}_cleaned.csv")

    save_outputs(dfs["messages"], topic_to_account, topic_to_group, dataset)

    print("\n✓ Pipeline complete.")
    return dfs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        choices=["old", "new_only", "combined"],
        default=None,
        help="Which integrated dataset to process. Omit to use data/messages.csv directly.",
    )
    args = parser.parse_args()
    run_pipeline(dataset=args.dataset)
