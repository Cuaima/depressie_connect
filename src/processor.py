import os
import warnings
import pandas as pd
from collections import Counter
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from text_anonymizer import anonymize as ta_anonymize

print("Processor module loaded.")

DATA_DIR = "data"
OUTPUT_DIR = "output"

CSV_FILES = ["accounts", "groups", "messages", "topics"]

ID_COLUMN = "PosterID"
DATE_COLUMNS = ["PostDate", "StartDate"]


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def read_csv_file(path):
    return pd.read_csv(path, on_bad_lines="warn")


def write_csv(df, name):
    df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)


# ----------------------------------------------------------------------
# Cleaning
# ----------------------------------------------------------------------

def parse_html(text):
    return BeautifulSoup(str(text), "html.parser").get_text()


def convert_dates(df):
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def anonymize_text_column(df, column):
    if column not in df.columns:
        return df

    texts = []
    maps = []

    for text in df[column].fillna("").astype(str):
        anonymized, mapping = ta_anonymize(text)
        texts.append(anonymized)
        maps.append(mapping)

    df = df.copy()
    df[column] = texts
    df[f"_{column}_AnonymizationMap"] = maps
    return df


def clean_dataframe(df):
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(how="all").reset_index(drop=True)

    # HTML cleaning
    for col in df.columns:
        df[col] = df[col].astype(str).apply(parse_html)

    # Text anonymization
    if "MessageText" in df.columns:
        df = anonymize_text_column(df, "MessageText")

    if "Name" in df.columns:
        df = anonymize_text_column(df, "Name")

    df = convert_dates(df)
    return df


# ----------------------------------------------------------------------
# Load
# ----------------------------------------------------------------------

def load_data():
    dfs = {}
    for name in CSV_FILES:
        path = os.path.join(DATA_DIR, f"{name}.csv")
        print(f"Loading {path}")
        dfs[name] = clean_dataframe(read_csv_file(path))
    return dfs


# ----------------------------------------------------------------------
# ID anonymization
# ----------------------------------------------------------------------

def anonymize_ids(dfs):
    all_ids = set()
    for df in dfs.values():
        if ID_COLUMN in df.columns:
            all_ids.update(df[ID_COLUMN].dropna())

    mapping = {uid: f"user_{i+1}" for i, uid in enumerate(sorted(all_ids))}

    for df in dfs.values():
        if ID_COLUMN in df.columns:
            df[ID_COLUMN] = df[ID_COLUMN].map(mapping)

    write_csv(
        pd.DataFrame(mapping.items(), columns=["OriginalID", "AnonymizedID"]),
        "anonymization_mapping.csv",
    )


# ----------------------------------------------------------------------
# Account-type splitting
# ----------------------------------------------------------------------

def split_by_account_type(groups, topics, messages):
    groups_by_type = {}

    for acc in groups["AccountID"].dropna().unique():
        groups_by_type[acc] = groups[groups["AccountID"] == acc]
        write_csv(groups_by_type[acc], f"groups_account_type_{acc}.csv")

    for acc, gdf in groups_by_type.items():
        topic_ids = gdf["ForumGroupID"].unique()
        tdf = topics[topics["ForumGroupID"].isin(topic_ids)]
        write_csv(tdf, f"topics_account_type_{acc}.csv")

        message_ids = tdf["ForumTopicID"].unique()
        mdf = messages[messages["ForumTopicID"].isin(message_ids)]
        write_csv(mdf, f"messages_account_type_{acc}.csv")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ensure_output_dir()

    dfs = load_data()
    anonymize_ids(dfs)

    split_by_account_type(
        dfs["groups"],
        dfs["topics"],
        dfs["messages"],
    )


if __name__ == "__main__":
    main()
