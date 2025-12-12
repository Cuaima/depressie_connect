import os
import warnings
import pandas as pd
from collections import Counter
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from text_anonymizer import anonymize, deanonymize


FILES = ["accounts", "groups", "messages", "topics"]

def to_datetime(df, column):
    df[column] = pd.to_datetime(df[column], errors="coerce")
    return df

def clean(df):
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)


    df_cleaned = df.replace(r"^\s*$", pd.NA, regex=True)
    df_cleaned = df_cleaned.dropna(how="all").reset_index(drop=True)


    if "MessageText" in df_cleaned.columns:
        df_cleaned["MessageText"] = df_cleaned["MessageText"].astype(str).apply(
        lambda x: BeautifulSoup(x, "html.parser").get_text()
        )


    if "PostedDate" in df_cleaned.columns:
        df_cleaned = to_datetime(df_cleaned, "PostedDate")
    elif "StartDate" in df_cleaned.columns:
        df_cleaned = to_datetime(df_cleaned, "StartDate")


    return df_cleaned

def anonymize(dfs, column="PosterID", file_names=FILES):
    all_ids = set()
    for df in dfs.values():
        if column in df.columns:
            all_ids.update(df[column].dropna().unique())

    user_map = {uid: f"user_{i+1}" for i, uid in enumerate(sorted(all_ids))}

    for name, df in dfs.items():
        if column in df.columns:
            df[column] = df[column].map(user_map)
            dfs[name] = df

    user_counter = Counter()
    for df in dfs.values():
        if column in df.columns:
            user_counter.update(df[column].dropna())

    with open("anonymization_mapping.txt", "w") as f:
        f.write("Original ID -> Anonymized ID\n")
        for original_id, anon_id in user_map.items():
            f.write(f"{original_id} -> {anon_id}\n")

    pd.DataFrame(
        list(user_map.items()), columns=["OriginalID", "AnonymizedID"]
    ).to_csv("anonymization_mapping.csv", index=False)

    return user_map

def load_csv_files(files=FILES):
    dfs = {}

    if os.path.exists("basic_exploration.txt"):
        os.remove("basic_exploration.txt")

    for file in files:
        path = f"data/{file}.csv"
        try:
            df = pd.read_csv(path, on_bad_lines="warn")
        except Exception as e:
            print(f"Error reading {path}: {e}")
            continue

        df = clean(df)

        with open("basic_exploration.txt", "a") as f:
            f.write(f"file: {file}\n")
            f.write(f"rows: {len(df)}\n")
            f.write(f"columns: {len(df.columns)}\n")
            f.write("column names:\n")
            for col in df.columns:
                f.write(f"  - {col}\n")
            f.write("\n\n")

        dfs[file] = df

    anonymize(dfs)
    return dfs


def new_files(dfs):
    for name, df in dfs.items():
        df.to_csv(f"{name}_cleaned_anonymized.csv", index=False)