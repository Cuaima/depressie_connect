import os
import warnings
import pandas as pd
from collections import Counter
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from text_anonymizer import anonymize as ta_anonymize, deanonymize as ta_deanonymize


# ----------------------------------------------------------------------
# Sanity Check
# ----------------------------------------------------------------------

print("Processor module loaded.")


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

DATA_DIR = "data"
OUTPUT_DIR = "output"

CSV_FILES = ["accounts", "groups", "messages", "topics"]

ID_COLUMN = "PosterID"
DATE_COLUMNS = ["PostDate", "StartDate"]


# ----------------------------------------------------------------------
# Utility functions
# ----------------------------------------------------------------------

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def read_csv_file(path):
    try:
        return pd.read_csv(path, on_bad_lines="warn")
    except Exception as e:
        raise RuntimeError(f"Error reading {path}: {e}")


def write_csv(df, name):
    df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)


def anonymize_message_text(df):
    """
    Apply text_anonymizer to MessageText column.
    Stores both anonymized text and mapping for later use.
    """
    if "MessageText" not in df.columns:
        return df, None

    anonymized_texts = []
    mapping_store = []

    print("Anonymizing MessageText column...")
    for text in df["MessageText"].fillna("").astype(str):
        anonymized, mapping = ta_anonymize(text)
        anonymized_texts.append(anonymized)
        mapping_store.append(mapping)
    print("Anonymization complete.")

    df = df.copy()
    df["MessageText"] = anonymized_texts
    df["_MessageText_AnonymizationMap"] = mapping_store

    return df, mapping_store


def write_cleaned_anonymized_files(dfs):
    """
    Write all cleaned and anonymized dataframes to CSV in OUTPUT_DIR.
    """
    for name, df in dfs.items():
        write_csv(df, f"{name}_cleaned_anonymized.csv")

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



def clean_dataframe(df):
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(how="all").reset_index(drop=True)

    for col in df.columns:
        df[col] = df[col].astype(str).apply(parse_html)
        df, _ = anonymize_message_text(df)


    df = convert_dates(df)
    return df


# ----------------------------------------------------------------------
# Loading
# ----------------------------------------------------------------------

def load_data(file_list):
    dfs = {}
    for name in file_list:
        path = os.path.join(DATA_DIR, f"{name}.csv")
        print(f"Loading {path}...")
        df = read_csv_file(path)
        print(f"Cleaning {name} dataframe...")
        dfs[name] = clean_dataframe(df)
    return dfs


# ----------------------------------------------------------------------
# Anonymization
# ----------------------------------------------------------------------

def build_anonymization_map(dfs, id_column):
    all_ids = set()

    for df in dfs.values():
        if id_column in df.columns:
            all_ids.update(df[id_column].dropna().unique())

    mapping = {uid: f"user_{i+1}" for i, uid in enumerate(sorted(all_ids))}
    return mapping


def apply_mapping(dfs, id_column, mapping):
    for name, df in dfs.items():
        if id_column in df.columns:
            df[id_column] = df[id_column].map(mapping)
    return dfs


def count_ids(dfs, id_column):
    counter = Counter()
    for df in dfs.values():
        if id_column in df.columns:
            counter.update(df[id_column].dropna())
    return counter


def anonymize(dfs, id_column=ID_COLUMN):
    mapping = build_anonymization_map(dfs, id_column)
    apply_mapping(dfs, id_column, mapping)

    mapping_df = pd.DataFrame(mapping.items(), columns=["OriginalID", "AnonymizedID"])
    write_csv(mapping_df, "anonymization_mapping.csv")

    return mapping



# ----------------------------------------------------------------------
# Main pipeline
# ----------------------------------------------------------------------

def main():
    ensure_output_dir()
    
    dfs = load_data(CSV_FILES)
    anonymize(dfs)

    # Save full cleaned & anonymized CSVs
    print("Writing cleaned and anonymized files...")
    write_cleaned_anonymized_files(dfs)


if __name__ == "__main__":
    main()
