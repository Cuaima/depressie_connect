import os
import warnings
import pandas as pd
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from text_anonymizer import anonymize as ta_anonymize

print("Processor module loaded.")

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

DATA_DIR = "data"
OUTPUT_DIR = "output"

CSV_FILES = ["accounts", "groups", "messages", "topics"]

ID_COLUMN = "PosterID"
DATE_COLUMNS = ["PostDate", "StartDate"]

# Anonymization controls
ANONYMIZE_TEXT = True
REPLACE_ORIGINAL_TEXT = True     # set True once you trust anonymization
EXPORT_ENTITY_REVIEW = True       # writes review CSVs

# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def read_csv_file(path: str) -> pd.DataFrame:
    return pd.read_csv(path, on_bad_lines="warn")


def write_csv(df: pd.DataFrame, name: str):
    df.to_csv(os.path.join(OUTPUT_DIR, name), index=False)


# ----------------------------------------------------------------------
# Cleaning helpers
# ----------------------------------------------------------------------

def parse_html(text: str) -> str:
    return BeautifulSoup(str(text), "html.parser").get_text()


def convert_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ----------------------------------------------------------------------
# Text anonymization (sidecar-style)
# ----------------------------------------------------------------------

def anonymize_text_column(
    df: pd.DataFrame,
    column: str,
    export_review: bool = EXPORT_ENTITY_REVIEW,
    replace_original: bool = REPLACE_ORIGINAL_TEXT,
) -> pd.DataFrame:
    """
    Adds:
      - {column}_anon      : anonymized text
      - {column}_entities  : extracted anonymized entities

    Does NOT overwrite original text unless replace_original=True.
    """

    if column not in df.columns:
        return df

    anon_texts = []
    anon_entities = []

    for text in df[column].fillna("").astype(str):
        anon, entities = ta_anonymize(text)
        anon_texts.append(anon)
        anon_entities.append(entities)

    df = df.copy()
    df[f"{column}_anon"] = anon_texts
    df[f"{column}_entities"] = anon_entities

    # Optional: export review CSV
    if export_review:
        review_df = pd.DataFrame({
            "original_text": df[column],
            "anonymized_text": df[f"{column}_anon"],
            "entities": df[f"{column}_entities"],
        })
        write_csv(review_df, f"review_anonymization_{column}.csv")

    # Optional: overwrite original text
    if replace_original:
        df[column] = df[f"{column}_anon"]

    return df


# ----------------------------------------------------------------------
# DataFrame cleaning pipeline
# ----------------------------------------------------------------------

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

    # Drop empty rows
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(how="all").reset_index(drop=True)

    # Strip HTML
    for col in df.columns:
        df[col] = df[col].astype(str).apply(parse_html)

    # Anonymize text columns
    if ANONYMIZE_TEXT:
        if "MessageText" in df.columns:
            df = anonymize_text_column(df, "MessageText")

        if "Name" in df.columns:
            df = anonymize_text_column(df, "Name")

    # Convert dates
    df = convert_dates(df)

    return df


# ----------------------------------------------------------------------
# Load & process all datasets
# ----------------------------------------------------------------------

def load_data() -> dict[str, pd.DataFrame]:
    dfs = {}

    for name in CSV_FILES:
        path = os.path.join(DATA_DIR, f"{name}.csv")
        print(f"Loading {path}")
        raw_df = read_csv_file(path)
        dfs[name] = clean_dataframe(raw_df)

    return dfs


# ----------------------------------------------------------------------
# ID anonymization
# ----------------------------------------------------------------------

def anonymize_ids(dfs: dict[str, pd.DataFrame]):
    all_ids = set()

    for df in dfs.values():
        if ID_COLUMN in df.columns:
            all_ids.update(df[ID_COLUMN].dropna())

    mapping = {
        uid: f"user_{i + 1}"
        for i, uid in enumerate(sorted(all_ids))
    }

    for df in dfs.values():
        if ID_COLUMN in df.columns:
            df[ID_COLUMN] = df[ID_COLUMN].map(mapping)

    write_csv(
        pd.DataFrame(mapping.items(), columns=["OriginalID", "AnonymizedID"]),
        "anonymization_mapping.csv",
    )


# ----------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------

def main():
    ensure_output_dir()

    dfs = load_data()
    anonymize_ids(dfs)

    # Write cleaned datasets
    for name, df in dfs.items():
        write_csv(df, f"{name}_cleaned.csv")

    print("Processing complete.")


if __name__ == "__main__":
    main()
