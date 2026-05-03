import os
import pandas as pd

# Load the CSV files

def read_csv_file(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    try:
        df = pd.read_csv(path, sep=None, engine="python", on_bad_lines="skip")
        print(f"Loaded {path}: {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        raise ValueError(f"Error reading {path}: {e}")


bbpress_data = read_csv_file('/Users/claudiagroot/Documents/RA/depression_connect_project/data/bbpress-export-2019-to-2020.csv')
messages_data = read_csv_file('../data/messages.csv')


# # Display basic info about the files
# print("BBPress data shape:", bbpress_data.shape)
# print("Messages data shape:", messages_data.shape)


# # Find common columns
# common_columns = bbpress_data.columns.intersection(messages_data.columns)
# print("\nCommon columns:", list(common_columns))

# # Find common values in the first common column (if any exist)
# if len(common_columns) > 0:
#     first_col = common_columns[0]
#     common_values = set(bbpress_data[first_col]) & set(messages_data[first_col])
#     print(f"\nCommon values in '{first_col}':", len(common_values))
#     print("Sample:", list(common_values)[:5])

# # Show first few rows of each file
# print("\nBBPress data (first 5 rows):")
# print(bbpress_data.head())
# print("\nMessages data (first 5 rows):")
# print(messages_data.head())