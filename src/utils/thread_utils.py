import pandas as pd

DATE_COL  = "PostDate"
TOPIC_COL = "ForumTopicID"


def label_roles(df: pd.DataFrame) -> pd.DataFrame:
    """Labels the first message in each thread as 'post', rest as 'reply'."""
    df = df.copy().sort_values(DATE_COL)
    first_idx = df.groupby(TOPIC_COL)[DATE_COL].idxmin().dropna()
    df["role"] = "reply"
    df.loc[first_idx, "role"] = "post"
    return df
