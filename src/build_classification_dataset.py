import os
import pandas as pd

DATA_DIR = "data"
OUTPUT_DIR = "output"
ACCOUNT_TYPE = 1
ACOOUNT_TYPE_B = 4

# Adjust these if your column names differ
TOPIC_TEXT_COLUMN = "Name"
MESSAGE_TEXT_COLUMN = "MessageText"


def main():
    # --------------------------------------------------
    # Load data
    # --------------------------------------------------

    messages = pd.read_csv(os.path.join(DATA_DIR, "messages.csv"))
    topics = pd.read_csv(os.path.join(DATA_DIR, "topics.csv"))
    groups = pd.read_csv(os.path.join(DATA_DIR, "groups.csv"))

    # --------------------------------------------------
    # Map topics → account type
    # --------------------------------------------------

    topic_to_account = (
        topics
        .merge(groups, on="ForumGroupID", how="left")
        .dropna(subset=["AccountID"])
        .set_index("ForumTopicID")["AccountID"]
        .to_dict()
    )

    # --------------------------------------------------
    # Identify users who posted in AccountID = 1
    # --------------------------------------------------

    messages["AccountID"] = messages["ForumTopicID"].map(topic_to_account)

    posters_in_type_1 = set(
        messages.loc[
            messages["AccountID"] == ACCOUNT_TYPE,
            "PosterID"
        ].dropna()
    )

    print(f"Found {len(posters_in_type_1)} posters active in account type {ACCOUNT_TYPE}")

    # --------------------------------------------------
    # Filter messages by those users
    # --------------------------------------------------

    message_rows = (
        messages[
            messages["PosterID"].isin(posters_in_type_1)
        ][MESSAGE_TEXT_COLUMN]
        .dropna()
        .astype(str)
        .tolist()
    )

    message_df = pd.DataFrame({
        "text": message_rows,
        "label": "message"
    })

    # --------------------------------------------------
    # Filter topics by those users
    # --------------------------------------------------

    topic_rows = (
        topics[
            topics["PosterID"].isin(posters_in_type_1)
        ][TOPIC_TEXT_COLUMN]
        .dropna()
        .astype(str)
        .tolist()
    )

    topic_df = pd.DataFrame({
        "text": topic_rows,
        "label": "topic"
    })

    # --------------------------------------------------
    # Combine into final dataset
    # --------------------------------------------------

    dataset = (
        pd.concat([topic_df, message_df], ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    print("Final dataset shape:", dataset.shape)
    print(dataset.head())

    # print number of samples with empty text
    empty_text_count = dataset[dataset["text"].str.strip() == ""].shape[0]
    print(f"Number of samples with empty text: {empty_text_count}")

    # create a csv file
    dataset.to_csv(os.path.join(OUTPUT_DIR, "classification_dataset.csv"), index=False)

    return dataset


if __name__ == "__main__":
    dataset = main()
