import os
import pandas as pd

DATA_DIR = "data"
OUTPUT_DIR = "output"

ACCOUNTS = [1, 4]

MESSAGE_TEXT_COLUMN = "MessageText"
DATE_COLUMN = "PostDate"


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
        .drop_duplicates("ForumTopicID")
        .set_index("ForumTopicID")["AccountID"]
        .to_dict()
    )

    messages["AccountID"] = messages["ForumTopicID"].map(topic_to_account)

    # --------------------------------------------------
    # Identify posters active in selected account types
    # --------------------------------------------------

    superuser_posters = set()

    for acc in ACCOUNTS:
        posters = set(
            messages.loc[
                messages["AccountID"] == acc,
                "PosterID"
            ].dropna()
        )
        print(f"Found {len(posters)} posters active in account type {acc}")
        superuser_posters.update(posters)

    # --------------------------------------------------
    # Filter messages to those posters
    # --------------------------------------------------

    messages = messages[
        messages["PosterID"].isin(superuser_posters)
    ].copy()

    # Ensure datetime
    messages[DATE_COLUMN] = pd.to_datetime(
        messages[DATE_COLUMN],
        errors="coerce"
    )

    messages = messages.dropna(subset=[MESSAGE_TEXT_COLUMN, DATE_COLUMN])

    # --------------------------------------------------
    # Identify oldest message per topic → "topic"
    # --------------------------------------------------

    oldest_messages = (
    messages
    .sort_values(DATE_COLUMN)
    .groupby("ForumTopicID")
    .head(1)
    )

    topic_df = pd.DataFrame({
        "text": oldest_messages[MESSAGE_TEXT_COLUMN].astype(str),
        "label": "topic"
    })

    # --------------------------------------------------
    # Remaining messages → "message"
    # --------------------------------------------------

    oldest_message_ids = set(oldest_messages.index)

    message_df = (
        messages
        .drop(index=oldest_messages.index)
        .assign(label="message")
        .rename(columns={MESSAGE_TEXT_COLUMN: "text"})[
            ["text", "label"]
        ]
    )

    # --------------------------------------------------
    # Combine & clean
    # --------------------------------------------------

    dataset = (
        pd.concat([topic_df, message_df], ignore_index=True)
        .dropna(subset=["text"])
    )

    dataset["text"] = dataset["text"].astype(str).str.strip()
    dataset = dataset[dataset["text"] != ""]

    dataset = dataset.drop_duplicates().reset_index(drop=True)

    # --------------------------------------------------
    # Output
    # --------------------------------------------------

    print("Final dataset shape:", dataset.shape)
    print(dataset["label"].value_counts())
    print(dataset.head())

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dataset.to_csv(
        os.path.join(OUTPUT_DIR, "classification_dataset.csv"),
        index=False
    )

    return dataset


if __name__ == "__main__":
    main()
