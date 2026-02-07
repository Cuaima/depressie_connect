import streamlit as st
import pandas as pd
from analysis import (compute_all_metrics,
 words_per_account_type_per_month,
    add_rolling_average, words_per_user_per_month, most_common_words)


# --------------------------------------------------
# Page config
# --------------------------------------------------

st.set_page_config(
    page_title="Depression Connect Analysis",
    layout="wide",
)

st.title("Depression Connect – Exploratory Analysis")

# --------------------------------------------------
# Data loading
# --------------------------------------------------

@st.cache_data
def load_messages(account_type: str = "2.0") -> pd.DataFrame:
    """
    Load cleaned & anonymized messages for a given account type.
    """
    path = f"output/messages_account_type_{account_type}.csv"
    return pd.read_csv(path)


@st.cache_data
def load_messages_by_type(types=("2.0", "3.0")):
    return {
        acc: pd.read_csv(f"output/messages_account_type_{acc}.csv")
        for acc in types
    }


@st.cache_data
def load_metrics(messages: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Compute all analytics tables.
    """
    return compute_all_metrics(messages)


# --------------------------------------------------
# Sidebar
# --------------------------------------------------

st.sidebar.header("Controls")

account_type = st.sidebar.selectbox(
    "Account type",
    options=["2.0", "3.0"],
    index=0,
)

view = st.sidebar.radio(
    "Select view",
    [
        "Words & Characters per User",
        "Words & Characters per Topic",
        "Messages per Topic",
        "Topics per User",
        "Words per User per Month",
        "Most Common Words",
    ],
)

comparison_view = st.sidebar.checkbox("Compare account types")
rolling_window = st.sidebar.slider(
    "Rolling average window (months)",
    min_value=1,
    max_value=12,
    value=3,
)

# --------------------------------------------------
# Load data once
# --------------------------------------------------

messages = load_messages(account_type)
metrics = load_metrics(messages)

# --------------------------------------------------
# Views
# --------------------------------------------------

if view == "Words & Characters per User":
    st.header("Words & Characters per User")

    df = metrics["words_chars_per_user"]
    st.dataframe(df, use_container_width=True)

    st.bar_chart(
        df
        .set_index("PosterID")["word_count"]
        .head(50)
    )

elif view == "Words & Characters per Topic":
    st.header("Words & Characters per Topic")

    df = metrics["words_chars_per_topic"]
    st.dataframe(df, use_container_width=True)

    st.bar_chart(
        df
        .set_index("ForumTopicID")["word_count"]
        .head(50)
    )

elif view == "Messages per Topic":
    st.header("Messages per Topic")

    df = metrics["messages_per_topic"]
    st.dataframe(df, use_container_width=True)

    st.bar_chart(
        df
        .set_index("ForumTopicID")["message_count"]
        .head(50)
    )

elif view == "Topics per User":
    st.header("Topics per User")

    df = metrics["topics_per_user"]
    st.dataframe(df, use_container_width=True)

    st.bar_chart(
        df
        .set_index("PosterID")["topic_count"]
        .head(50)
    )

elif view == "Words per User per Month":
    st.header("Words per User per Month")

    df = metrics["words_per_user_per_month"]

    users = sorted(df["PosterID"].unique())
    selected_user = st.selectbox("Select user", users)

    user_df = df[df["PosterID"] == selected_user]

    st.dataframe(user_df, use_container_width=True)

    st.line_chart(
        user_df
        .set_index("year_month")["word_count"]
    )

elif view == "Most Common Words":
    st.header("Most Common Words")

    top_n = st.slider("Number of words to display", min_value=10, max_value=100, value=50)

    # Optional stopwords example
    remove_stopwords = st.checkbox("Remove stopwords?", value=True)
    stopwords = set(["the", "and", "for", "with", "that", "this"]) if remove_stopwords else None

    df_words = most_common_words(messages, top_n=top_n, remove_stopwords=remove_stopwords, stopwords=stopwords)

    st.dataframe(df_words, use_container_width=True)

    st.bar_chart(
        df_words.set_index("Word")["Count"]
    )


def rolling_words_per_user_per_month(
    messages: pd.DataFrame,
    window: int = 3,
) -> pd.DataFrame:
    df = words_per_user_per_month(messages)

    return add_rolling_average(
        df=df,
        group_cols=["PosterID"],
        time_col="year_month",
        value_col="word_count",
        window=window,
    )


if comparison_view:
    st.header("Account Type Comparison – Monthly Word Volume")

    messages_by_type = load_messages_by_type()

    monthly = words_per_account_type_per_month(messages_by_type)

    monthly = add_rolling_average(
        df=monthly,
        group_cols=["AccountType"],
        time_col="year_month",
        value_col="word_count",
        window=rolling_window,
    )

    st.dataframe(monthly)

    st.line_chart(
        monthly.pivot(
            index="year_month",
            columns="AccountType",
            values=f"word_count_rolling_{rolling_window}",
        )
    )