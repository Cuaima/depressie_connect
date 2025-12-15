import streamlit as st
import pandas as pd
from analysis import (
    words_and_chars_per_user,
    words_and_chars_per_topic,
    messages_per_topic,
    topics_per_user,
    words_per_user_per_month,
)

st.set_page_config(
    page_title="Depression Connect Analysis",
    layout="wide"
)

st.title("Depression Connect – Exploratory Analysis")

# ----------------------------------------------------------------------
# Sidebar
# ----------------------------------------------------------------------

st.sidebar.header("Navigation")
view = st.sidebar.radio(
    "Select view",
    [
        "Words & Characters per User",
        "Words & Characters per Topic",
        "Messages per Topic",
        "Topics per User",
        "Words per User per Month",
    ],
)

# ----------------------------------------------------------------------
# Views
# ----------------------------------------------------------------------

if view == "Words & Characters per User":
    st.header("Words & Characters per User")
    df = words_and_chars_per_user()
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("PosterID")["word_count"])

elif view == "Words & Characters per Topic":
    st.header("Words & Characters per Topic")
    df = words_and_chars_per_topic()
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("ForumTopicID")["word_count"])

elif view == "Messages per Topic":
    st.header("Messages per Topic")
    df = messages_per_topic()
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("ForumTopicID")["message_count"])

elif view == "Topics per User":
    st.header("Topics per User")
    df = topics_per_user()
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("PosterID")["topic_count"])

elif view == "Words per User per Month":
    st.header("Words per User per Month")

    df = words_per_user_per_month()

    users = sorted(df["PosterID"].unique())
    selected_user = st.selectbox("Select user", users)

    user_df = df[df["PosterID"] == selected_user]

    st.dataframe(user_df, use_container_width=True)
    st.line_chart(
        user_df.set_index("year_month")["word_count"]
    )
