# =============================================================================
# app.py  –  Streamlit dashboard for Depression Connect analysis
# =============================================================================

import streamlit as st
import pandas as pd
from analysis import compute_all_metrics, most_common_words
from exploration import (
    load_messages,
    posts_per_user, posts_per_user_stats,
    posts_per_thread, posts_per_thread_stats,
    user_activity_span, user_activity_span_stats,
    user_posting_rate,
    activity_per_day,
    posts_by_hour, posts_by_day_of_week, posts_by_month,
    top_users,
    ik_statistics,
)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Depression Connect Analysis", layout="wide")
st.title("Depression Connect – Exploratory Analysis")

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data
def get_messages() -> pd.DataFrame:
    return load_messages()

@st.cache_data
def get_metrics(messages: pd.DataFrame) -> dict:
    return compute_all_metrics(messages)

messages = get_messages()
metrics  = get_metrics(messages)

# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.header("Controls")

view = st.sidebar.radio(
    "Select view",
    [
        "Posts per User",
        "Posts per Thread",
        "User Activity Span",
        "Posting Rate",
        "Activity over Time",
        "Popular Hours",
        "Popular Days of Week",
        "Popular Months",
        "Top 10 Users",
        "IK Usage",
        "Most Common Words",
    ],
)

# ── Helper to display mean / median / mode ────────────────────────────────────

def show_central_tendency(label: str, stats: dict):
    col1, col2, col3 = st.columns(3)
    col1.metric(f"{label} — Mean",   stats["mean"])
    col2.metric(f"{label} — Median", stats["median"])
    col3.metric(f"{label} — Mode",   stats["mode"])


# ── Views ─────────────────────────────────────────────────────────────────────

if view == "Posts per User":
    st.header("Posts per User")

    stats = posts_per_user_stats(messages)
    show_central_tendency("Posts per user", stats)

    df = posts_per_user(messages)
    st.subheader("Distribution")
    st.bar_chart(df.set_index(df.columns[0])["post_count"].head(50))
    st.subheader("Full table")
    st.dataframe(df, use_container_width=True)


elif view == "Posts per Thread":
    st.header("Posts per Thread")

    stats = posts_per_thread_stats(messages)
    st.subheader("Total messages per thread (including opening post)")
    show_central_tendency("Total messages", stats["total_messages"])

    st.subheader("Replies only (excluding opening post)")
    show_central_tendency("Replies", stats["replies_only"])

    df = posts_per_thread(messages)
    st.subheader("Distribution — total messages")
    st.bar_chart(df.set_index("ForumTopicID")["total_messages"].head(50))
    st.subheader("Full table")
    st.dataframe(df, use_container_width=True)


elif view == "User Activity Span":
    st.header("User Activity Span")

    stats = user_activity_span_stats(messages)
    st.subheader("Days between first and last post")
    show_central_tendency("Span days", stats["active_days_span"])

    st.subheader("Distinct days with at least one post")
    show_central_tendency("Active days", stats["active_days_count"])

    df = user_activity_span(messages)
    st.subheader("Full table")
    st.dataframe(df, use_container_width=True)


elif view == "Posting Rate":
    st.header("Posts Relative to Time Active")
    st.caption(
        "posts_per_span_day = total posts ÷ days between first and last post. "
        "posts_per_active_day = total posts ÷ distinct days with a post."
    )

    df = user_posting_rate(messages)

    st.subheader("Posts per active day (top 50)")
    st.bar_chart(df.set_index("PosterID")["posts_per_active_day"].head(50))

    st.subheader("Full table")
    st.dataframe(df, use_container_width=True)


elif view == "Activity over Time":
    st.header("Post Volume per Day")

    df = activity_per_day(messages)
    df["date"] = pd.to_datetime(df["date"])

    st.line_chart(df.set_index("date")["post_count"])
    st.subheader("Full table")
    st.dataframe(df, use_container_width=True)


elif view == "Popular Hours":
    st.header("Popular Hours of Day")
    st.caption("Based on the hour extracted from PostDate (server time).")

    df = posts_by_hour(messages)
    st.bar_chart(df.set_index("hour")["post_count"])
    st.dataframe(df, use_container_width=True)


elif view == "Popular Days of Week":
    st.header("Popular Days of Week")

    df = posts_by_day_of_week(messages)
    st.bar_chart(df.set_index("day_of_week")["post_count"])
    st.dataframe(df, use_container_width=True)


elif view == "Popular Months":
    st.header("Popular Months")

    df = posts_by_month(messages)
    st.bar_chart(df.set_index("month")["post_count"])
    st.dataframe(df, use_container_width=True)


elif view == "Top 10 Users":
    st.header("Top 10 Most Active Users")

    df = top_users(messages, n=10)
    st.dataframe(df, use_container_width=True)

    st.subheader("Post count")
    st.bar_chart(df.set_index("PosterID")["post_count"])

    st.subheader("Posts per active day")
    st.bar_chart(df.set_index("PosterID")["posts_per_active_day"])


elif view == "IK Usage":
    st.header("'Ik' Usage")

    ik = ik_statistics(messages)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total 'ik' count",  ik["total_ik_count"])
    col2.metric("Total word count",  ik["total_word_count"])
    col3.metric("Overall 'ik' %",    f"{ik['overall_ik_pct']}%")

    st.subheader("Per-user count")
    show_central_tendency("Count", ik["per_user_stats"])

    st.subheader("Per-user % of total words")
    show_central_tendency("Percentage", ik["per_user_pct_stats"])

    st.subheader("Per-user table")
    st.dataframe(ik["per_user"], use_container_width=True)

    st.subheader("Top 10 'ik' users")
    st.bar_chart(
        ik["per_user"].head(10).set_index("PosterID")["ik_count"]
    )


elif view == "Most Common Words":
    st.header("Most Common Words")

    top_n = st.slider("Number of words", min_value=10, max_value=100, value=50)
    remove_sw = st.checkbox("Remove stopwords?", value=True)
    stopwords  = {"de", "het", "een", "en", "van", "in", "is", "ik",
                  "dat", "op", "te", "met", "voor", "zijn", "er"} if remove_sw else None

    df = most_common_words(messages, top_n=top_n, remove_stopwords=remove_sw, stopwords=stopwords)
    st.bar_chart(df.set_index("Word")["Count"])
    st.dataframe(df, use_container_width=True)
