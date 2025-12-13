from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import pandas as pd
import os
from src.analysis import (
    load_df,
    words_and_chars_per_user,
    words_and_chars_per_topic,
    messages_per_topic,
    topics_per_user,
    words_per_user_per_month
)

app = FastAPI(title="Depression Connect Live Analytics API")

# ----------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------

@app.get("/words-per-user")
def get_words_per_user():
    try:
        df = words_and_chars_per_user()
        return df.to_dict(orient="records")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="messages_cleaned_anonymized.csv not found")


@app.get("/words-per-topic")
def get_words_per_topic():
    try:
        df = words_and_chars_per_topic()
        return df.to_dict(orient="records")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="messages_cleaned_anonymized.csv not found")


@app.get("/messages-per-topic")
def get_messages_per_topic():
    try:
        df = messages_per_topic()
        return df.to_dict(orient="records")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="messages_cleaned_anonymized.csv not found")


@app.get("/topics-per-user")
def get_topics_per_user():
    try:
        df = topics_per_user()
        return df.to_dict(orient="records")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="messages_cleaned_anonymized.csv not found")


@app.get("/words-per-user-per-month")
def get_words_per_user_per_month(user: Optional[str] = Query(None, description="Filter by user ID")):
    try:
        df = words_per_user_per_month()
        if user:
            df = df[df["PosterID"] == user]
        return df.to_dict(orient="records")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="messages_cleaned_anonymized.csv not found")


@app.get("/health")
def health():
    return {"status": "ok"}
