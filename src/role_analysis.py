# =============================================================================
# role_analysis.py  –  post vs. reply analyses
#
# Adds the EDA items comparing opening posts with replies:
#   - word count by role
#   - popular words by role (raw top-N frequency, not log-odds)
#   - sentence structure by role (avg sentences, words/sentence, punctuation)
#   - emoji use by role (% messages with emoji, mean count, top emojis)
#
# Called from eda_report.py via add_role_section_to_pdf().
# Can also be used standalone for interactive exploration.
# =============================================================================

from __future__ import annotations

from collections import Counter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from utils.thread_utils import (
    label_roles, tokenize_words, sentence_stats, extract_emojis
)

PRIMARY   = "#2E5E8E"
SECONDARY = "#EEF3F8"
ACCENT    = "#E8A838"
C_POST    = "#2166AC"
C_REPLY   = "#D6604D"

DEFAULT_STOPWORDS = {
    "de", "het", "een", "en", "van", "in", "is", "ik", "dat", "op",
    "te", "met", "voor", "zijn", "er", "maar", "ook", "als", "aan",
    "niet", "ze", "je", "me", "hij", "we", "bij", "zo", "dan", "nog",
    "wel", "om", "die", "wat", "mij", "dit", "al", "nu", "heb", "was",
    "kan", "meer", "heeft", "hem", "haar", "hun", "uit", "door", "dus",
    "of", "geen", "naar", "toch", "even", "maar", "weer", "wordt",
    "worden", "deze", "zou", "kunnen", "moet", "moeten", "ben", "zijn",
}


# =============================================================================
# Computation
# =============================================================================

def compute_role_stats(
    df: pd.DataFrame,
    text_col: str = "MessageText",
    topic_col: str = "ForumTopicID",
    date_col: str = "PostDate",
    stopwords: set | None = None,
    top_n_words: int = 25,
    top_n_emoji: int = 20,
) -> dict:
    """Compute all post-vs-reply analyses in one pass."""
    if stopwords is None:
        stopwords = DEFAULT_STOPWORDS

    df = label_roles(df, topic_col=topic_col, date_col=date_col)
    texts = df[text_col].fillna("").astype(str)

    # ── Word count by role ────────────────────────────────────────────────────
    word_counts = texts.apply(lambda t: len(t.split()))
    word_count_by_role = (
        pd.DataFrame({"role": df["role"].values, "word_count": word_counts.values})
        .groupby("role")["word_count"]
        .agg(["mean", "median", "std", "count"])
        .reindex(["post", "reply"])
    )

    # ── Popular words by role (raw frequency) ─────────────────────────────────
    popular_words: dict[str, list] = {}
    for role in ["post", "reply"]:
        mask = df["role"] == role
        counter: Counter = Counter()
        for t in texts[mask]:
            words = [w for w in tokenize_words(t) if w not in stopwords and len(w) > 1]
            counter.update(words)
        popular_words[role] = counter.most_common(top_n_words)

    # ── Sentence structure by role ─────────────────────────────────────────────
    sent_records = texts.apply(sentence_stats)
    sent_df = pd.DataFrame(list(sent_records))
    sent_df["role"] = df["role"].values
    sentence_by_role = sent_df.groupby("role").agg(
        sentences_per_message    =("n_sentences",       "mean"),
        words_per_sentence       =("words_per_sentence", "mean"),
        avg_word_length          =("avg_word_length",    "mean"),
        questions_per_message    =("n_questions",        "mean"),
        exclamations_per_message =("n_exclamations",     "mean"),
        commas_per_message       =("n_commas",           "mean"),
        pct_ending_in_question   =("ends_in_question",   "mean"),
    ).reindex(["post", "reply"])
    sentence_by_role["pct_ending_in_question"] *= 100

    # ── Emoji use by role ─────────────────────────────────────────────────────
    emoji_lists = texts.apply(extract_emojis)
    emoji_count = emoji_lists.apply(len)
    has_emoji   = emoji_count > 0
    emoji_frame = pd.DataFrame({
        "role":        df["role"].values,
        "emoji_count": emoji_count.values,
        "has_emoji":   has_emoji.values,
    })
    emoji_by_role = emoji_frame.groupby("role").agg(
        pct_messages_with_emoji=("has_emoji",   "mean"),
        mean_emoji_count        =("emoji_count", "mean"),
    ).reindex(["post", "reply"])
    emoji_by_role["pct_messages_with_emoji"] *= 100

    emoji_counters: dict[str, Counter] = {}
    for role in ["post", "reply"]:
        mask = df["role"] == role
        all_emojis = [e for lst in emoji_lists[mask] for e in lst]
        emoji_counters[role] = Counter(all_emojis)

    top_emojis: dict[str, list] = {}
    for role in ["post", "reply"]:
        c = emoji_counters[role]
        total = sum(c.values()) or 1
        top_emojis[role] = [
            (emo, cnt, round(cnt / total * 100, 2)) for emo, cnt in c.most_common(top_n_emoji)
        ]

    unique_emojis: dict[str, list] = {}
    for role, other in [("post", "reply"), ("reply", "post")]:
        other_set = set(emoji_counters[other].keys())
        unique_emojis[role] = [
            (emo, cnt)
            for emo, cnt in emoji_counters[role].most_common()
            if emo not in other_set
        ][:top_n_emoji]

    return {
        "df_roles":           df,
        "word_count_by_role": word_count_by_role,
        "popular_words":      popular_words,
        "sentence_by_role":   sentence_by_role,
        "emoji_by_role":      emoji_by_role,
        "top_emojis":         top_emojis,
        "unique_emojis":      unique_emojis,
    }


# =============================================================================
# Figures
# =============================================================================

def _style_ax(ax, title, xlabel, ylabel=""):
    ax.set_title(title, fontsize=13, fontweight="bold", color=PRIMARY, pad=10)
    ax.set_xlabel(xlabel, fontsize=10, color="#333333")
    ax.set_ylabel(ylabel, fontsize=10, color="#333333")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CCCCCC")
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.tick_params(colors="#555555")
    ax.set_facecolor(SECONDARY)


def _section_divider(title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 2.2))
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes, ha="center", va="center",
            fontsize=18, fontweight="bold", color=PRIMARY)
    return fig


def fig_word_count_by_role(word_count_by_role: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 4))
    means = word_count_by_role["mean"]
    bars = ax.bar(means.index, means.values, color=[C_POST, C_REPLY], edgecolor="white")
    for bar, val in zip(bars, means.values):
        ax.text(bar.get_x() + bar.get_width() / 2, val, f"{val:.1f}",
                ha="center", va="bottom", fontsize=10, fontweight="bold")
    _style_ax(ax, "Mean Words per Message – Posts vs Replies", "Role", "Mean word count")
    fig.tight_layout()
    return fig


def fig_popular_words(popular_words: dict, top_n: int = 20) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(12, max(4, top_n * 0.3)))
    for ax, role, color, label in zip(
        axes, ["post", "reply"], [C_POST, C_REPLY], ["Opening Posts", "Replies"]
    ):
        items = popular_words[role][:top_n][::-1]
        words  = [w for w, _ in items]
        counts = [c for _, c in items]
        ax.barh(words, counts, color=color, edgecolor="white", linewidth=0.5)
        _style_ax(ax, f"Top {top_n} Words – {label}", "Count")
    fig.suptitle("Most Frequent Words: Posts vs Replies (stopwords removed)",
                 fontsize=14, fontweight="bold", color=PRIMARY)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def fig_sentence_structure(sentence_by_role: pd.DataFrame) -> plt.Figure:
    metrics = [
        ("sentences_per_message",    "Sentences / message"),
        ("words_per_sentence",       "Words / sentence"),
        ("avg_word_length",          "Avg word length (chars)"),
        ("questions_per_message",    "Question marks / message"),
        ("exclamations_per_message", "Exclamation marks / message"),
        ("pct_ending_in_question",   "% messages ending in '?'"),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(13, 7))
    for ax, (col, label) in zip(axes.flat, metrics):
        vals = sentence_by_role[col]
        bars = ax.bar(vals.index, vals.values, color=[C_POST, C_REPLY], edgecolor="white")
        for bar, val in zip(bars, vals.values):
            ax.text(bar.get_x() + bar.get_width() / 2, val, f"{val:.2f}",
                    ha="center", va="bottom", fontsize=8)
        _style_ax(ax, label, "", "")
    fig.suptitle("Sentence Structure: Posts vs Replies", fontsize=14,
                 fontweight="bold", color=PRIMARY)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def fig_emoji_by_role(emoji_by_role: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    pct = emoji_by_role["pct_messages_with_emoji"]
    bars0 = axes[0].bar(pct.index, pct.values, color=[C_POST, C_REPLY], edgecolor="white")
    for bar, val in zip(bars0, pct.values):
        axes[0].text(bar.get_x() + bar.get_width() / 2, val, f"{val:.1f}%",
                     ha="center", va="bottom", fontsize=10, fontweight="bold")
    _style_ax(axes[0], "% Messages Containing Emoji", "Role", "% of messages")

    mean_cnt = emoji_by_role["mean_emoji_count"]
    bars1 = axes[1].bar(mean_cnt.index, mean_cnt.values, color=[C_POST, C_REPLY], edgecolor="white")
    for bar, val in zip(bars1, mean_cnt.values):
        axes[1].text(bar.get_x() + bar.get_width() / 2, val, f"{val:.3f}",
                     ha="center", va="bottom", fontsize=10, fontweight="bold")
    _style_ax(axes[1], "Mean Emoji Count per Message", "Role", "Mean count")
    fig.suptitle("Emoji Use by Message Role", fontsize=14, fontweight="bold", color=PRIMARY)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    return fig


def fig_top_emojis_table(top_emojis: dict, top_n: int = 20) -> plt.Figure:
    rows = max(len(top_emojis["post"]), len(top_emojis["reply"]), 1)
    fig, ax = plt.subplots(figsize=(10, max(3, rows * 0.32 + 1)))
    ax.axis("off")
    col_labels = ["#", "Posts emoji", "Count", "%", "Replies emoji", "Count", "%"]
    cell_data = []
    for i in range(min(top_n, rows)):
        p = top_emojis["post"][i]  if i < len(top_emojis["post"])  else ("", "", "")
        r = top_emojis["reply"][i] if i < len(top_emojis["reply"]) else ("", "", "")
        cell_data.append([i + 1, p[0], p[1], p[2], r[0], r[1], r[2]])
    tbl = ax.table(cellText=cell_data, colLabels=col_labels, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.4)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cell_data) + 1):
        if i % 2 == 0:
            for j in range(len(col_labels)):
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title(f"Top {top_n} Emojis – Posts vs Replies", fontsize=13,
                 fontweight="bold", color=PRIMARY, pad=10)
    fig.tight_layout()
    return fig


def fig_top_emojis_bar(top_emojis: dict, top_n: int = 15) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(12, max(4, top_n * 0.4)))
    for ax, role, color, label in zip(
        axes, ["post", "reply"], [C_POST, C_REPLY], ["Opening Posts", "Replies"]
    ):
        items = top_emojis[role][:top_n][::-1]
        emojis = [e for e, _, _ in items]
        counts = [c for _, c, _ in items]
        ax.barh(emojis, counts, color=color, edgecolor="white", linewidth=0.5)
        _style_ax(ax, f"Top {top_n} Emojis – {label}", "Count")
    fig.suptitle("Most Frequent Emojis: Posts vs Replies",
                 fontsize=14, fontweight="bold", color=PRIMARY)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def fig_unique_emojis_table(unique_emojis: dict, top_n: int = 20) -> plt.Figure:
    rows = max(len(unique_emojis["post"]), len(unique_emojis["reply"]), 1)
    fig, ax = plt.subplots(figsize=(8, max(3, min(rows, top_n) * 0.32 + 1)))
    ax.axis("off")
    col_labels = ["#", "Posts only", "Count", "Replies only", "Count"]
    cell_data = []
    for i in range(min(top_n, rows)):
        p = unique_emojis["post"][i]  if i < len(unique_emojis["post"])  else ("", "")
        r = unique_emojis["reply"][i] if i < len(unique_emojis["reply"]) else ("", "")
        cell_data.append([i + 1, p[0], p[1], r[0], r[1]])
    if not cell_data:
        cell_data = [["–", "none", "", "none", ""]]
    tbl = ax.table(cellText=cell_data, colLabels=col_labels, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.4)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor(PRIMARY)
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(cell_data) + 1):
        if i % 2 == 0:
            for j in range(len(col_labels)):
                tbl[(i, j)].set_facecolor(SECONDARY)
    ax.set_title(f"Emojis Exclusive to Each Role (top {top_n})", fontsize=13,
                 fontweight="bold", color=PRIMARY, pad=10)
    fig.tight_layout()
    return fig


# =============================================================================
# PDF entry point
# =============================================================================

def add_role_section_to_pdf(pdf, df: pd.DataFrame, stopwords: set | None = None) -> dict:
    """
    Appends the full role-based section to an open matplotlib PdfPages object.
    Returns the computed stats dict so the caller can inspect values if needed.
    """
    stats = compute_role_stats(df, stopwords=stopwords)

    def save(fig):
        pdf.savefig(fig, bbox_inches="tight")
        plt.close("all")

    save(_section_divider("Role-Based Analysis – Posts vs Replies"))
    save(fig_word_count_by_role(stats["word_count_by_role"]))
    save(fig_popular_words(stats["popular_words"]))
    save(fig_sentence_structure(stats["sentence_by_role"]))
    save(fig_emoji_by_role(stats["emoji_by_role"]))
    save(fig_top_emojis_bar(stats["top_emojis"]))
    save(fig_top_emojis_table(stats["top_emojis"]))
    save(fig_unique_emojis_table(stats["unique_emojis"]))

    return stats
