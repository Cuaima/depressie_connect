# Statistical Design Decisions

This document records the analytical choices made in the pipeline and the reasoning behind them. It is intended as a methods reference for write-up and for anyone revisiting the code.

---

## 1. Multiple-comparison correction: Benjamini–Hochberg FDR

**Where:** `cds_prevalence.py` → `compute_category_ranking()`  
**What:** After running a significance test for each of the 12 CDS categories, p-values are corrected using the Benjamini–Hochberg (BH) false discovery rate procedure.

**Why:**  
Running 12 separate tests at α = 0.05 would produce on average 0.6 false positives by chance even with no real signal. BH correction controls the *expected proportion* of discoveries that are false (the FDR), rather than the family-wise error rate (FWER) like Bonferroni. BH is preferred here because:

- The categories are correlated (a message containing one cognitive distortion often contains others), which makes FWER correction overly conservative.
- We care about the rate of false discoveries in a ranked list of categories, not about guaranteeing zero false positives.

**Output columns:** `p_value` (uncorrected), `p_value_bh` (BH-corrected), `sig_after_correction` (bool, threshold 0.05).

---

## 2. CDS category test: Mann-Whitney U on per-user rates, not chi-square on messages

**Where:** `cds_prevalence.py` → `compute_category_ranking()`  
**What:** The post-vs-reply comparison uses Mann-Whitney U applied to *per-user* mean CDS rates, rather than a chi-square test on raw message counts.

**Why — the pseudo-replication problem:**  
Chi-square on message counts treats every message as an independent observation. This assumption fails on a forum: messages from the same user share that user's writing style, vocabulary, and psychological state. A small number of very prolific users with a strong tendency toward a particular category (e.g., one user who uses catastrophising language in 60% of their posts) can drive a statistically significant chi-square result even after BH correction — not because there is a real population-level difference between posts and replies, but because that user wrote a lot.

This is a form of *pseudo-replication*: inflating the effective sample size beyond the number of independent units (users).

**The fix:**  
For each category and each role (`post` / `reply`), we first aggregate to the user level: each user's rate for that role is the mean of their CDS indicator across all their messages in that role. We then compare the distribution of per-user rates between roles using Mann-Whitney U. Each user contributes at most one observation per role, which satisfies the independence assumption at the right level of analysis.

**Effect size:** Rank-biserial correlation, `r = 1 − 2U / (n₁ · n₂)`. This is the natural effect size for Mann-Whitney U and is interpretable as the probability that a randomly drawn post-user has a higher CDS rate than a randomly drawn reply-user, rescaled to [−1, 1].

**Limitation to note in write-up:**  
Users who post but never reply, and those who reply but never post, are placed in separate groups. However, users who do both appear in *both* groups, meaning those two groups are not fully independent. Treating them as independent is a mild conservatism (slightly deflates the test statistic) but far preferable to message-level pseudo-replication. A fully correct approach would use a paired test or a mixed-effects model with user as a random effect.

**Practical implication:**  
The `n_users_posts` and `n_users_replies` columns in the output table will be smaller than the message counts. On a depression support forum, many users post once (seeking support) and never reply. The reply group is therefore dominated by high-engagement community members, who are a self-selected subset. The post-vs-reply comparison measures a combination of *role* (opener vs responder) and *engagement level*, which should be acknowledged in interpretation.

---

## 3. User inclusion threshold

**Where:** `config.py` → `MIN_POSTS_PER_USER`; `postprocess.py` → `filter_min_posts()`  
**What:** Users with fewer than `MIN_POSTS_PER_USER` total posts are excluded from `messages_structured.csv` before any analysis.

**Why:**  
Single-post users are common on mental health forums: someone creates an account to ask for help once and never returns. Including them:

- Inflates the apparent number of unique voices (overstates community breadth).
- Introduces noise into per-user statistics (activity span, engagement rate, CDS rate), since one post is not enough to estimate a user-level tendency reliably.
- Disproportionately affects the Mann-Whitney U test: a large number of users with exactly one post all have rates of either 0% or 100%, which distorts the distribution.

The threshold is set in `config.py` and applied once in `postprocess.py` so all downstream scripts see a consistent population.

**Default:** `MIN_POSTS_PER_USER = 5` (set by the researcher after reviewing the distribution).

---

## 4. Group and user exclusions

**Where:** `config.py`, `preprocess.py`, `postprocess.py`  
**What:** Several exclusion layers are applied before analysis.

| Layer | Where applied | What is removed |
|---|---|---|
| Account type | `preprocess.py` | Accounts 1 (test) and 4 (demo) and all their users |
| Moderators | `preprocess.py` | 8 confirmed moderator UUIDs (identified via `scripts/find_moderators.py`, hardcoded in `config.py`) |
| Off-topic groups | `postprocess.py` | Threads in groups matching `INTRO_GROUP_KEYWORDS` (welcome, poems, off-topic, games, etc.) |
| Low-activity users | `postprocess.py` | Users below `MIN_POSTS_PER_USER` (see §3) |

**Why moderators are excluded:**  
Moderators post differently from community members by role: they welcome new users, enforce rules, and redirect conversations. Including them would conflate staff communication patterns with peer-to-peer support dynamics, which is the subject of the study.

**Why intro/off-topic groups are excluded:**  
Introduction threads and recreational groups (poetry, word games, off-topic chat) contain fundamentally different linguistic content from the support-seeking threads that are the object of analysis. Mixing them would dilute CDS and LIWC signals from the therapeutic context.

---

## 5. Analysis input: postprocessed data (`messages_structured.csv`), not raw preprocessed

**Where:** All analysis scripts (`cds_prevalence.py`, `liwc_analysis.py`, `exploration.py`, `exploratory_analysis.py`, `eda_report.py`, `user_longitudinal.py`, `full_report.py`)  
**What:** Analysis scripts read from `output/messages_structured.csv` (written by `postprocess.py`), not from `output/preprocessed/messages_community.csv` (written by `preprocess.py`).

**Why:**  
`messages_community.csv` contains all messages that passed language detection and anonymisation, including intro/welcome groups and single-post users. `messages_structured.csv` additionally has the group exclusions (§4), the user threshold (§3), thread structure flags (`is_initial_post`, `reply_index`, `thread_has_replies`), and the normalised text column (`text_normalized`). Using the structured file ensures every analysis script operates on the same, consistently filtered population.

---

## 6. First-person singular pronouns as a depression marker

**Where:** `liwc_analysis.py`, `user_longitudinal.py`, `full_report.py`  
**What:** First-person singular pronoun (FPS) usage is extracted as a dedicated feature: Dutch pronouns `ik`, `mij`, `me`, `mijn`, `mezelf`.

**Why:**  
Elevated first-person singular pronoun use is one of the most replicated linguistic markers of depression (Rude et al. 2004; Kacewicz et al. 2014). It reflects self-focused attention, which is a core feature of depressive cognition. If the LIWC dictionary does not include a first-person singular category (`i`), `ensure_fps()` in `liwc_analysis.py` adds a `fps_dutch` category covering the Dutch pronouns, so the feature is always available regardless of dictionary version.

---

## 7. Absolutist words

**Where:** `src/utils/absolutist.py`, called from `liwc_analysis.py` and `full_report.py`  
**What:** A Dutch absolutist word list is scored per message to produce an `absolutist_rate` feature (absolutist words / total words).

**Why:**  
Al-Mosaiwi & Johnstone (2018) found that absolutist thinking — expressed through words like "always", "never", "completely", "nothing" — is elevated in individuals with depression and anxiety, even compared to other negative-affect groups. The effect held on forum data. This feature is distinct from LIWC categories and provides an additional lexical marker of cognitive rigidity.

**Note:** The Dutch word list was constructed by translation from the English original and has not been independently validated on Dutch text. Treat as exploratory until reviewed by a native Dutch speaker familiar with the clinical context.

---

## 8. Role-based analyses: mean-only metrics and future testing

**Where:** `role_analysis.py`  
**What:** Sentence structure, emoji rate, and popular-word frequency are currently reported as means only (no significance tests).

**Note for future work:**  
These distributions are right-skewed (a few very long posts, a few emoji-heavy users). If significance tests are added, do **not** use a t-test. Use Mann-Whitney U or bootstrap confidence intervals, for the same pseudo-replication reason as §2: aggregate to per-user rates before testing.
