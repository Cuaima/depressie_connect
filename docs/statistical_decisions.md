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

The test is only run when both role groups contain at least 5 users; below that, the test statistics are reported as missing (`NaN`) rather than computed on unstable groups.

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

**On the two community accounts (2 and 3) — both are retained.**
The forum has two community sections: account 2 (`community.depressieconnect.nl`, for people with depression) and account 3 (`naasten.depressieconnect.nl`, for *naasten* — relatives and companions of people with depression). Only the test (1) and demo (4) accounts are excluded; both community sections stay in.

The account marks the **forum section a message was posted in — its topic area — not the poster's role.** The same person can post in either section, so a message's account does not indicate whether its author has depression or is a relative. The data therefore cannot be split into "depression" vs "relatives" user populations by account, and neither section is excluded on that basis. Any write-up should describe the population as the peer-support community of this forum as a whole, not as "people with depression, relatives excluded."

**Why moderators are excluded:**  
Moderators post differently from community members by role: they welcome new users, enforce rules, and redirect conversations. Including them would conflate staff communication patterns with peer-to-peer support dynamics, which is the subject of the study.

**Why intro/off-topic groups are excluded:**  
Introduction threads and recreational groups (poetry, word games, off-topic chat) contain fundamentally different linguistic content from the support-seeking threads that are the object of analysis. Mixing them would dilute CDS and LIWC signals from the therapeutic context.

---

## 5. Analysis input: postprocessed data (`messages_structured.csv`), not raw preprocessed

**Where:** All analysis scripts (`cds_prevalence.py`, `liwc_analysis.py`, `exploration.py`, `exploratory_analysis.py`, `eda_report.py`, `user_longitudinal.py`, `full_report.py`)  
**What:** Analysis scripts read from `output/messages_structured.csv` (written by `postprocess.py`), not from `output/preprocessed/messages_community.csv` (written by `preprocess.py`).

**Why:**  
`messages_community.csv` contains all messages that passed language detection and pseudonymisation, including intro/welcome groups and single-post users. `messages_structured.csv` additionally has the group exclusions (§4), the user threshold (§3), thread structure flags (`is_initial_post`, `reply_index`, `thread_has_replies`), and the normalised text column (`text_normalized`). Using the structured file ensures every analysis script operates on the same, consistently filtered population.

**Pseudonymization placeholders are stripped before analysis.** NER-based text masking replaces entities with placeholders like `[ENTITY_PERSON_1]`. These stay in the stored `MessageText` (they carry the masking) but are removed before any scoring or tokenization — both in `text_normalized` (postprocess) and at text load in every analysis script (`utils/thread_utils.strip_entity_placeholders`). Left in, they tokenize into words (`of`, `work`, `art`) that inflate LIWC function-word scores, word counts, and word-frequency charts.

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

---

## 9. Pandemic-period comparison: per-user Kruskal-Wallis, not corpus log-likelihood

> **Scope note (2026-08-19): this analysis is exploratory only.** The project
> scope was capped at 2022 with the `old` variant as the basis for all main
> findings, because message source and calendar time are confounded past 2022
> (the "post" period is almost entirely the new export). The pandemic comparison
> requires the post-2022 data, so it cannot be a source of reported findings; it
> is retained as documented, exploratory context. No feature reaches significance
> in the all-users omnibus in either variant regardless (see the results).

**Where:** `pandemic_period_analysis.py`; period boundaries in `config.py` (`PANDEMIC_CUTOFF_DATE`, `PANDEMIC_END_DATE`)
**What:** Psycholinguistic features (Yahya & Abdul Rahim 2023, §2.3: pronouns, function words, tenses, emotion, informal language, absolutist words) are compared across three pandemic periods.

**Period definitions:**

| Period | Boundary | Justification |
|---|---|---|
| `pre` | PostDate < 2020-03-11 | WHO pandemic declaration |
| `during` | 2020-03-11 ≤ PostDate < 2022-03-23 | — |
| `post` | PostDate ≥ 2022-03-23 | End of most Dutch COVID restrictions (chosen over the WHO emergency-end date 2023-05-05 because the forum population is Dutch; decision by the researcher, 2026-08-17) |

**Deliberate departure from the source paper:**
Yahya & Abdul Rahim compare pooled corpora with log-likelihood tests, which treats every token/message as independent — the same pseudo-replication problem as §2. We instead aggregate to per-user mean rates within each period, then test:

1. **Omnibus:** Kruskal-Wallis across the three periods, per feature (epsilon-squared effect size).
2. **Post-hoc:** pairwise Mann-Whitney U (pre/during, during/post, pre/post), only for features whose omnibus test survives BH correction (rank-biserial effect size, as §2).
3. **Correction:** BH-FDR applied twice — first within the omnibus family to select features for post-hoc testing, then across the pooled set of all p-values (omnibus + post-hoc), which is what the reported `p_bh` column reflects.

Groups with fewer than 5 users are excluded from testing (reported as skipped, never silently).

**Confound — period × dataset variant:**
The old export covers roughly 2019–2022 (pre + during), the new export roughly 2022 onward (essentially post). Period effects are therefore partially confounded with export source. The report's first diagnostic page is a `dataset_variant × pandemic_period` cross-tab (messages and unique users per cell) making this visible; near-empty cells are flagged. Interpret period contrasts on the `combined` variant with this confound in mind.

**Independence limitation and sensitivity analysis:**
Users who post in more than one period appear in more than one group, violating between-group independence (same class of issue as the §2 post/reply overlap). The report quantifies how many users appear in 1, 2, and 3 periods, and repeats the full analysis restricted to single-period users. Both result sets are written side by side (`analysis` column: `all_users` vs `single_period`) in `pandemic_period_stats.csv`. Divergence between the two indicates the multi-period users are driving results. A fully correct approach would be a mixed-effects model with user as a random effect; the sensitivity design was chosen for transparency and consistency with §2.

**Dictionary availability:**
Only categories actually present in the available dictionaries are analyzed (custom LIWC-2015 scorer output and/or LIWC-22 CLI output, labeled by source suffix `__liwc2015` / `__liwc22`). The report lists which Yahya categories are genuinely absent from each source — notably LIWC-22's `Analytic` summary variable, which is unavailable with an external Dutch dictionary (see §5 of the LIWC-22 validation report). Absolutist words come from `src/utils/absolutist.py` (§7).
