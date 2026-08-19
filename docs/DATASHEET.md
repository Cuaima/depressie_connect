# Datasheet — Depression Connect Forum Corpus

Following Gebru et al. (2021), *Datasheets for Datasets*. This datasheet
documents the corpus **as used by this analysis repository** (descriptive
psycholinguistic analysis). It complements — and should be read with — the
data-governance note ([`DATA_GOVERNANCE.md`](DATA_GOVERNANCE.md)), the
statistical-methods record ([`statistical_decisions.md`](statistical_decisions.md)),
and the ethics checklist ([`../ETHICS.md`](../ETHICS.md)).

> **Items marked `⚠ TO CONFIRM` could not be verified from the repository
> alone** (they concern collection facts held by the data owner) and should be
> checked with the platform organization before this datasheet is treated as
> final. Most such items were answered in supervisor correspondence with Janna
> on 19 August 2026 and are now written in below; the remaining open items are
> tracked in [`TO_CONFIRM.md`](TO_CONFIRM.md).

---

## 1. Motivation

- **Why was the dataset created?** To study the language of a Dutch-language
  online depression peer-support community — cognitive distortion schemata
  (CDS), LIWC psycholinguistic markers, absolutist and first-person-pronoun
  use, and change across pandemic periods. The corpus also underpins a separate
  thesis on predicting supportive peer responses.
- **Who created and funded it?** The forum data are owned by the **Depressie
  Vereniging** (the Dutch Depression Association), an association; **MEO**
  (<https://wijzijnmeo.nl>, an internet agency) currently hosts the platform and
  collects the data on the association's behalf, as processor rather than owner.
  Development of the Depression Connect platform was funded by a **ZonMw grant**
  with **Prof. Jan Spijker** as principal investigator; the Depressie Vereniging
  subsequently took the platform over and funds it. Research use is by Claudia
  Yáñez (Tilburg University, MSc Data Science & Society) under a data-sharing
  agreement; no separate funding attaches to that research use.
  *(Confirmed 19 Aug 2026; the exact registered legal name of the association is
  the one remaining detail to verify — see `TO_CONFIRM.md`.)*

## 2. Composition

The analysis operates on three dataset **variants** produced by
`integrate_datasets.py` from two platform exports (a legacy "old" export and a
newer export), then filtered by `preprocess.py` / `postprocess.py`. Counts below
are the **post-filtering** `messages_structured*.csv` used by all analysis
scripts (regenerated 2026-08-18 on the final config):

| Variant | Messages | Users | Threads | Date range |
|---|---:|---:|---:|---|
| `old` | 18,550 | 427 | 2,024 | 2019-06-19 – 2022-10-05 |
| `new_only` | 25,504 | 530 | 3,388 | 2019-05-17 – 2026-03-30 |
| `combined` (default) | 45,181 | 940 | 5,495 | 2019-05-17 – 2026-03-30 |

- **What does each instance represent?** One forum message (a post or a reply),
  with its pseudonymous author ID, thread ID, group ID, timestamp, and text.
  Thread structure flags (`is_initial_post`, `reply_index`, `reply_count`,
  `thread_has_replies`) are added in postprocessing.
- **Raw structure of the two exports.** They differ: the old export is
  relational (four CSVs), the new export is a flat bbPress dump with no lookup
  tables and no account column. Documented, with diagrams, in
  [`DATA_SCHEMA.md`](DATA_SCHEMA.md).
- **Note on the date range.** The new export extends the corpus to **March 2026**
  — later than the thesis dataset (which ends Oct 2022). The `old` and `combined`
  variants therefore differ from the thesis's reported figures; do not cite
  thesis counts for this corpus.
- **Scope decision (2026-08-19): analysis capped at 2022.** Because source and
  time are confounded past 2022, main findings use the `old` variant (Jun 2019 –
  Oct 2022); `new_only` and `combined` are exploratory context only. See
  `TO_CONFIRM.md` and report §2.5.
- **Labels?** No manually annotated labels in this repo (the supportive-reply
  annotation lives in the thesis project). Derived signals only: CDS category
  matches, LIWC scores, role (post/reply), pandemic period.
- **Missing data?** Messages with no parseable date or empty text are dropped at
  load. Missingness is not otherwise imputed.
- **Sensitive content?** Yes — first-person accounts of depression and related
  distress from vulnerable individuals. Handled under the storage and
  pseudonymization rules in `DATA_GOVERNANCE.md`.

## 3. Collection process

- **How was the data acquired?** Exported by the platform side from the live
  forum database and shared under agreement; not scraped or re-collected by the
  researcher. The **old** export was pulled **when the original platform host's
  contract ended**; the **new** export was pulled **via MEO**, the current host.
  The **new** export's extraction date is ≈ its handover (6 March
  2026; content runs to 30 March 2026). The **old** export's extraction date is
  ~late 2022, inferred from its content ending 5 Oct 2022 (it would be the date
  the original host's contract ended); it is **not** recorded in the Smit
  papers, whose extractions cluster around 2020 (the quantitative survey data
  were extracted 24 Sept 2020). The Smit papers do corroborate the
  start of the corpus: the forum "launched in mid-2019", matching the old data's
  first message on 19 June 2019.
- **Handover dates.** Old export handed to the researcher on **31 October 2025**;
  new export on **6 March 2026**.
- **Sampling / completeness.** Each export is intended as complete forum content,
  but the two are not perfectly consistent. They overlap partially, and the new
  export's content from *before the change of hands* appears to have been
  back-propagated and is not reliable. This shows in the data: the new export has
  almost no pre-2022 content (67 messages in 2019, 140 in 2020, none in 2021).
  This is a further reason main findings use the old variant and cap at 2022
  (§2).
- **Consent.** No new data collected from participants; the owner pseudonymized
  the data before sharing. Research use rests on **consent given at
  registration**: on signing up for the platform, users agree that their data may
  be used for research. This is covered by a passive consent form that also
  carries the platform's other terms and conditions of use. Because consent is
  obtained through platform registration in this way, **no separate ethics
  approval** was sought for this dataset, and none was required.
  *(Confirmed 19 Aug 2026.)*
- **Time frame of collection.** Message timestamps span 2019–2026 (see §2).
  Handover: old export 31 Oct 2025, new export 6 March 2026 (above).

## 4. Preprocessing / cleaning / labeling

Applied by `preprocess.py` then `postprocess.py` (rationale in
`statistical_decisions.md` §3–§5):

- **Account exclusions:** superuser accounts (test = 1, demo = 4) and their
  posters removed. Both community sections are retained: account 2 (the
  depression community) and account 3 (`naasten`, for relatives/companions).
  The account marks the forum *section* a message was posted in, not the
  poster's role — the same person can post in either — so the data cannot be
  split into "depression" vs "relatives" users by account, and neither section
  is excluded.
- **Moderator exclusion:** 8 confirmed moderator UUIDs removed, including
  moderator-initiated threads (`config.MODERATOR_POSTER_IDS`; discovery via
  `scripts/find_moderators.py`).
- **Group exclusions:** intro/welcome and off-topic/recreational groups dropped
  (`INTRO_GROUP_KEYWORDS`).
- **Quality filters:** messages shorter than `MIN_WORD_COUNT = 5` words dropped;
  users with fewer than `MIN_POSTS_PER_USER = 5` total posts excluded.
- **Pseudonymization (two layers):** author IDs replaced with pseudonyms via a
  stored mapping; NER-based text masking (spaCy `nl_core_news_lg`) replacing
  entities with `[ENTITY_*]` placeholders. The data are pseudonymized, not
  anonymized (free text may still identify; see `DATA_GOVERNANCE.md` §4).
  Placeholders remain in stored text but are **stripped before any
  scoring/tokenization** (`utils/thread_utils.strip_entity_placeholders`).
- **Normalization:** lowercased `text_normalized` column with whitespace and
  repeated-character normalization; placeholders removed.
- **Is raw data retained?** The researcher holds the raw exports locally; they
  are **excluded from version control** (`.gitignore`) and are not distributed.

## 5. Uses

- **Used in this repo for:** descriptive EDA, CDS prevalence (post vs. reply,
  per-user Mann-Whitney U), LIWC / LIWC-22 scoring and validation, absolutist
  and first-person-pronoun markers, and the pandemic-period comparison.
- **Also usable for / used elsewhere:** the thesis classifier (predicting
  supportive peer replies).
- **Recommended use of the three variants.** Every analysis and report script
  takes a `--dataset` flag and writes one output per variant, named by the
  convention in `dataset_io.suffix`: `_old`, `_new_only`, and **no suffix for
  `combined`**, which keeps the legacy filenames (e.g.
  `messages_structured_old.csv`, `messages_structured_new_only.csv`,
  `messages_structured.csv`). `preprocess.py` and `postprocess.py` follow the
  same convention. The one exception is the integration step, which writes
  bespoke names — `messages_old.csv`, `messages_new_only.csv`, and
  `integrated_messages.csv` for combined (`dataset_io.integrated_input_path`).

  **Anyone working with this corpus is advised to treat the old and the new
  export as two different datasets** rather than one continuous corpus: they
  come from different platform hosts, have different raw schemas
  (`DATA_SCHEMA.md`), and their author identifiers are matched only
  inferentially. The `combined` variant is provided for convenience and
  completeness, but it is the least reliable of the three and should not carry a
  claim on its own. In descending order of reliability:

  1. **`old`** — internally consistent, relational, and the most thoroughly
     verified; carries the section and group structure. All main findings in
     this project rest on it.
  2. **`new_only`** — internally consistent on its own terms, but flat: no
     account/section information, and its pre-handover content is
     back-propagated and unreliable (§3), so it is dependable mainly from 2022
     onward.
  3. **`combined`** — additionally depends on the behavioural ID bridge and on
     cross-export deduplication, and confounds message source with calendar
     time. Useful as context and for the confound diagnostics; treat any result
     computed on it as exploratory.
- **Uses to avoid.** Not for clinical risk screening or individual mental-health
  assessment; no demographic metadata exists, so no fairness analysis across
  protected groups is possible. Findings are bounded to this single Dutch-language
  platform and time window (external-validity limits in `DATA_GOVERNANCE.md` §6).
- **Known bias.** User activity is highly skewed toward a small number of
  prolific contributors; per-user aggregation and the activity threshold
  mitigate but do not remove this. The `old`/`new` exports are confounded with
  pandemic period (old ≈ pre+during, new ≈ post), surfaced explicitly in the
  pandemic report's variant × period cross-tab.

## 6. Distribution

- **Will it be distributed?** No. The corpus is private under the data-sharing
  agreement and subject to GDPR; it is not deposited publicly and has no DOI.
- **Version identification.** Identified by date range and raw row counts logged
  in the preprocessing report after a real-data run (no public DOI possible).
- **IP / terms.** Owned by the Depressie Vereniging, which retains ownership
  during and after the project. The agreement is not believed to impose specific
  redistribution restrictions beyond academic-research-only use, but ⚠ TO CONFIRM
  against the signed agreement text — this was left open in the 19 Aug 2026
  correspondence ("we would have to check").

## 7. Maintenance

- **Who maintains it here?** The researcher, for the duration of the project;
  the authoritative copy and any erasure requests are handled by the data owner
  (`ETHICS.md` B.4).
- **Retention.** Not retained beyond the research scope. No required deletion
  date or retention limit is believed to be imposed by the agreement, but this
  was left open on 19 Aug 2026 and ⚠ TO CONFIRM against the agreement text.
- **Will it be updated?** Possibly, if a further export is supplied — it would be
  integrated as an additional variant rather than overwriting existing ones.

---

*Datasheet framework: Gebru, T., Morgenstern, J., Vecchione, B., Vaughan, J. W.,
Wallach, H., Daumé III, H., & Crawford, K. (2021). Datasheets for Datasets.
Communications of the ACM, 64(12), 86–92.*
