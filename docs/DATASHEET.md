# Datasheet — Depression Connect Forum Corpus

Following Gebru et al. (2021), *Datasheets for Datasets*. This datasheet
documents the corpus **as used by this analysis repository** (descriptive
psycholinguistic analysis). It complements — and should be read with — the
data-governance note ([`DATA_GOVERNANCE.md`](DATA_GOVERNANCE.md)), the
statistical-methods record ([`statistical_decisions.md`](statistical_decisions.md)),
and the ethics checklist ([`../ETHICS.md`](../ETHICS.md)).

> **Items marked `⚠ TO CONFIRM` could not be verified from the repository
> alone** (they concern collection facts held by the data owner) and should be
> checked with the platform organization / Julian before this datasheet is
> treated as final.

---

## 1. Motivation

- **Why was the dataset created?** To study the language of a Dutch-language
  online depression peer-support community — cognitive distortion schemata
  (CDS), LIWC psycholinguistic markers, absolutist and first-person-pronoun
  use, and change across pandemic periods. The corpus also underpins a separate
  thesis on predicting supportive peer responses.
- **Who created and funded it?** The forum data are owned by the platform
  organization (⚠ TO CONFIRM exact legal entity — inferred as the Dutch
  Depression Association / Pro Persona ecosystem from affiliated Smit et al.
  publications). Research use is by Claudia Yáñez (Tilburg University, MSc Data
  Science & Society) under a data-sharing agreement. ⚠ TO CONFIRM funding source.

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

- **How was the data acquired?** Exported by the platform owner from the live
  forum database and shared under agreement; not scraped or re-collected by the
  researcher. ⚠ TO CONFIRM export dates and extraction method.
- **Sampling.** Not a sample — the exports are intended as complete forum
  content for the covered period, minus the exclusions in §4. ⚠ TO CONFIRM
  completeness of each export.
- **Consent.** No new data collected from participants; the owner pseudonymized
  the data before sharing. ⚠ TO CONFIRM the consent basis under which forum
  users' content may be used for research (e.g. platform terms of service /
  broad consent at registration).
- **Time frame of collection.** Message timestamps span 2019–2026 (see §2);
  the export/handover dates to the researcher are ⚠ TO CONFIRM.

## 4. Preprocessing / cleaning / labeling

Applied by `preprocess.py` then `postprocess.py` (rationale in
`statistical_decisions.md` §3–§5):

- **Account exclusions:** superuser accounts (test = 1, demo = 4) and their
  posters removed; the two real communities (2, 3) retained.
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
- **IP / terms.** Owned by the platform organization, which retains ownership
  during and after the project. ⚠ TO CONFIRM the precise redistribution and
  retention terms of the agreement.

## 7. Maintenance

- **Who maintains it here?** The researcher, for the duration of the project;
  the authoritative copy and any erasure requests are handled by the data owner
  (`ETHICS.md` B.4).
- **Retention.** Not retained beyond the research scope. ⚠ TO CONFIRM a specific
  deletion date/plan with the data owner.
- **Will it be updated?** Possibly, if a further export is supplied — it would be
  integrated as an additional variant rather than overwriting existing ones.

---

*Datasheet framework: Gebru, T., Morgenstern, J., Vecchione, B., Vaughan, J. W.,
Wallach, H., Daumé III, H., & Crawford, K. (2021). Datasheets for Datasets.
Communications of the ACM, 64(12), 86–92.*
