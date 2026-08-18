# Data Governance & Ethics — Analysis Pipeline

Scope note for the **descriptive analysis** repository (CDS prevalence, LIWC /
LIWC-22, absolutist & first-person-pronoun markers, pandemic-period
comparison). This document covers the ethical surface that actually applies
here: **data collection, storage, and analysis**. It does *not* duplicate the
modeling/deployment assessment — that lives in the thesis ELSA
(`reporting/elsa.md`, Deliverable D6) in the
[`dss_thesis`](https://gitlab.com/Cuaima1/dss_thesis) repository, which shares
the same underlying dataset. The checklist form of the below is
[`../ETHICS.md`](../ETHICS.md).

---

## 1. Data provenance & legal basis

- **Source:** Depression Connect, a Dutch-language peer-support forum managed by
  a mental health organization (the Dutch Depression Association / Pro Persona
  ecosystem).
- **Agreement:** obtained under a formal data-sharing agreement between Tilburg
  University and the platform organization, for academic research only. The data
  owner retains ownership during and after the project.
- **Consent & approval:** no new data were collected from human participants.
  The data owner pseudonymized the data before sharing (direct identifiers
  removed). No additional ethical approval was required under these conditions.
- **Legal regime:** the data remain sensitive mental-health content from
  vulnerable individuals and stay subject to GDPR obligations and the agreement
  terms even after pseudonymization.

## 2. What this repository processes

- Four relational CSVs (`accounts`, `groups`, `topics`, `messages`) covering
  **10 Apr 2019 – 05 Oct 2022**.
- Derived, filtered datasets (`messages_structured*.csv`) and analysis outputs
  (PDF reports, score CSVs, the Excel export).

## 3. Storage & access controls

- **Raw data are never committed.** `data/` inputs, anonymization mapping files,
  and entity-review CSVs are excluded via `.gitignore`. Verify before every push
  that no raw `messages.csv`, mapping, or `*_entity_review*` file is staged.
- Analysis runs locally against the researcher's copy of the restricted data.
- **Right to erasure** is handled upstream by the data owner (see `ETHICS.md`
  B.4); this repo holds only a working copy and retains no data beyond the
  research scope.
- **Version identification:** the dataset has no public DOI (it is private).
  It is identified by its date range and raw row counts, logged in the
  preprocessing report after a real-data run.

## 4. Anonymization (two layers)

1. **ID pseudonymization** — `PosterID` values are pseudonymous; superuser
   (test/demo) and confirmed-moderator accounts are excluded in `preprocess.py`.
2. **Text anonymization** — NER-based entity replacement
   (`custom_text_anonymizer`, spaCy `nl_core_news_lg`) rewrites persons, places,
   works, etc. as `[ENTITY_*]` placeholders in `MessageText`. This is a **loud**
   dependency: if the anonymizer is unavailable the pipeline raises rather than
   emitting un-anonymized text.
   - Placeholders are kept in the stored text (they *are* the anonymization) but
     are **stripped before any scoring/tokenization** so they cannot leak into
     LIWC counts or word frequencies (`utils/thread_utils.strip_entity_placeholders`;
     see `statistical_decisions.md` §5).

## 5. Analysis-integrity commitments

These are the "honest representation" (checklist C.3) commitments specific to
what this repo computes:

- **No pseudo-replication.** All inferential tests aggregate to the user level
  before testing (`statistical_decisions.md` §2, §9). Message-level tests are
  treated as invalid.
- **Multiple-comparison control.** Benjamini–Hochberg FDR across category /
  feature families (§1).
- **Confounds surfaced, not buried.** The pandemic-period report leads with a
  `dataset_variant × period` cross-tab because the old/new exports are
  confounded with pre/post periods (§9), and runs a single-period-user
  sensitivity analysis.
- **Skew acknowledged.** User activity is highly skewed (a small number of
  prolific repliers dominate); per-user aggregation and the `MIN_POSTS_PER_USER`
  threshold (§3) mitigate but do not remove this. Interpretation is scoped
  accordingly.
- **Exploratory markers flagged.** The Dutch absolutist word list is a
  translation not yet validated by a native speaker and is labelled exploratory
  in code and docs (§7).

## 6. Representativeness & limitations (external validity)

Single platform, single language (Dutch), fixed time window, no demographic
metadata (no gender/age/ethnicity — so no disaggregation along protected
characteristics is possible). Findings are not claimed to generalize beyond this
population without further validation.

## 7. Not covered here (by design)

Modeling fairness, train/test leakage, model cards, hyperparameter transparency,
deployment redress/rollback/drift, and unintended-use monitoring — all belong to
the thesis classifier and are assessed in its ELSA report. See `../ETHICS.md`
sections D and E (marked N/A here) and `dss_thesis/reporting/elsa.md`.

## 8. Open governance items

- `docs/DATASHEET.md` (Gebru et al. "Datasheets for Datasets") is **not yet
  written** for this repo; `ETHICS.md` A.6 / footnote 2 anticipate it.
