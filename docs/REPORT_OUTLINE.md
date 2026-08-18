# Report Outline — Depression Connect Psycholinguistic Analysis

Scaffold for the written report deliverable (Step 6). Each section lists **what
it argues**, **the source material already in the repo**, and **which figure/
table shows the result**. Prose is yours to write; this maps where everything
lives so you're not hunting for it.

Reference material to adapt directly: the thesis D2 literature review and D5
technical report (intro framing, LIWC background), and the docs below.

---

## 1. Introduction & motivation

- **Argues:** why the language of a depression peer-support forum is worth
  analyzing; the two analytical layers (CDS + LIWC-family markers).
- **Source:** thesis D5 intro; `README.md` overview; `DATASHEET.md` §1.
- **Figures:** none (framing).

## 2. Dataset description

- **Argues:** what the corpus is, how it was filtered, and its limits.
- **Source:** `DATASHEET.md` (composition table, provenance), `config.py`
  (exclusion rules/thresholds), `statistical_decisions.md` §3–§5 (rationale),
  `DATA_GOVERNANCE.md` (ethics/anonymization). Numbers from the regenerated EDA
  reports.
- **Figures:** EDA reports (`eda_report_all_users*.pdf`,
  `eda_report_multi_posters*.pdf`) — message/user/thread counts, activity
  distributions; datasheet composition table (45,181 msgs / 940 users / 5,495
  threads combined).
- **Note:** state the dataset-variant scheme (old / new_only / combined) and the
  March-2026 window decision (see `TO_CONFIRM.md`).

## 3. Related work → analysis mapping

- **Argues:** each prior study and which pipeline component operationalizes it.
- **Source:** `docs/studies/README.md` (both tables — Studies→Code and
  Analysis→Studies).
- **Figures:** none (a table adapted from studies/README.md).

## 4. Methods

- **Argues:** the statistical choices and why (pseudo-replication avoidance,
  BH correction, per-user aggregation, ≥5-user minimum, anonymization stripping).
- **Source:** `statistical_decisions.md` §1–§9 — this section is essentially a
  prose version of that document.
- **Figures:** none.

## 5. Results — per analysis

### 5a. Cognitive distortion prevalence (CDS)
- **Source/figures:** `cds_prevalence_report*.pdf`, `cds_category_ranking*.csv`,
  `cds_phrase_ranking*.csv`. Post-vs-reply per-user Mann-Whitney U + BH.
- **Anchor study:** 113-helpline manuscript (comparison target).

### 5b. LIWC psycholinguistic markers
- **Source/figures:** `liwc_report*.pdf`, `liwc_per_user*.csv`. Category
  prevalence, post-vs-reply, absolutist rate, FPS pronouns.
- **Anchor studies:** Smirnova 2018, Eichstaedt 2018 (FPS);
  Al-Mosaiwi & Johnstone 2018 (absolutist).

### 5c. LIWC-22 validation
- **Source/figures:** `liwc_validation_report*.pdf`,
  `liwc_validation_comparison*.csv`. Custom scorer vs official CLI (Pearson r,
  MAE). Internal cross-check — supports trusting 5b.

### 5d. Pandemic-period comparison
- **Source/figures:** `pandemic_period_report*.pdf`,
  `pandemic_period_stats*.csv`. Kruskal-Wallis + post-hoc across pre/during/post;
  variant×period confound cross-tab; single-period sensitivity.
- **Anchor study:** Yahya & Abdul Rahim 2023.
- **Key finding (combined):** FPS (`i`) and `sad` significantly lower post-period;
  replicated across custom + LIWC-22 scorers; caveat = new-export confound.

### 5e. Per-user longitudinal trends  *(see §6 zoom-in below)*
- **Source/figures:** `user_longitudinal_report*.pdf`. CDS + LIWC over time for
  the most engaged users.

## 6. Limitations

- **Argues:** external-validity bounds and known biases.
- **Source:** `DATA_GOVERNANCE.md` §6, `DATASHEET.md` §5, `statistical_decisions.md`
  (per-section caveats): single platform/language, no demographics, activity
  skew, period×variant confound, unvalidated Dutch absolutist list.

## 7. Conclusion

- **Argues:** what the markers show about this community; what's exploratory.

---

## Assembly notes

- All figures already exist as regenerated PDFs (2026-08-18) in `output/`, or are
  merged per variant in `master_report*.pdf`.
- Decide report scope: combined variant as primary, with old/new_only as
  robustness? (Recommended — the confound story lives in the contrast.)
- Open inputs before finalizing: `TO_CONFIRM.md` items (dataset section),
  formatting/length expectations (Step 8).
