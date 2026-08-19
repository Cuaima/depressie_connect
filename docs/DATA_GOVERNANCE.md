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

- **Source:** Depression Connect, a Dutch-language peer-support forum owned by
  the **Depressie Vereniging** (the Dutch Depression Association). The platform
  is currently hosted and the data collected by **MEO**
  (<https://wijzijnmeo.nl>, an internet agency) on the association's behalf —
  processor, not owner. The platform's development was funded by a **ZonMw
  grant** (PI: Prof. Jan Spijker); the association later took it over and funds
  it. *(Confirmed with Janna, 19 Aug 2026; exact registered legal name of the
  association still to be verified — `DATASHEET.md` §1, `TO_CONFIRM.md`.)*
- **Agreement:** obtained under a formal data-sharing agreement between Tilburg
  University and the data owner, for academic research only. The data owner
  retains ownership during and after the project.
- **Consent & approval:** no new data were collected from human participants.
  Research use rests on **consent given at platform registration** — users agree
  on sign-up that their data may be used for research, via a passive consent form
  that also carries the platform's terms and conditions of use. Because consent
  is obtained this way, **no separate ethics approval** exists for this dataset
  and none was required. The data owner pseudonymized the data before sharing
  (direct identifiers removed).
- **Legal regime:** the data remain sensitive mental-health content from
  vulnerable individuals and stay subject to GDPR obligations and the agreement
  terms even after pseudonymization.

## 2. What this repository processes

- **Old export:** four relational CSVs (`accounts`, `groups`, `topics`,
  `messages`) covering **10 Apr 2019 – 05 Oct 2022**.
- **New export:** flat bbPress message exports in `data/new/`, split by year
  range, with no lookup tables and no account/section column. Schemas and their
  reconciliation: [`DATA_SCHEMA.md`](DATA_SCHEMA.md).
- Derived, filtered datasets (`messages_structured*.csv`) and analysis outputs
  (PDF reports, score CSVs, the Excel export).

## 3. Storage & access controls

- **Raw data are never committed.** `data/` inputs, the pseudonymization mapping
  files, and entity-review CSVs are excluded via `.gitignore`. Verify before every
  push that no raw `messages.csv`, mapping, or `*_entity_review*` file is staged.
- Analysis runs locally against the researcher's copy of the restricted data.
- **AI tooling is blocked from the data directories.** `.claude/settings.json`
  is committed (not local-only) and denies `Read(data/**)`, `Read(output/**)`,
  and `Read(jic/**)`, so the rules apply to every clone and session. Combined
  with the `.gitignore` exclusions above, no forum content is reachable from the
  repository. Full disclosure of where AI was and was not used, and the limits
  of these controls: [`AI_USE.md`](AI_USE.md).
- **Right to erasure** is handled upstream by the data owner (see `ETHICS.md`
  B.4); this repo holds only a working copy and retains no data beyond the
  research scope.
- **Version identification:** the dataset has no public DOI (it is private).
  It is identified by its date range and raw row counts, logged in the
  preprocessing report after a real-data run.

## 4. Pseudonymization (two layers)

The data are treated as **pseudonymized, not anonymized**: free text can still
carry identifying detail that no automatic step is guaranteed to catch, so
complete anonymity is not claimed and the data remain personal data under GDPR
and the data-sharing agreement.

1. **ID pseudonymization** — `PosterID` values are replaced with pseudonyms
   through a stored mapping; superuser (test/demo) and confirmed-moderator
   accounts are excluded in `preprocess.py`.
2. **Text pseudonymization** — NER-based entity replacement (the
   `custom_text_anonymizer` module, spaCy `nl_core_news_lg`) rewrites persons,
   places, works, etc. as `[ENTITY_*]` placeholders in `MessageText`. This is a
   **loud** dependency: if the component is unavailable the pipeline raises
   rather than emitting text with its entities un-masked.
   - Placeholders are kept in the stored text (they carry the masking) but are
     **stripped before any scoring/tokenization** so they cannot leak into
     LIWC counts or word frequencies (`utils/thread_utils.strip_entity_placeholders`;
     see `statistical_decisions.md` §5).

*(The code module is named `custom_text_anonymizer` for historical reasons; the
process it performs is pseudonymization as described above.)*

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

- `docs/DATASHEET.md` (Gebru et al. "Datasheets for Datasets") is **written**
  and satisfies `ETHICS.md` A.6 / footnote 2.
- **Redistribution & retention terms** of the data-sharing agreement are not yet
  verified against the signed text (`ETHICS.md` B.3/B.4, `DATASHEET.md` §6–§7).
  No deletion date or retention limit is believed to be imposed, but this is
  unconfirmed.
- Two provenance details remain open: the legacy export's exact extraction date
  and the data owner's exact registered legal name. Tracked in
  [`TO_CONFIRM.md`](TO_CONFIRM.md).
