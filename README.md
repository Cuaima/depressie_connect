# Depression Connect Project

Research pipeline for processing and analysing Dutch depression forum data. The pipeline cleans raw forum exports, anonymizes user identifiers and message text using NER, structures threads, and extracts psycholinguistic features (LIWC) for downstream classification tasks.

## Project structure

```
depression_connect_project/
├── data/                          # Raw input CSVs (gitignored)
│   ├── accounts.csv
│   ├── groups.csv
│   ├── topics.csv
│   ├── messages.csv
│   └── new/                       # New-format exports (semicolon-separated)
│       └── *.csv
│
├── output/                        # Generated outputs (gitignored)
│   ├── preprocessed/              # Outputs from preprocess.py
│   │   ├── messages_community.csv          # combined / single-export run
│   │   ├── messages_community_old.csv      # old-data-only run
│   │   ├── messages_community_new_only.csv # new-data-only run
│   │   ├── anonymization_mapping.csv
│   │   └── review_anonymization_MessageText.csv
│   ├── messages_structured.csv    # Output from postprocess.py
│   ├── messages_old.csv           # Old-data slice (from integrate_datasets.py)
│   ├── messages_new_only.csv      # New-data slice (from integrate_datasets.py)
│   ├── messages_combined.csv      # Full merged dataset (from integrate_datasets.py)
│   └── liwc_output.csv            # Output from liwc_analysis.py
│
├── src/
│   ├── config.py                  # Single source of truth for all settings
│   ├── integrate_datasets.py      # Step 0: merge old + new data exports
│   ├── run_ingestion.py           # Step 0 orchestrator: diagnose → integrate
│   ├── preprocess.py              # Step 1: clean, filter, anonymize
│   ├── postprocess.py             # Step 2: thread structure, group filter, normalize
│   ├── liwc_analysis.py           # Step 3: extract LIWC features from .dic file + PDF report
│   ├── exploratory_analysis.py    # CDS prevalence analysis + PDF report
│   ├── cds_prevalence.py          # CDS category + phrase ranking + PDF report
│   ├── exploration.py             # Descriptive statistics + EDA PDF report
│   ├── build_classification_dataset.py
│   ├── sample_ik_voel.py          # Sample "ik voel" messages for manual review
│   ├── user_longitudinal.py       # Per-user CDS + LIWC time-series analysis
│   ├── full_report.py             # Consolidated single-PDF report (all sections, one file)
│   ├── build_master_report.py     # Merge existing sub-report PDFs into one file (pypdf)
│   ├── dataset_io.py              # Single source of truth for dataset → filename resolution
│   ├── role_analysis.py           # Post vs reply analysis figures (used by eda_report.py)
│   ├── diagnose_new_data.py       # Pre-integration diagnostic (read-only)
│   ├── analysis.py
│   ├── eda_report.py              # Legacy EDA report (superseded by exploration.py)
│   ├── custom_text_anonymizer/    # NER-based text anonymizer (Dutch spaCy)
│   │   ├── core.py
│   │   └── main.py
│   ├── postvscomment/             # Experimental post-vs-reply classifier
│   │   └── postvscomment.py
│   ├── liwc22_cli_runner.py       # LIWC-22 CLI wrapper → liwc22_scores.csv
│   ├── liwc_validation_report.py  # Custom scorer vs LIWC-22 comparison PDF
│   ├── utils/                     # Shared utilities
│   │   ├── CDS.py                 # Cognitive distortion schemata loader + scorer (gitignored)
│   │   ├── thread_utils.py        # label_roles(), NLP helpers (tokenize, sentence stats, emoji)
│   │   ├── absolutist.py          # Dutch absolutist word list + scoring functions
│   │   └── spinner.py             # Animated terminal spinner for long-running steps
│   └── app.py                     # Streamlit dashboard
│
├── scripts/                       # One-off diagnostic and inspection utilities
│   ├── find_moderators.py         # Identify moderator posters → output/moderator_review.csv
│   ├── inspect_short_messages.py  # Print low-word-count messages for manual review
│   ├── reply_distribution.py      # Per-user reply counts and self-reply rates
│   ├── sample_user_messages.py    # Sample messages from a specific user
│   └── user_stats.py              # Count unique users by role (posters vs repliers)
│
├── archive/                       # Superseded scripts (kept for reference)
│   ├── plotsperyear.py
│   └── plot_messages.py
│
├── tests/
├── requirements.txt
├── pyproject.toml
├── Makefile
├── ETHICS.md
├── CITATION.cff
└── .gitignore
```

## Execution order

Run scripts in this order. Steps 1–3 must each be run once per dataset when working with three splits; otherwise run them once without `--dataset`.

```
1. run_ingestion.py          diagnose + integrate → messages_old.csv,
                              messages_new_only.csv, messages_combined.csv
        ↓  (only if merging two exports; skip if you have a single messages.csv)

2. preprocess.py             run once per dataset
     --dataset old           → preprocessed/messages_community_old.csv
     --dataset new_only      → preprocessed/messages_community_new_only.csv
     --dataset combined      → preprocessed/messages_community.csv
        ↓

3. postprocess.py            run once per dataset
     --dataset old           → messages_structured_old.csv
     --dataset new_only      → messages_structured_new_only.csv
     --dataset combined      → messages_structured.csv
        ↓

4. (on whichever dataset(s) you need; all scripts accept --dataset {old,new_only,combined})
   build_classification_dataset.py
   sample_ik_voel.py
   exploration.py             →  EDA + role-based linguistics (words, sentences, emoji)
        ↓

5. (independent of each other, all accept --dataset)
   exploratory_analysis.py   →  forum activity time series + CDS prevalence
   cds_prevalence.py          →  CDS category & phrase ranking
   liwc_analysis.py           →  LIWC psycholinguistic features
   user_longitudinal.py       →  per-user CDS + LIWC time series (top N posters)

5b. (optional, requires LIWC-22 app installed and licensed)
   liwc22_cli_runner.py       →  run LIWC-22 CLI → liwc22_scores.csv
   liwc_validation_report.py  →  compare custom scorer vs LIWC-22 → liwc_validation_report.pdf
        ↓  must run liwc_analysis.py first so liwc_scores.csv exists

5c. pandemic_period_analysis.py →  pre/during/post pandemic comparison
        ↓  reads liwc_scores.csv and/or liwc22_scores.csv (run 5 and/or 5b first);
           period boundaries in config.py (PANDEMIC_CUTOFF_DATE / PANDEMIC_END_DATE)

6. Choose one consolidated PDF approach:

   A. full_report.py          →  loads data ONCE, scores CDS + LIWC once, writes all
                                   sections to a single PDF in one pass
        --dataset old          → full_report_old.pdf
        --dataset new_only     → full_report_new_only.pdf
        --dataset combined     → full_report.pdf  (default)
        --all                  → all three datasets

   B. build_master_report.py  →  merges the sub-report PDFs produced in steps 4–5
                                   using pypdf; requires all sub-reports to exist first
        --dataset combined     → master_report.pdf  (default)
        --all-variants         → all three datasets
        --run                  → also re-runs each sub-script before merging

      Commands to run before `make master-report`:
        make pipeline          # preprocess + postprocess (steps 1–2)
        make analyse           # eda + cds + liwc  (produces sub-report PDFs)
        make longitudinal      # per-user time-series PDF
        make master-report     # merge

      For all three dataset variants:
        make pipeline-all
        make analyse-all
        make longitudinal-all
        make master-report-all

7. app.py                    streamlit run src/app.py
```

## Pipeline steps

### Step 1 — run\_ingestion.py *(only if merging two exports)*

Orchestrates the two-phase ingestion process. Run this instead of calling `diagnose_new_data.py` and `integrate_datasets.py` separately.

**Phase 1** (`diagnose_new_data.py`) is read-only. It reports overlap between the old and new exports, flags likely shared/admin accounts, and writes `output/diag_*.csv` files for manual review. The script pauses after this phase and waits for you to press Enter before continuing.

**Phase 2** (`integrate_datasets.py`) builds a confidence-rated ID bridge, deduplicates overlapping posts, and writes:

| File | Contents |
|---|---|
| `output/messages_old.csv` | Posts from the old export only |
| `output/messages_new_only.csv` | Posts from the new export only (after dedup) |
| `output/messages_combined.csv` | Full merged dataset |
| `output/integrated_messages.csv` | Same as combined (backward-compatible name) |

```bash
PYTHONPATH=./src python src/run_ingestion.py
```

### Step 2 — preprocess.py

Reads from `data/messages.csv` (default) or one of the three dataset files written by step 1. Runs HTML stripping, date parsing, superuser and moderator removal, text standardization, word-count filtering, ID anonymization, and NER-based text anonymization. Writes all output to `output/preprocessed/`.

**Single-export run:**
```bash
PYTHONPATH=./src python src/preprocess.py
```

**Three-dataset runs** (after step 1):
```bash
PYTHONPATH=./src python src/preprocess.py --dataset old
PYTHONPATH=./src python src/preprocess.py --dataset new_only
PYTHONPATH=./src python src/preprocess.py --dataset combined
```

Output is named `messages_community_old.csv`, `messages_community_new_only.csv`, and `messages_community.csv` respectively.

### Step 3 — postprocess.py

Reads the preprocessed community CSV and adds thread structure. Runs intro/welcome group filtering, initial-post flagging, reply indexing, thread-success labeling, and text normalization.

**Single-export run:**
```bash
PYTHONPATH=./src python src/postprocess.py
```

**Three-dataset runs:**
```bash
PYTHONPATH=./src python src/postprocess.py --dataset old
PYTHONPATH=./src python src/postprocess.py --dataset new_only
PYTHONPATH=./src python src/postprocess.py --dataset combined
```

Output is named `messages_structured_old.csv`, `messages_structured_new_only.csv`, and `messages_structured.csv` respectively.

### Steps 4–5 — Analysis scripts

All analysis scripts accept `--dataset {old,new_only,combined}`. Omit the flag to run on the default combined/single-export dataset. Output files are suffixed with the dataset name (e.g. `liwc_report_old.pdf`).

All analysis scripts read from `output/messages_structured.csv` (the postprocess output), which has intro/welcome groups already filtered out. Run `make pipeline` (or `make pipeline-all`) before running any analysis.

| Script | Input | Outputs |
|---|---|---|
| `build_classification_dataset.py` | `preprocessed/messages_community.csv` | `classification_dataset.csv` |
| `exploration.py` | `messages_structured.csv` | `eda_report_all_users.pdf`, `eda_report_multi_posters.pdf`, `messages_multi_posters.csv` |
| `exploratory_analysis.py` | `messages_structured.csv` | `exploratory_report.pdf`, `cds_scores.csv`, `cds_per_user.csv` |
| `cds_prevalence.py` | `messages_structured.csv` | `cds_prevalence_report.pdf`, `cds_category_ranking.csv`, `cds_phrase_ranking.csv` |
| `liwc_analysis.py` | `messages_structured.csv` | `liwc_report.pdf`, `liwc_scores.csv`, `liwc_per_user.csv` |
| `user_longitudinal.py` | `messages_structured.csv` | `user_longitudinal_report.pdf` |
| `liwc22_cli_runner.py` *(optional)* | `messages_structured.csv` | `liwc22_scores.csv` |
| `liwc22_report.py` *(optional)* | `liwc22_scores.csv` | `liwc22_report.pdf` — descriptive report on LIWC-22's own scores (mirrors `liwc_report.pdf`) |
| `liwc_validation_report.py` *(optional)* | `liwc_scores.csv` + `liwc22_scores.csv` | `liwc_validation_report.pdf`, `liwc_validation_comparison.csv` |
| `pandemic_period_analysis.py` | `liwc_scores.csv` and/or `liwc22_scores.csv` | `pandemic_period_report.pdf`, `pandemic_period_stats.csv` |

The EDA report (`exploration.py`) includes a **Role-Based Analysis** section at the end of each PDF:

- **Distinctive words** — diverging bar chart of words disproportionately common in initial posts vs replies (log₂ frequency ratio, Dutch stopwords removed)
- **Popular words** — side-by-side top-20 most frequent content words for posts and replies
- **Words per message** — side-by-side distributions for posts vs replies
- **Emoji use** — % of messages containing emoji and mean emoji count, split by role
- **Sentence structure** — avg sentences per message, words per sentence, chars per sentence, and punctuation patterns (`?`, `!`, `...`) split by role

`user_longitudinal.py` scores the top N most active posters (default: 5) on both CDS categories and LIWC categories, aggregates per month, and writes one PDF per user with time-series plots.

```bash
# EDA report (includes role-based linguistics section)
PYTHONPATH=./src python src/exploration.py
PYTHONPATH=./src python src/exploration.py --dataset old

# CDS and LIWC analyses
PYTHONPATH=./src python src/exploratory_analysis.py
PYTHONPATH=./src python src/cds_prevalence.py
PYTHONPATH=./src python src/liwc_analysis.py
# … or with a dataset variant:
PYTHONPATH=./src python src/exploratory_analysis.py --dataset old
PYTHONPATH=./src python src/cds_prevalence.py --dataset old
PYTHONPATH=./src python src/liwc_analysis.py --dataset old

# Per-user longitudinal analysis
PYTHONPATH=./src python src/user_longitudinal.py
PYTHONPATH=./src python src/user_longitudinal.py --dataset old --top 10
```

### Step 6 — full_report.py

Runs all analyses for a given dataset and writes everything into a single consolidated PDF. CDS and LIWC are scored only once, so this is faster than running each script individually.

Sections in the output PDF:
1. Exploratory Data Analysis (histograms, activity patterns, "ik"/"mijn" usage)
2. Role-Based Linguistics (popular words, distinctive words, emoji use, sentence structure)
3. Forum Activity & CDS Prevalence Over Time
4. CDS Category & Phrase Analysis
5. LIWC Psycholinguistic Analysis *(skipped silently if no `.dic` file is configured)*

```bash
# Default (combined / single-export dataset)
PYTHONPATH=./src python src/full_report.py

# Specific dataset variant
PYTHONPATH=./src python src/full_report.py --dataset old
PYTHONPATH=./src python src/full_report.py --dataset new_only

# All three datasets in one run
PYTHONPATH=./src python src/full_report.py --all
```

Output: `output/full_report.pdf`, `output/full_report_old.pdf`, `output/full_report_new_only.pdf`

### Step 7 — app.py

```bash
PYTHONPATH=./src streamlit run src/app.py
```

## Shared utilities (`src/utils/`)

| Module | Exports |
|---|---|
| `utils/thread_utils.py` | `label_roles(df)` — labels first post as `"post"`, rest as `"reply"` |
| `utils/CDS.py` | `load_CDS()`, `find_CDS()`, `process_dataset()` — cognitive distortion schemata scoring |

All analysis scripts (`exploratory_analysis.py`, `cds_prevalence.py`, `liwc_analysis.py`, `exploration.py`) import from `utils/` rather than defining their own copies.

## Makefile reference

All pipeline steps are available as Make targets. `PYTHONPATH=src` is set automatically.

### Single-export workflow

```bash
make install          # create venv, install dependencies

make ingest           # Step 0 — only if merging two exports
make preprocess       # Step 1 — reads data/messages.csv
make postprocess      # Step 2
# or both steps at once:
make pipeline
```

### Three-dataset workflow

```bash
make ingest           # Step 0 — produces messages_old/new_only/combined.csv

make preprocess-all   # Step 1 for all three datasets
make postprocess-all  # Step 2 for all three datasets
# or both steps at once:
make pipeline-all
```

To process a single dataset without running all three:

```bash
make preprocess  DATASET=old
make postprocess DATASET=old
```

### Analysis and app

```bash
make eda              # EDA report (exploration.py)
make cds              # CDS prevalence (exploratory_analysis + cds_prevalence)
make liwc             # LIWC analysis
make analyse          # all three (eda + cds + liwc)
make analyse-all      # all three analysis scripts for old, new_only, and combined
make longitudinal     # per-user LIWC/CDS time series
make longitudinal-all # longitudinal analysis for old, new_only, and combined

# Option A — single-pass consolidated PDF (faster, no intermediate files needed)
make full-report          # combined dataset
make full-report-all      # old, new_only, and combined

# Option B — merge existing sub-report PDFs with pypdf
#   Prerequisites (all three datasets):
#     make pipeline-all && make analyse-all && make longitudinal-all
make master-report        # merge sub-reports → master_report.pdf (combined dataset)
make master-report-all    # all three dataset variants

make app          # launch Streamlit dashboard
```

### LIWC-22 validation *(optional, requires LIWC-22 app)*

Runs the official LIWC-22 CLI against the same structured messages and compares its scores category-by-category with the custom scorer. Produces a validation PDF (section 7 in the master report).

**Prerequisites:**
- LIWC-22 app installed at `/Applications/LIWC-22.app` and licence activated (open the GUI once)
- Dutch `.dicx` dictionary file at `data/LIWC2015 Dictionary - Dutch.dicx`
- `make liwc` must have run first so `liwc_scores.csv` exists for the comparison
- Override paths if needed: `LIWC22_CLI=... LIWC22_DICT=... make liwc-validate`

```bash
make liwc22               # run LIWC-22 CLI → liwc22_scores.csv
make liwc-validate        # run CLI + produce validation report PDF
make liwc22-all           # LIWC-22 scores for old, new_only, and combined
make liwc-validate-all    # full validation for all three dataset variants
make liwc22-report        # standalone descriptive report on LIWC-22's own scores
make liwc22-report-all    # LIWC-22 report for old, new_only, and combined
```

`liwc22_report.py` mirrors `liwc_report.pdf`'s structure (category prevalence,
posts vs replies, monthly trends) but reports LIWC-22's own numbers directly —
it does not compare against the custom scorer (that's `liwc-validate`).

The validation report includes:
1. Per-category Pearson r and MAE (mean absolute difference in percentage points) between scorers
2. Scatter plots for the most divergent categories
3. A note explaining why Analytic / Clout / Authentic / Tone are absent (they require the built-in English LIWC-22 dictionary, not an external `.dicx` file)
4. Coverage differences — categories present in one scorer but not the other
5. Word-count agreement as a tokenisation sanity check

```bash
# Full workflow for one dataset (liwc must run before liwc-validate)
make pipeline DATASET=combined
make liwc DATASET=combined
make liwc-validate DATASET=combined
```

### Pandemic-period comparison

Compares psycholinguistic markers (pronouns, function words, tenses, emotion, informal language, absolutist words — after Yahya & Abdul Rahim 2023) across three pandemic periods (`pre` / `during` / `post`, boundaries in `config.py`). Uses per-user Kruskal-Wallis + pairwise Mann-Whitney U with BH correction instead of the paper's corpus-level log-likelihood — see `docs/statistical_decisions.md` §9, including the period × dataset-variant confound and the single-period-user sensitivity analysis.

Requires `liwc_scores.csv` (from `make liwc`) and/or `liwc22_scores.csv` (from `make liwc22`).

```bash
make pandemic                 # combined
make pandemic DATASET=old
make pandemic-all             # all three variants
python src/pandemic_period_analysis.py --end-date 2023-05-05   # provisional experiment
```

Any analysis target accepts a `DATASET=` override:

```bash
make eda DATASET=old
make cds DATASET=new_only
make full-report DATASET=old
```

Run `make help` to see all available targets.

## Setup

**Requirements:** Python 3.11+

```bash
make install
```

Or manually:

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

After installing, download the Dutch spaCy model required for text anonymization:

```bash
.venv/bin/python -m spacy download nl_core_news_lg
```

### Data files

Place the following raw CSVs in `data/` before running the pipeline:

| File | Description |
|---|---|
| `accounts.csv` | Forum account metadata |
| `groups.csv` | Forum group metadata (AccountID, Name) |
| `topics.csv` | Thread metadata (ForumTopicID, ForumGroupID) |
| `messages.csv` | Raw messages (PosterID, ForumTopicID, MessageText, PostDate) |
| `data/new/*.csv` | New-format exports, semicolon-separated *(step 0 only)* |
| `data/LIWC2015 Dictionary - Dutch.dicx` | Dutch LIWC-2015 dictionary *(steps 3 and 5b)* |

## Configuration

All pipeline settings are in [src/config.py](src/config.py). Key options:

| Setting | Default | Description |
|---|---|---|
| `SUPERUSER_ACCOUNT_IDS` | `{1, 4}` | Account IDs for test/demo forums — their posters are excluded |
| `COMMUNITY_ACCOUNT_IDS` | `{2, 3}` | Account IDs for the real communities |
| `MODERATOR_POSTER_IDS` | 8 UUIDs | Confirmed moderator poster IDs to exclude |
| `INTRO_GROUP_KEYWORDS` | see file | Group name substrings that mark off-topic/welcome groups |
| `MIN_WORD_COUNT` | `5` | Minimum words for a message to be kept |
| `MIN_POSTS_PER_USER` | `5` | Minimum total posts for a user to be included (applied in postprocess.py) |
| `ANONYMIZE_TEXT` | `True` | Run NER-based text anonymization |
| `REPLACE_ORIGINAL_TEXT` | `True` | Replace original text with anonymized version in output |
| `EXPORT_ENTITY_REVIEW` | `True` | Write original vs anonymized text to a review CSV |
| `INTEGRATED_OLD_PATH` | `output/messages_old.csv` | Old-data slice produced by integrate_datasets.py |
| `INTEGRATED_NEW_PATH` | `output/messages_new_only.csv` | New-data slice produced by integrate_datasets.py |
| `INTEGRATED_COMBINED_PATH` | `output/messages_combined.csv` | Full merged dataset produced by integrate_datasets.py |

## Output files

| File | Description |
|---|---|
| `output/preprocessed/messages_community.csv` | Cleaned, anonymized community messages (combined or single-export) |
| `output/preprocessed/messages_community_old.csv` | Same, old-data slice only |
| `output/preprocessed/messages_community_new_only.csv` | Same, new-data slice only |
| `output/preprocessed/anonymization_mapping.csv` | OriginalID → user\_N mapping |
| `output/preprocessed/review_anonymization_MessageText.csv` | Original vs anonymized text for review |
| `output/preprocessed/entities_MessageText.csv` | NER entities found per message |
| `output/messages_structured.csv` | Thread-structured dataset with `is_initial_post`, `reply_index`, `thread_has_replies`, `text_normalized` |
| `output/messages_old.csv` | Old-export slice (post integration, pre-preprocess) |
| `output/messages_new_only.csv` | New-export slice (post integration, pre-preprocess) |
| `output/messages_combined.csv` | Full merged dataset (post integration, pre-preprocess) |
| `output/liwc_scores.csv` | Custom LIWC scorer: category scores per message |
| `output/liwc22_scores.csv` | LIWC-22 CLI scores per message (optional) |
| `output/liwc_validation_report.pdf` | Custom scorer vs LIWC-22 comparison report (optional) |
| `output/liwc_validation_comparison.csv` | Per-category Pearson r and MAE table (optional) |
| `output/full_report.pdf` | Consolidated report — all analysis sections in one PDF |
| `output/full_report_old.pdf` | Same, old-data slice only |
| `output/full_report_new_only.pdf` | Same, new-data slice only |
| `output/integrated_messages.csv` | Merged old + new data *(step 0 only, backward-compatible name)* |
| `output/id_bridge.csv` | UUID → integer ID mapping with confidence ratings *(step 0 only)* |

## Ethical considerations

This project processes data from a mental health support forum. See [ETHICS.md](ETHICS.md) for the full ethics checklist. In brief:

- All poster IDs are replaced with anonymous tokens (`user_N`)
- Message text is processed through NER to replace names, locations, and contact details with placeholders
- Raw data and all output files are excluded from version control via `.gitignore`
- The anonymization mapping is stored separately and should be handled with care

## Citation

If you use this code, please cite it as described in [CITATION.cff](CITATION.cff).
