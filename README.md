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
│   ├── diagnose_new_data.py       # Pre-integration diagnostic (read-only)
│   ├── analysis.py
│   ├── eda_report.py              # Legacy EDA report (superseded by exploration.py)
│   ├── custom_text_anonymizer/    # NER-based text anonymizer (Dutch spaCy)
│   │   ├── core.py
│   │   └── main.py
│   ├── postvscomment/             # Experimental post-vs-reply classifier
│   │   └── postvscomment.py
│   ├── utils/                     # Shared utilities
│   │   ├── CDS.py                 # Cognitive distortion schemata loader + scorer
│   │   └── thread_utils.py        # label_roles() and label_thread_roles()
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

6. full_report.py             →  single consolidated PDF combining steps 4–5
     --dataset old            → full_report_old.pdf
     --dataset new_only       → full_report_new_only.pdf
     --dataset combined       → full_report.pdf          (default)
     --all                    → all three datasets in one run

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

| Script | Input (`preprocessed/`) | Outputs |
|---|---|---|
| `build_classification_dataset.py` | `messages_community.csv` | `classification_dataset.csv` |
| `exploration.py` | `messages_community.csv` | `eda_report_all_users.pdf`, `eda_report_multi_posters.pdf`, `messages_multi_posters.csv` |
| `exploratory_analysis.py` | `messages_community.csv` | `exploratory_report.pdf`, `cds_scores.csv`, `cds_per_user.csv` |
| `cds_prevalence.py` | `messages_community.csv` | `cds_prevalence_report.pdf`, `cds_category_ranking.csv`, `cds_phrase_ranking.csv` |
| `liwc_analysis.py` | `messages_community.csv` | `liwc_report.pdf`, `liwc_scores.csv`, `liwc_per_user.csv` |
| `user_longitudinal.py` | `messages_structured.csv` | `longitudinal/user_longitudinal.pdf` |

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
make eda          # EDA report (exploration.py)
make cds          # CDS prevalence (exploratory_analysis + cds_prevalence)
make liwc         # LIWC analysis
make analyse      # all three (eda + cds + liwc)
make longitudinal # per-user LIWC/CDS time series

make full-report          # consolidated single-PDF report (combined dataset)
make full-report-all      # run for old, new_only, and combined

make app          # launch Streamlit dashboard
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
| `data/LIWC2015Dutch.dic` | Dutch LIWC dictionary *(step 3 only)* |

## Configuration

All pipeline settings are in [src/config.py](src/config.py). Key options:

| Setting | Default | Description |
|---|---|---|
| `SUPERUSER_ACCOUNT_IDS` | `{1, 4}` | Account IDs for test/demo forums — their posters are excluded |
| `COMMUNITY_ACCOUNT_IDS` | `{2, 3}` | Account IDs for the real communities |
| `MODERATOR_POSTER_IDS` | 8 UUIDs | Confirmed moderator poster IDs to exclude |
| `INTRO_GROUP_KEYWORDS` | see file | Group name substrings that mark off-topic/welcome groups |
| `MIN_WORD_COUNT` | `1` | Minimum words for a message to be kept |
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
| `output/liwc_scores.csv` | LIWC category scores per message |
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
