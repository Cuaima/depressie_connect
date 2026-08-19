# Studies → Code Map

Where each study in this folder is used in the pipeline. Studies marked
*report-only* provide context for the written report but have no natural code
anchor.

| Study (file) | What it is | Code anchor |
|---|---|---|
| `manuscript_R1_submission.pdf` | CDS prevalence in the Dutch 113 suicide-helpline chats; 12-type cognitive distortion schemata with Dutch n-gram markers | `src/utils/CDS.py`, `src/translations/list_of_CDS_NL.tsv` (12 categories, 265 markers — same design) |
| `al-mosaiwi-johnstone-2018-...pdf` | Absolutist words as a marker specific to anxiety/depression (incl. 2019 corrigendum) | `src/utils/absolutist.py` (Dutch translation of the 19-word set) |
| `Yahya.pdf` | Yahya & Abdul Rahim 2023 — linguistic markers of depression in tweets pre/during COVID | `src/pandemic_period_analysis.py` (feature set §2.3; we replace their corpus log-likelihood with per-user rank tests, see `docs/statistical_decisions.md` §9) |
| `Smirnova 2018.pdf` | Language patterns discriminate mild depression from normal sadness | `src/liwc_analysis.py` FPS section (`ensure_fps`); `docs/statistical_decisions.md` §6 |
| `eichstaedt-et-al-2018-...pdf` | Facebook language predicts medical-record depression (LIWC-style markers, FPS among top predictors) | `src/liwc_analysis.py` FPS section; supports `src/user_longitudinal.py` (temporal language → depression) |
| `Smit qual evaluation depressie connect.pdf` | Qualitative evaluation of Depression Connect — the forum this dataset comes from | Dataset description (README, future `docs/DATASHEET.md`) |
| `Smit quant evaluatie DC.pdf` | Longitudinal survey: Depression Connect engagement → recovery-related empowerment | `src/postprocess.py` `label_thread_success()`; motivates engagement metrics in `src/exploration.py` / `src/user_longitudinal.py` |
| `Smit qual evaluation peer support experiental knowledge.pdf` | Experiential knowledge & self-management in depression | Report-only (interpretation of peer-to-peer reply content) |
| `lib_jmir-2019-4-e11410.pdf` | Milne et al. 2019 — moderator responsiveness/triage in online peer support | `src/config.py` `MODERATOR_POSTER_IDS` (moderator exclusion rationale, `docs/statistical_decisions.md` §4) |
| `lib_intr-03-2021-0189.pdf` | Social capital factors in social support acquisition in online health communities | `src/postprocess.py` `label_thread_success()`; thread-level reply analysis |
| `ahani.pdf` | Social Support Detection as an NLP task (psycholinguistic + sentiment features) | `src/build_classification_dataset.py`, `src/postvscomment/` |
| `lib_jmir-2023-1-e51712.pdf` | Chatbots for emotional support across cultures | Report-only |
| `lib_peerj-cs-2828.pdf` | DRIVE-model mental-health detection in text during COVID-19 | Report-only (secondary context for the pandemic-period section) |

---

# Analysis → Studies Map

The reverse view, keyed by deliverable/analysis rather than by paper — for the
thesis write-up, answering "which studies justify *this* analysis?". "Method"
studies motivate the technique; "grounding" studies justify why the feature or
choice matters for depression/peer support.

| Analysis (script / output) | Supporting studies | Role |
|---|---|---|
| **CDS prevalence** (`cds_prevalence.py`, `exploratory_analysis.py`, `utils/CDS.py`) | 113 helpline manuscript (`manuscript_R1_submission.pdf`) | Method — 12-type CDS n-gram design and Dutch markers; comparison target for forum vs. helpline prevalence |
| **CDS statistics** (per-user Mann-Whitney, `docs/statistical_decisions.md` §2) | 113 helpline manuscript | Grounding — aggregate-level reporting to avoid pseudo-replication |
| **LIWC — FPS pronouns** (`liwc_analysis.py` `ensure_fps`, `user_longitudinal.py`) | Smirnova 2018; Eichstaedt 2018 | Grounding — elevated first-person-singular use as a replicated depression marker |
| **LIWC — absolutist words** (`utils/absolutist.py`, `liwc_analysis.py`, `full_report.py`) | Al-Mosaiwi & Johnstone 2018 (+2019 corrigendum) | Method + grounding — the 19-word set and its specificity to anxiety/depression |
| **Pandemic-period comparison** (`pandemic_period_analysis.py`) | Yahya & Abdul Rahim 2023 | Method — feature set (§2.3) and pre/during/post design; DRIVE/peerj (`lib_peerj-cs-2828.pdf`) as secondary COVID-coping context |
| **LIWC-22 validation** (`liwc22_cli_runner.py`, `liwc_validation_report.py`) | — | Internal cross-check of the custom scorer; no external paper anchor |
| **Moderator exclusion** (`config.py` `MODERATOR_POSTER_IDS`, `preprocess.py`) | Milne et al. 2019 | Grounding — moderators' distinct triage/role behaviour vs. peers |
| **Thread-success / reply labelling** (`postprocess.py` `label_thread_success`) | Smit quant DC eval; Liu et al. 2021 (`lib_intr-03-2021-0189.pdf`) | Grounding — receiving replies as the support mechanism; drivers of support acquisition |
| **Post-vs-reply classification** (`build_classification_dataset.py`, `postvscomment/`) | Ahani et al. (SSD) | Method — social support detection as an NLP task with psycholinguistic features |
| **Dataset description / EDA** (`exploration.py`, `eda_report.py`, future `DATASHEET.md`) | Smit qual DC eval; Smit experiential-knowledge; Chin 2023 chatbots | Grounding — what Depression Connect is, peer-support content, broader digital-support context |
