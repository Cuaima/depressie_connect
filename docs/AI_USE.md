# Use of AI in This Project

A disclosure of where AI assistance was used, where it was deliberately not
used, and what controls kept it away from the forum data. Companion to
[`DATA_GOVERNANCE.md`](DATA_GOVERNANCE.md) and [`../ETHICS.md`](../ETHICS.md).

The short version: **AI was used to build the pipeline, not to run the
analysis, and it never had access to the message data.** The two claims are
separable, and both are stated precisely below.

---

## 1. There is no AI in the analysis itself

Nothing in this repository sends text to a language model, and no result in the
report is produced or mediated by one. Every processing step is local and
deterministic:

| Step | Method | Nature |
|---|---|---|
| Text pseudonymization | spaCy `nl_core_news_lg` NER | local statistical model, downloaded once, no network use at runtime |
| CDS detection | rule/dictionary matching (`CDS.py`) | deterministic |
| LIWC-2015 scoring | local dictionary (`liwc15.dic`, Dutch `.dicx`) | deterministic lookup |
| LIWC-22 scoring | LIWC-22 desktop CLI, licensed, run locally | vendor software, local execution |
| Statistics | pandas / scipy / statsmodels | deterministic |

Two points worth stating explicitly for an ethics reader:

- **The NER model is not generative.** It labels spans as entities so they can be
  replaced with `[ENTITY_*]` placeholders. It does not rewrite, summarize, or
  otherwise author text, and it runs on the researcher's machine.
- **No network calls exist in the analysis code.** A search across `src/` and
  `scripts/` for HTTP clients, API keys, and provider SDKs (`openai`,
  `anthropic`, `requests.`, `urllib`, `http(s)://`, `api_key`) returns no
  matches. `requirements.txt` contains no LLM client library. Message text
  therefore has no route out of the machine through this code.

The supportive-reply classifier is a **separate project** (`dss_thesis`) with its
own ELSA assessment; nothing about it is covered here.

## 2. Where AI assistance was used

Claude (Claude Code, Anthropic) was used as a **software and writing assistant**
during development. Concretely, it contributed to:

- pipeline code — preprocessing, postprocessing, dataset integration, the
  analysis and reporting scripts;
- the `pytest` suite that covers them;
- documentation, including this file, the datasheet, the data-governance note,
  the schema documentation, and drafts of the report's methods and dataset
  sections;
- recording provenance answers received from supervisors.

Of the 63 commits in this repository, **25 carry a `Co-Authored-By: Claude`
trailer**, so the extent of assistance is auditable from `git log` rather than
resting on this description:

```
git log --grep='Co-Authored-By: Claude' --oneline
```

**Responsibility is not delegated.** Every AI-assisted change was reviewed by the
researcher before being committed, and the researcher is the author of the work
and accountable for its correctness. AI assistance is a tool used in producing
the code and prose, not a co-investigator, and it made no analytical or
interpretive decisions: the statistical design choices are recorded, with their
reasoning and their dates, in [`statistical_decisions.md`](statistical_decisions.md).

## 3. How AI was kept away from the data

Three independent layers, in order of strength.

**1. The data are not in the repository.** `.gitignore` excludes `data/`,
`output/*` (except report PDFs), the pseudonymization mapping files, entity
review files, and every `*.csv` / `*.tsv` / `*.parquet` / database extension.
Raw exports live only on the researcher's machine. Anything an assistant reads
from a clone of this repo therefore contains no forum content.

**2. Tool-level deny rules block the data directories.** `.claude/settings.json`
is committed to the repository — deliberately, so the rules apply to any clone
and any session, not just to one person's local setup:

```json
{
  "permissions": {
    "deny": ["Read(data/**)", "Read(output/**)", "Read(jic/**)"]
  }
}
```

`data/**` covers both exports, `output/**` covers every derived file including
the preprocessed message CSVs and the generated report PDFs, and `jic/**` covers
the refactoring scratch area. `.gitignore` keeps only `settings.local.json` and
other `*.local.json` overrides out of version control, so the shared deny rules
cannot be silently weakened for everyone by a personal setting.

**3. Development ran against synthetic fixtures.** The test suite builds its own
data inline — small hand-written DataFrames with values like `"Hello World"` and
poster `"u1"` (`tests/test_preprocess.py` and siblings). No test reads a real
export, so the code could be developed and verified against fixtures rather than
against forum messages.

## 4. Limits of these controls — stated honestly

The controls above are real but not absolute, and an ethics statement that
claimed otherwise would be wrong.

- **The deny rules cover the assistant's file-reading tool, not every possible
  path.** Shell commands are gated by per-command approval instead. In practice
  this worked as intended: during the schema documentation session (19 Aug 2026)
  a command reading CSV *header rows* — column names only, no message content —
  was approved, and a subsequent command that would have printed data rows was
  declined by the researcher. That is the intended division, but it depends on
  the human reviewing each command rather than on a hard block.
- **Approval is a human control, so it can fail.** A carelessly approved command
  could expose data that the deny rules would have stopped. The rules are the
  durable protection; command approval is judgement.
- **Nothing here governs the researcher's own screen.** Running the pipeline
  prints diagnostics, and some helper scripts (`sample_user_messages.py`,
  `inspect_short_messages.py`, `sample_ik_voel.py`) exist precisely to show
  message text for manual review. That output is for the researcher, and it is
  the researcher's responsibility not to paste such content into any external
  tool, AI or otherwise.
- **Pseudonymized is not anonymous.** Even the processed text remains personal
  data (`DATA_GOVERNANCE.md` §4), so the rule is about *all* forum text, not only
  the raw exports.
- **AI-assisted code can carry subtle errors.** The mitigation is the test suite
  and human review, not trust in the tool. Where a generated claim about the code
  turned out to be wrong, it was corrected against the source and the correction
  recorded (see `DATA_SCHEMA.md` §1 on the `accounts.csv` join).

## 5. Suggested disclosure wording for the report

> AI assistance (Claude, Anthropic) was used in developing the analysis code,
> its test suite, and drafts of the documentation; all such output was reviewed
> by the researcher, who is responsible for the work. No language model was used
> in the analysis itself: pseudonymization, dictionary scoring, and all
> statistics run locally and deterministically, and the analysis code makes no
> network calls. AI tooling had no access to the forum data — the datasets are
> excluded from version control and blocked at the tool level by committed deny
> rules on the data and output directories.
