## 2. Dataset

### 2.1 Source and provenance

The data come from Depression Connect, a Dutch-language online peer-support
forum for people affected by depression. The forum is owned by the Depressie
Vereniging (the Dutch Depression Association); the platform is currently hosted,
and its data collected, by the internet agency MEO on the association's behalf.
The platform was developed with funding from a ZonMw grant (PI: Prof. Jan
Spijker) and is now funded by the association. The data were shared for research
under a data-sharing agreement and were pseudonymized by the data owner before
sharing. No new data were collected from participants for this project.

Research use of the forum content rests on consent given at registration: users
agree on sign-up that their data may be used for research, through a passive
consent form that also carries the platform's terms of use. There is therefore no
separate ethics approval for this dataset, and none was required.

Both exports were produced by the platform side from the live forum database,
not scraped. The legacy export was pulled when the original platform host's
contract ended and was handed over on 31 October 2025; its content runs to
5 October 2022. The newer export was pulled via MEO and handed over on 6 March
2026, with content to 30 March 2026. The exact extraction date of the legacy
export is not on record; remaining open items are tracked in
`docs/TO_CONFIRM.md`.

### 2.2 Variants

The corpus is delivered as two exports: a legacy export and a newer export. The
pipeline builds three variants from them. The `old` variant is the legacy
export. The `new_only` variant is the newer export with messages already present
in the legacy export removed. The `combined` variant is their union.

The two exports do not share a structure. The legacy export is a small
relational database dump: four tables, in which messages belong to topics,
topics to groups, and groups to a forum section, so a message can be traced to
the section it was posted in and to the group hierarchy it sits under. Figure
2.1 shows this schema. The newer export is a flat bbPress dump — one
denormalized message table, split across files by year range, with the topic
title repeated on every message and no lookup tables. It carries no section
identifier at all, and its author identifiers are integers rather than the
legacy export's UUIDs. The full comparison, and the reconciliation the pipeline
performs, are documented in `docs/DATA_SCHEMA.md`.

Two consequences matter for interpretation. First, because only the legacy
export identifies forum sections, the section-level reasoning in Section 2.4
applies to that export alone. Second, matching authors across the two exports is
inferential rather than exact: the pipeline bridges legacy UUIDs to the newer
integer identifiers by posting behaviour and only accepts high-confidence
matches, so unmatched authors may appear under two identities. Per-user
aggregation is therefore performed within a variant, never across the seam.

For this reason, the recommendation to anyone reusing this corpus is to treat
the legacy and the newer export as two different datasets rather than one
continuous one. The pipeline still produces all three variants for every
analysis, and the `combined` variant remains available should a merged view be
wanted, but it is the least reliable of the three: it inherits the uncertainties
of both exports and adds the author bridge, the cross-export deduplication, and
the source-time confound on top. Reliability runs `old`, then `new_only`, then
`combined`.

**Figure 2.1.** Schema of the legacy export
(`docs/schema_forum_database_old.pdf`). This diagram describes the legacy export
only; the newer export has no equivalent structure.

The `old` variant is treated as the primary source of evidence throughout this
report. It covers the pre-pandemic and pandemic period and has been verified
more thoroughly. The `new_only` and `combined` variants are reported as
supporting material. Any finding that depends on the period after 2022 rests on
the newer export and is labeled exploratory, because in that export message
source and calendar time are confounded (see limitations).

### 2.3 Composition

Counts below are after all filtering (Section 4.2), from the regenerated
`messages_structured*.csv` outputs.

| Variant | Messages | Users | Threads | Date range |
|---|---:|---:|---:|---|
| `old` (primary) | 18,550 | 427 | 2,024 | 2019-06-19 to 2022-10-05 |
| `new_only` | 25,504 | 530 | 3,388 | 2019-05-17 to 2026-03-30 |
| `combined` | 45,181 | 940 | 5,495 | 2019-05-17 to 2026-03-30 |

Each row is one forum message, with a pseudonymous author identifier, a thread
identifier, a timestamp, and the message text. Messages are labeled as opening
posts or replies from their position in the thread. The dataset carries no
demographic or clinical metadata.

### 2.4 Filtering and pseudonymization

Filtering is applied once before any analysis, so every script sees the same
population. Test and demo accounts, eight confirmed moderator accounts and the
threads they opened, and introduction and recreational groups are removed.
Messages under five words and users with fewer than five messages are dropped.
The full rationale is in Section 4.2 and in `docs/statistical_decisions.md`.

The forum has two community sections, one for people with depression and one for
their relatives and companions (`naasten`). Both are retained. The section a
message appears in marks its topic area, not the author's role: the same person
can post in either section, so the account does not identify whether a message
was written by someone with depression or by a relative. The population is
therefore the peer-support community of this forum as a whole, and it is not
split into "depression" and "relatives" groups.

The data are treated as pseudonymized, not anonymized. Author identifiers are
replaced with pseudonyms through a stored mapping, and named entities in the
message text are replaced with placeholders using named-entity recognition.
Because free text can still carry identifying detail that no automatic step is
guaranteed to catch, complete anonymity cannot be claimed, and the data remain
personal data under the data-sharing agreement and GDPR. The entity placeholders
are removed before any scoring, so they cannot inflate word counts or dictionary
matches.

### 2.5 Scope: analysis capped at 2022

The newer export extends the corpus to March 2026, but message source and
calendar time are confounded beyond 2022 (the old export covers the pre and
during-pandemic years, the new export mostly the years after). The analysis is
therefore capped at 2022 and based on the `old` variant, which runs from June
2019 to October 2022. The `new_only` and `combined` variants, and any result
that depends on the post-2022 period, are treated as exploratory context and are
not sources of reported findings.
