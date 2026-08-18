## 2. Dataset

### 2.1 Source and provenance

The data come from Depression Connect, a Dutch-language online peer-support
forum for people affected by depression. They were shared for research under a
data-sharing agreement and were pseudonymized by the data owner before sharing.
No new data were collected from participants for this project.

Some provenance facts are still being confirmed with the supervisors and are
left as placeholders here: [TO CONFIRM: data owner legal entity], [TO CONFIRM:
consent basis for research use], and [TO CONFIRM: export and handover dates].
These are tracked in `docs/TO_CONFIRM.md`.

### 2.2 Variants

The corpus is delivered as two exports: a legacy export and a newer export. The
pipeline builds three variants from them. The `old` variant is the legacy
export. The `new_only` variant is the newer export with messages already present
in the legacy export removed. The `combined` variant is their union.

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

The data are treated as pseudonymized, not anonymized. Author identifiers are
replaced with pseudonyms through a stored mapping, and named entities in the
message text are replaced with placeholders using named-entity recognition.
Because free text can still carry identifying detail that no automatic step is
guaranteed to catch, complete anonymity cannot be claimed, and the data remain
personal data under the data-sharing agreement and GDPR. The entity placeholders
are removed before any scoring, so they cannot inflate word counts or dictionary
matches.

### 2.5 Scope note

The newer export extends the corpus to March 2026, later than the 2022 endpoint
of related work on this forum. The choice of analysis end date is being settled
with the supervisors ([TO CONFIRM: analysis end-date scope]); this report does
not assume the full window or a capped window until that is decided.
