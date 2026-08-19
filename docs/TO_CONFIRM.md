# Open Questions to Confirm — for the Supervisors / Data Owner

A short checklist of facts this project **cannot verify from the code or data
alone**. They concern the data-sharing agreement and the platform's collection
process, and are needed to finalize the datasheet, the data-governance note, and
the report's dataset section. Bring this list to the meeting.

Each item notes where it currently sits in the docs so the answer can be dropped
straight in.

## Data provenance & agreement

- [ ] **Legal entity of the data owner.** Currently written as "the platform
  organization (inferred: Dutch Depression Association / Pro Persona ecosystem)".
  What is the exact organization name to cite?
  → `DATASHEET.md` §1, `DATA_GOVERNANCE.md` §1
- [ ] **Consent basis.** Under what basis may forum users' content be used for
  research (e.g. platform terms of service, broad consent at registration,
  specific ethics approval)?
  → `DATASHEET.md` §3
- [ ] **Redistribution & retention terms.** What exactly does the data-sharing
  agreement permit/forbid for redistribution, and is there a required data
  deletion date or retention limit?
  → `DATASHEET.md` §6–§7, `ETHICS.md` B.3/B.4
- [ ] **Funding source** (if any) behind the dataset/collection.
  → `DATASHEET.md` §1

## Collection process

- [~] **Export dates & extraction method.** New export: extraction ≈ its
  handover date, 6 March 2026 (content runs to 30 March 2026). Old export:
  extraction ~late 2022, inferred from content ending 5 Oct 2022. Checked the
  Smit papers (2026-08-19): the old extraction date is **not** there — their
  extractions cluster around 2020 (quant survey data extracted 24 Sept 2020) —
  though they do confirm the forum "launched in mid-2019", matching the old
  data's first message (19 June 2019). Only the old export's exact extraction
  date remains open, to confirm with the data owner.
  → `DATASHEET.md` §3
- [x] **Export completeness. ANSWERED.** Each export is intended as complete
  forum content, but the two are not perfectly consistent: they overlap
  partially, and the new export's content from *before* the change of hands
  appears to have been back-propagated and should not be relied on. This is
  consistent with the observed data — the new export has almost no pre-2022
  content (67 msgs in 2019, 140 in 2020, none in 2021) — and reinforces the
  2022-cap / old-primary scope decision.
  → `DATASHEET.md` §3
- [x] **Handover dates. ANSWERED.** Old export handed over **31 October 2025**;
  new export handed over **6 March 2026** (coincides with its extraction).
  → `DATASHEET.md` §3

## Scope decision (research call, not a data-owner question) — RESOLVED

- [x] **Analysis end date. DECIDED (2026-08-19): cap at 2022 — the `old`
  variant is the basis for all main findings.** The newer export extends the
  corpus to March 2026, but message source and calendar time are confounded
  past 2022 (old ≈ pre/during, new ≈ post), so the analysis is scoped to the
  legacy export, which runs Jun 2019 – Oct 2022. The `combined` and `new_only`
  variants and anything depending on the post-2022 period (notably the pandemic
  comparison) are exploratory context only, not sources of reported findings.
  → `DATASHEET.md` §2, `statistical_decisions.md` §9, report §2

---

*Once answered, delete the `TO CONFIRM` markers in `DATASHEET.md` /
`DATA_GOVERNANCE.md` and replace with the confirmed facts.*
