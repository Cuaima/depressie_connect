## 3. Related work and analysis mapping

This project operationalizes a set of prior findings on the language of
depression and online peer support. Each study motivates a specific part of the
pipeline. The table below states, for each study, what it contributes and which
analysis carries it. A fuller two-way index is kept in
`docs/studies/README.md`.

| Study | Contribution to this project | Analysis |
|---|---|---|
| 113 helpline study (cognitive distortion prevalence) | The 12-category, 265-marker Dutch CDS scheme, and aggregate-level reporting to avoid pseudo-replication | CDS prevalence (5a) |
| Al-Mosaiwi and Johnstone (2018) | Absolutist words as a marker specific to anxiety and depression | LIWC markers (5b) |
| Smirnova (2018) | First-person singular pronoun use distinguishes mild depression from normal sadness | LIWC markers (5b) |
| Eichstaedt et al. (2018) | First-person singular use among the strongest language predictors of recorded depression | LIWC markers (5b) |
| Yahya and Abdul Rahim (2023) | Pre-during-post pandemic comparison of depression markers; feature set for the comparison | Pandemic comparison (5d) |
| Milne et al. (2019) | Moderators act by platform role rather than as peers | Moderator exclusion (methods 4.2) |
| Smit et al. (Depression Connect engagement) | Forum engagement is associated with recovery-related empowerment | Thread-reply structure |
| Ahani et al. (social support detection) | Social support detection as an NLP task using psycholinguistic features | Post-versus-reply structure |

The methodological departure from Yahya and Abdul Rahim (2023) is deliberate and
is described in the methods section: their corpus-level log-likelihood test pools
all messages, which this project avoids by aggregating to per-user rates before
testing.
