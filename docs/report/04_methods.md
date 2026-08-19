## 4. Methods

### 4.1 Dataset variants and weighting

The corpus exists in three variants: the legacy export (`old`, 2019 to 2022),
the newer export with previously seen messages removed (`new_only`, mostly 2022
onward), and their union (`combined`). The two exports were produced at
different times by different extraction runs, and the older export has been
verified more thoroughly. All analyses therefore treat the `old` variant as the
primary evidence. Results from `new_only` and `combined` are reported as
supporting material and marked exploratory where they depend on the newer
export.

### 4.2 Filtering

Filtering is applied once, before any analysis, so every script operates on the
same population. Test and demo accounts are removed, together with eight
confirmed moderator accounts and the threads they opened. Introduction and
recreational groups are excluded because their language differs in kind from
support-seeking threads. Messages under five words are dropped, and users with
fewer than five messages are excluded, because one or two messages cannot
support a stable per-user estimate. Named entities were replaced with
placeholders during pseudonymization; these placeholders are stripped before any
scoring so they cannot inflate word counts or dictionary matches.

### 4.3 Unit of analysis

The user, never the message. Messages from one author share that author's
style and state, so treating them as independent observations inflates the
effective sample size. This is the pseudo-replication problem. All statistics
in this report first aggregate each feature to a per-user mean rate and then
test at the user level. This choice follows the aggregate-level reporting in
the 113 helpline study and deliberately departs from the corpus-level
log-likelihood testing in Yahya and Abdul Rahim (2023), which pools all
messages and is exposed to exactly this problem.

### 4.4 Statistical tests

Per-user rates are compared with rank-based tests, since the distributions are
right-skewed. Two-group comparisons (posts versus replies) use Mann-Whitney U
with the rank-biserial correlation as effect size. The three pandemic periods
are compared with a Kruskal-Wallis omnibus test per feature (epsilon-squared
effect size); only features surviving correction proceed to pairwise
Mann-Whitney post-hoc tests. All p-values are corrected with the
Benjamini-Hochberg false discovery rate procedure. FDR control was chosen over
family-wise error control because the tested categories are correlated and the
research question concerns a ranked list of markers rather than any single
comparison. Tests are only run where each group contains at least five users;
smaller groups are reported as skipped.

### 4.5 Psycholinguistic features

Cognitive distortion schemata (CDS) are detected with the 12-category,
265-marker Dutch n-gram set from the 113 helpline study. LIWC categories are
scored twice, by a custom scorer using the Dutch LIWC-2015 dictionary and by
the official LIWC-22 CLI with the Dutch LIWC-22 dictionary. Agreement between
the two scorers is reported in the validation section and supports treating
the LIWC results as implementation-independent. First-person singular pronouns
are always available as a feature regardless of dictionary version, given
their status as a replicated depression marker (Smirnova 2018; Eichstaedt et
al. 2018). Absolutist words use a Dutch translation of the 19-word list from
Al-Mosaiwi and Johnstone (2018).

### 4.6 Pandemic periods

Messages are assigned to three periods by timestamp: before 11 March 2020 (WHO
pandemic declaration), from then until 23 March 2022 (end of most Dutch
restrictions), and after. The Dutch end date was chosen over the WHO emergency
end date because the forum population is Dutch. Because users may post in more
than one period, the analysis is run twice: once on all users and once
restricted to users active in a single period. Divergence between the two runs
indicates that multi-period users drive a result, and is reported where it
occurs.
