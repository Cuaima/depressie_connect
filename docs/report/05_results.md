## 5. Results

Each subsection reports the `old` variant first, as the primary evidence, and
uses `combined` as support. Numbers are from the regenerated outputs; the source
file and the corroborating figure are named for each claim. Figure references
point to sections of the per-variant report PDFs in `output/` (also merged into
`master_report*.pdf`).

### 5a. Cognitive distortion prevalence

Source: `cds_category_ranking_old.csv` (primary), `cds_category_ranking.csv`
(combined). Figures: `cds_prevalence_report_old.pdf`, Section 1 (category
ranking, posts-versus-replies bars, and the statistics table with effect sizes
and corrected p-values).

In the `old` variant, Dichotomous Reasoning is by far the most common cognitive
distortion, appearing in 49.5% of messages. The next most common are
Overgeneralizing (26.9%) and Should statements (22.9%). The remaining nine
categories each appear in under 15% of messages. This ordering is shown in the
category ranking bar chart (Section 1).

Dichotomous Reasoning is markedly more common in opening posts than in replies:
63.3% of opening posts versus 47.8% of replies, with a rank-biserial effect size
of -0.41 (opening posts higher). This gap is shown in the posts-versus-replies
bar chart, and the per-category effect sizes and corrected p-values are in the
statistics table on the same section. Seven of the twelve categories are
significant after Benjamini-Hochberg correction.

The `combined` variant (`cds_prevalence_report.pdf`, Section 1) shows the same
ordering, with Dichotomous Reasoning again first at 47.4%. Its post-versus-reply
gap is milder there (52.7% versus 46.7%, rank-biserial -0.14). With 940 users the
test has more power, so all twelve categories reach significance; most effect
sizes are small, and the effect size rather than the p-value is the better guide
to which differences matter. The weaker gap in `combined` is consistent with the
newer export changing the mix of posts and repliers.

### 5b. LIWC markers

Source: `liwc_scores_old.csv`, `liwc_report_old.pdf`. Figures: first-person
singular by role (Section 4) and absolutist words by role (Section 5) in
`liwc_report_old.pdf`; both categories also appear in the overall
posts-versus-replies chart (Section 2).

First-person singular pronoun use is higher in opening posts than in replies
(7.3% versus 5.4% of words), shown in the first-person singular by role bar chart
(Section 4). This matches the interpretation of first-person singular use as an
index of self-focused attention, which opening posts, written while seeking
support, would be expected to carry more of than replies. The absolutist word
rate is similar across roles (1.4% in posts, 1.7% in replies; Section 5).

These are descriptive means. The per-user statistical testing described in the
methods is applied to the cognitive distortion and pandemic analyses; the LIWC
role means here are reported as description, not as tested contrasts.

### 5c. Scorer validation

Source: `liwc_validation_comparison*.csv`. Figures: per-category correlation bar
chart and the divergence scatter grid in `liwc_validation_report_old.pdf`
(Sections 1 and 2).

The custom LIWC-2015 scorer and the official LIWC-22 CLI agree closely. The
median per-category correlation is 0.999 in the `combined` variant, 0.997 in
`old`, and 0.9975 in `new_only`, and all 73 shared categories correlate above
0.9. The correlation bar chart shows every category above the 0.7 reference line;
the scatter grid shows the point clouds falling on the identity line. This
supports treating the LIWC results as independent of the specific scorer. An
earlier version of this comparison contained a row-alignment defect that made the
two scorers appear uncorrelated; that defect has been corrected and the reports
regenerated.

### 5d. Pandemic period comparison

Source: `pandemic_period_stats*.csv`. Figures: the variant-by-period cross-tab
and the omnibus results table in `pandemic_period_report_old.pdf` (Sections 1 and
3).

In the `old` variant, no feature shows a significant difference across the three
periods in the all-users omnibus test, as the omnibus results table shows (no row
marked significant). In the single-period sensitivity analysis, only netspeak
differs, and in the same direction for both scorers. Period effects on these
markers are therefore weak or absent in the primary variant.

The `combined` variant shows the same overall picture: no feature survives the
all-users omnibus, and the single-period sensitivity flags only pronoun and
netspeak categories. Because message source and calendar time are confounded in
`combined`, which the variant-by-period cross-tab on the first page makes
explicit (the old export covers the pre and during periods, the new export mostly
the after period), these contrasts partly reflect which export a message came
from and are treated as exploratory.

Earlier runs of this analysis, before the data-completeness fix described in the
methods, suggested declines in first-person singular and sadness words after the
pandemic. Those did not survive the correction and are not reported as findings.

### 5e. Sustained-engagement users

Source and figure: `user_longitudinal_sustained.pdf` (the engagement table names
the selected users and their posting shape; the following pages plot each user's
markers over time).

Among users who are both prolific and long-active, two posting shapes appear, as
the engagement table sets out. Long-haul steady users post at a low cadence over
a long span, for example one user with 479 posts across about five years at
roughly eight posts per month. High-intensity users concentrate many posts into a
shorter span, for example one user with 1,656 posts across about three years at
roughly forty-nine posts per month. The time-series pages track each user's
cognitive distortion and LIWC markers over their active months, so that
within-user change over time can be read separately from the between-user
differences in the rest of the report.
