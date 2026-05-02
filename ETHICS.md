# Data Science Ethics Checklist

[![Deon badge](https://img.shields.io/badge/ethics%20checklist-deon-brightgreen.svg?style=popout-square)](http://deon.drivendata.org/)

## A. Data Collection
 - [ ] **A.1 Informed consent**: If there are human subjects, have they given informed consent, where subjects affirmatively opt-in and have a clear understanding of the data uses to which they consent?
 - [ ] **A.2 Collection bias**: Have we considered sources of bias that could be introduced during data collection and survey design and taken steps to mitigate those?
 - [ ] **A.3 Limit PII exposure**: Have we considered ways to minimize exposure of personally identifiable information (PII) for example through anonymization or not collecting information that isn't relevant for analysis?
 - [ ] **A.4 Downstream bias mitigation**: Have we considered ways to enable testing downstream results for biased outcomes (e.g., collecting data on protected group status like race or gender)?
 - [ ] **A.5 Sampling frame documentation** (REFORMS 3b): Have we clearly described the sampling frame and any known limitations of the data (e.g., underrepresentation of certain groups, geographic or temporal coverage gaps) and noted any ways it may differ from the target population about which claims are made? [^1]
 - [ ] **A.6 Datasheet for the dataset**(Mitchell et al.): Have we produced or consulted a datasheet that records the dataset's motivation, composition, collection process, preprocessing steps, recommended uses, and known limitations so that future users can make informed decisions about its appropriateness for their task? [^2]

## B. Data Storage
 - [ ] **B.1 Data security**: Do we have a plan to protect and secure data (e.g., encryption at rest and in transit, access controls on internal users and third parties, access logs, and up-to-date software)?
 - [ ] **B.2 Right to be forgotten**: Do we have a mechanism through which an individual can request their personal information be removed?
 - [ ] **B.3 Data retention plan**: Is there a schedule or plan to delete the data after it is no longer needed?
 - [ ] **B.4 Right to erasure**: Have we implemented a process to allow individuals to request deletion of their personal data in compliance with relevant regulations (e.g., GDPR's "right to be forgotten") and have we communicated this right clearly to data subjects?
 - [ ] **B.5 Persistent dataset identification** (REFORMS 2a): Is the exact version of the dataset used in our analysis uniquely identified (e.g., through a DOI or versioned URL) so that future researchers can reproduce or audit our work against the same data? [^3]
 - [ ] **B.6 Dataset maintenance and long-term access** (Mitchell et al.): Have we designated a maintainer for the dataset, specified a process for handling error reports or update requests, and planned for long-term hosting so that the data remains accessible and trustworthy over time?

## C. Analysis
 - [ ] **C.1 Missing perspectives**: Have we sought to address blindspots in the analysis through engagement with relevant stakeholders (e.g., checking assumptions and discussing implications with affected communities and subject matter experts)?
 - [ ] **C.2 Dataset bias**: Have we examined the data for possible sources of bias and taken steps to mitigate or address these biases (e.g., stereotype perpetuation, confirmation bias, imbalanced classes, or omitted confounding variables)?
 - [ ] **C.3 Honest representation**: Are our visualizations, summary statistics, and reports designed to honestly represent the underlying data?
 - [ ] **C.4 Privacy in analysis**: Have we ensured that data with PII are not used or displayed unless necessary for the analysis?
 - [ ] **C.5 Auditability**: Is the process of generating the analysis well documented and reproducible if we discover issues in the future?
 - [ ] **C.6 Data leakage prevention** (REFORMS 6a–6c): Have we verified that no information from the test set is used during training or model selection, and that each input feature is legitimate for the task and does not serve as a proxy shortcut? [^4]
 - [ ] **C.7 Missing data reporting** (REFORMS 3f): Have we reported the extent of missing data, broken down by outcome class where applicable, and documented how missingness was handled (e.g., imputation methods, exclusion criteria) to allow others to assess potential biases introduced by missing data? [^5]

## D. Modeling
 - [ ] **D.1 Proxy discrimination**: Have we ensured that the model does not rely on variables or proxies for variables that are unfairly discriminatory?
 - [ ] **D.2 Fairness across groups**: Have we tested model results for fairness with respect to different affected groups (e.g., tested for disparate error rates)?
 - [ ] **D.3 Metric selection**: Have we considered the effects of optimizing for our defined metrics and considered additional metrics?
 - [ ] **D.4 Explainability**: Can we explain in understandable terms a decision the model made in cases where a justification is needed?
 - [ ] **D.5 Communicate bias**: Have we communicated the shortcomings, limitations, and biases of the model to relevant stakeholders in ways that can be generally understood?
 - [ ] **D.6 Model card** (Mitchell et al.): Have we produced a model card documenting the model's intended use, out-of-scope uses, evaluated populations, performance disaggregated by relevant subgroups, and known limitations to inform users and stakeholders about the model's appropriate applications and potential risks? [^6]
 - [ ] **D.7 Hyperparameter transparency** (REFORMS 5e): Have we fully reported the hyperparameter search space, the selection procedure (e.g., grid search, random search, Bayesian optimization), and the final hyperparameter values chosen for the model to ensure that the optimization process itself does not constitute a hidden source of overfitting or cherry-picking? [^7]

## E. Deployment
 - [ ] **E.1 Redress**: Have we discussed with our organization a plan for response if users are harmed by the results (e.g., how does the data science team evaluate these cases and update analysis and models to prevent future harm)?
 - [ ] **E.2 Roll back**: Is there a way to turn off or roll back the model in production if necessary?
 - [ ] **E.3 Concept drift**: Do we test and monitor for concept drift to ensure the model remains fair over time?
 - [ ] **E.4 Unintended use**: Have we taken steps to identify and prevent unintended uses and abuse of the model and do we have a plan to monitor these once the model is deployed?
 - [ ] **E.5 Generalizability boundaries**(REFORMS 8a–8b): Have we provided evidence of external validity and explicitly described the contexts (e.g., populations, settings, time periods) in which we do not expect our findings to hold? [^8]
 - [ ] **E.6 Affected parties and feedback channels** (Mitchell et al.): Have we identified all groups likely to be affected by model outputs, established channels for receiving feedback from these groups, and committed to responding to concerns raised to ensure that the model's impact is continuously monitored and addressed? [^9]


    [^1]: The dataset is restricted to a clearly defined sampling frame (single platform, language, and time window), and exclusions are explicitly documented. These constraints are treated as threats to external validity, and all claims are scoped accordingly to avoid overgeneralization beyond the observed population.
    [^2]: I am producing a datasheet covering the dataset's motivation (academic research under a data-sharing agreement), composition (four CSV relational files), known preprocessing (pronoun removal, pseudonymization), recommended uses (support detection in Dutch mental health text), and limitations (no moderator IDs, no engagement metrics, inconsistent forum-type labels). This will be submitted as a supplementary appendix to the thesis.
    [^3]: Because the dataset is private and cannot be publicly deposited, I document its version via the exact date range of the extract, logged in the project's data governance record. The four source CSV files are identified by filename and row counts to allow internal audit. If an extended extract is obtained, it will be versioned separately as the out-of-time validation set.
    [^4]: A stratified 70/15/15 split is applied before any feature engineering; all normalization, imputation, and SMOTE oversampling are fitted exclusively on the training partition and applied to validation and test sets. Splits are stratified by user so no individual contributes messages to more than one partition, preventing repeated-measures leakage. Each feature is reviewed for construct validity.
    [^5]: Missingness is documented at the feature level. Missingness rates are reported separately for the "successful" and "unsuccessful" thread classes to detect differential patterns.
    [^6]: A model card is produced for the final selected model, covering: intended use, explicitly out-of-scope uses, evaluated population, performance disaggregated by support type and user activity level, and known limitations including language specificity and the absence of demographic metadata.
    [^7]: Grid search hyperparameter spaces are fully specified in the thesis appendix for all four models, including the ranges tested and the selection criterion. Final hyperparameter values for the reported model are listed in a results table. 
    [^8]: Model validity is explicitly bounded to the population and temporal context represented in the training data. Limitations in geographic, linguistic, and platform diversity are acknowledged, and no claims are made beyond these conditions without further validation.
    [^9]: The primary affected parties are forum users posting in vulnerable states, the platform moderators who may act on model outputs, and the managing mental health organization. Results and the model card are shared with the platform operator prior to any application. The thesis explicitly recommends that any deployment of automated flagging include human moderator oversight and a clear user-facing mechanism to contest automated decisions.

*Data Science Ethics Checklist generated with [deon](http://deon.drivendata.org).*
*Additional items (A.5–A.6, B.4–B.6, C.6–C.7, D.6–D.7, E.5–E.6) derived from:*

 - *Kapoor et al. (2024). REFORMS: Consensus-based Recommendations for Machine-learning-based Science. Science Advances, 10, eadk3452.*
 - *Mitchell et al. (2019). Model Cards for Model Reporting. Proceedings of FAccT 2019.*
