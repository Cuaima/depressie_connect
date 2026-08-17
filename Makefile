VENV   := .venv
PYTHON := python3.12
PIP    := $(VENV)/bin/pip3.12
PY     := $(VENV)/bin/python3.12
export PYTHONPATH := src

# Optional: override with  make preprocess DATASET=old
DATASET      ?=
DATASET_FLAG  = $(if $(DATASET),--dataset $(DATASET),)

.PHONY: help install clean \
        ingest \
        preprocess preprocess-all \
        postprocess postprocess-all \
        pipeline pipeline-all \
        eda cds liwc analyse analyse-all \
        liwc22 liwc-validate liwc22-all liwc-validate-all \
        pandemic pandemic-all \
        full-report full-report-all \
        master-report master-report-all \
        longitudinal longitudinal-all \
        export export-all \
        app \
        test

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "Setup"
	@echo "  make install            Create venv and install dependencies"
	@echo ""
	@echo "Single-export pipeline  (one data/messages.csv)"
	@echo "  make ingest             Step 0: diagnose + integrate (only if merging exports)"
	@echo "  make preprocess         Step 1: clean, filter, anonymize"
	@echo "  make postprocess        Step 2: thread structure, group filter, normalize"
	@echo "  make pipeline           Steps 1–2 in sequence"
	@echo ""
	@echo "Three-dataset pipeline  (after make ingest)"
	@echo "  make preprocess-all     Step 1 for old, new_only, combined"
	@echo "  make postprocess-all    Step 2 for old, new_only, combined"
	@echo "  make pipeline-all       Steps 1–2 for all three datasets"
	@echo ""
	@echo "  Target one dataset:     make preprocess DATASET=old"
	@echo "                          make postprocess DATASET=new_only"
	@echo ""
	@echo "Analysis"
	@echo "  make eda                EDA report  (exploration.py)"
	@echo "  make cds                CDS analysis (exploratory_analysis + cds_prevalence)"
	@echo "  make liwc               LIWC analysis (liwc_analysis)"
	@echo "  make analyse            All three analysis scripts"
	@echo "  make analyse-all        All three analysis scripts for old, new_only, and combined"
	@echo "  make longitudinal       Per-user LIWC/CDS time series (user_longitudinal)"
	@echo "  make longitudinal-all   Longitudinal analysis for old, new_only, and combined"
	@echo "  make full-report        Single-pass consolidated PDF (full_report.py)"
	@echo "  make full-report-all    Full report for old, new_only, and combined"
	@echo "  make master-report      Merge sub-report PDFs with pypdf (build_master_report.py)"
	@echo "  make master-report-all  Master report for all three variants"
	@echo "  make pipeline-all && make analyse-all && make longitudinal-all && make master-report-all"
	@echo ""
	@echo "LIWC-22 validation  (requires LIWC-22 app installed and licensed)"
	@echo "  make liwc22             Run LIWC-22 CLI → liwc22_scores.csv"
	@echo "  make liwc-validate      Run LIWC-22 CLI + produce validation report PDF"
	@echo "  make liwc22-all         LIWC-22 scores for old, new_only, and combined"
	@echo "  make liwc-validate-all  Full validation for old, new_only, and combined"
	@echo "  Note: run 'make liwc' (custom scorer) before 'make liwc-validate'"
	@echo ""
	@echo "Pandemic-period analysis  (needs liwc and/or liwc22 scores first)"
	@echo "  make pandemic           Pre/during/post comparison → pandemic_period_report.pdf"
	@echo "  make pandemic-all       Pandemic comparison for old, new_only, and combined"
	@echo ""
	@echo "  Dataset flag:           make eda DATASET=old  (works for eda/cds/liwc/full-report/master-report)"
	@echo ""
	@echo "Export"
	@echo "  make export             Export anonymized messages + topics to Excel (combined)"
	@echo "  make export-all         Export all three dataset variants to Excel"
	@echo ""
	@echo "App"
	@echo "  make app                Launch Streamlit dashboard"
	@echo ""
	@echo "  make clean              Remove virtual environment"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

clean:
	rm -rf $(VENV)
	find . -type d -name __pycache__ -exec rm -rf {} +

# ── Step 0: Ingest ────────────────────────────────────────────────────────────

ingest:
	$(PY) src/run_ingestion.py

# ── Step 1: Preprocess ────────────────────────────────────────────────────────

preprocess:
	$(PY) src/preprocess.py $(DATASET_FLAG)

preprocess-all:
	$(PY) src/preprocess.py --dataset old
	$(PY) src/preprocess.py --dataset new_only
	$(PY) src/preprocess.py --dataset combined

# ── Step 2: Postprocess ───────────────────────────────────────────────────────

postprocess:
	$(PY) src/postprocess.py $(DATASET_FLAG)

postprocess-all:
	$(PY) src/postprocess.py --dataset old
	$(PY) src/postprocess.py --dataset new_only
	$(PY) src/postprocess.py --dataset combined

# ── Combined pipeline targets ─────────────────────────────────────────────────

pipeline: preprocess postprocess

pipeline-all: preprocess-all postprocess-all

# ── Analysis ──────────────────────────────────────────────────────────────────

eda:
	$(PY) src/exploration.py $(DATASET_FLAG)

cds:
	$(PY) src/exploratory_analysis.py $(DATASET_FLAG)
	$(PY) src/cds_prevalence.py $(DATASET_FLAG)

liwc:
	$(PY) src/liwc_analysis.py $(DATASET_FLAG)

analyse: eda cds liwc

analyse-all:
	$(PY) src/exploration.py --dataset old
	$(PY) src/exploration.py --dataset new_only
	$(PY) src/exploration.py --dataset combined
	$(PY) src/exploratory_analysis.py --dataset old
	$(PY) src/exploratory_analysis.py --dataset new_only
	$(PY) src/exploratory_analysis.py --dataset combined
	$(PY) src/cds_prevalence.py --dataset old
	$(PY) src/cds_prevalence.py --dataset new_only
	$(PY) src/cds_prevalence.py --dataset combined
	$(PY) src/liwc_analysis.py --dataset old
	$(PY) src/liwc_analysis.py --dataset new_only
	$(PY) src/liwc_analysis.py --dataset combined
	@echo ""
	@echo "NOTE: LIWC-22 validation was not run (requires licensed LIWC-22 app)."
	@echo "      liwc_scores CSV files are now ready. To run LIWC-22 next:"
	@echo "        make liwc-validate-all"

full-report:
	$(PY) src/full_report.py $(DATASET_FLAG)

full-report-all:
	$(PY) src/full_report.py --all

master-report:
	$(PY) src/build_master_report.py $(DATASET_FLAG)

master-report-all:
	$(PY) src/build_master_report.py --all-variants

longitudinal:
	$(PY) src/user_longitudinal.py $(DATASET_FLAG)

longitudinal-all:
	$(PY) src/user_longitudinal.py --dataset old
	$(PY) src/user_longitudinal.py --dataset new_only
	$(PY) src/user_longitudinal.py --dataset combined

# ── LIWC-22 validation ────────────────────────────────────────────────────────
# Requires LIWC-22 app installed and licensed.
# Run 'make liwc' (custom scorer) before 'make liwc-validate' so liwc_scores.csv
# exists for the comparison.

liwc22:
	$(PY) src/liwc22_cli_runner.py $(DATASET_FLAG)

liwc-validate:
	$(PY) src/liwc22_cli_runner.py $(DATASET_FLAG)
	$(PY) src/liwc_validation_report.py $(DATASET_FLAG)

liwc22-all:
	$(PY) src/liwc22_cli_runner.py --dataset old
	$(PY) src/liwc22_cli_runner.py --dataset new_only
	$(PY) src/liwc22_cli_runner.py --dataset combined

liwc-validate-all:
	$(PY) src/liwc22_cli_runner.py --dataset old
	$(PY) src/liwc_validation_report.py --dataset old
	$(PY) src/liwc22_cli_runner.py --dataset new_only
	$(PY) src/liwc_validation_report.py --dataset new_only
	$(PY) src/liwc22_cli_runner.py --dataset combined
	$(PY) src/liwc_validation_report.py --dataset combined

# ── Pandemic-period analysis ─────────────────────────────────────────────────
# Reads liwc_scores.csv and/or liwc22_scores.csv — run 'make liwc' (and
# optionally 'make liwc22') first.

pandemic:
	$(PY) src/pandemic_period_analysis.py $(DATASET_FLAG)

pandemic-all:
	$(PY) src/pandemic_period_analysis.py --all

# ── Excel export ─────────────────────────────────────────────────────────────

export:
	$(PY) scripts/export_to_excel.py $(DATASET_FLAG)

export-all:
	$(PY) scripts/export_to_excel.py --all

# ── App ───────────────────────────────────────────────────────────────────────

app:
	$(PY) -m streamlit run src/app.py

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	$(PY) -m pytest
