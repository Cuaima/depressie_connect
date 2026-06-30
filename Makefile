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
        eda cds liwc analyse \
        full-report full-report-all \
        longitudinal \
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
	@echo "  make full-report        Full consolidated PDF  (all sections, one file)"
	@echo "  make full-report-all    Full report for old, new_only, and combined"
	@echo "  make longitudinal       Per-user LIWC/CDS time series (user_longitudinal)"
	@echo ""
	@echo "  Dataset flag:           make eda DATASET=old  (works for eda/cds/liwc/full-report)"
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

full-report:
	$(PY) src/full_report.py $(DATASET_FLAG)

full-report-all:
	$(PY) src/full_report.py --all

longitudinal:
	$(PY) src/user_longitudinal.py $(DATASET_FLAG)

# ── App ───────────────────────────────────────────────────────────────────────

app:
	$(PY) -m streamlit run src/app.py

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	$(PY) -m pytest
