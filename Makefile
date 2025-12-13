VENV := .venv
PYTHON := python3.11
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

.PHONY: help venv install clean run spark

help:
	@echo "Available targets:"
	@echo "  make venv     Create virtual environment"
	@echo "  make install  Install dependencies"
	@echo "  make run      Run pandas pipeline"
	@echo "  make spark    Run Spark anonymizer"
	@echo "  make clean    Remove virtual environment"

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

run:
	$(PY) src/processor.py

spark:
	$(PY) src/spark_anonymizer.py

clean:
	rm -rf $(VENV)
	rm -rf __pycache__/