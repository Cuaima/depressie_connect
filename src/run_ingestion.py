# =============================================================================
# run_ingestion.py  –  orchestrate Step 0 of the pipeline
#
# Run this before preprocess.py when merging old + new data exports.
# Step 1: diagnose_new_data.main()   – read-only diagnostics; review output
# Step 2: integrate_datasets.run_integration() – build ID bridge + merge
#
# Review the diag_*.csv reports written by step 1 before proceeding to step 2.
#
# Run with:  PYTHONPATH=./src python src/run_ingestion.py
# =============================================================================

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import diagnose_new_data
import integrate_datasets


def main():
    print("=" * 60)
    print("Step 1: Diagnosing new data (read-only)…")
    print("=" * 60)
    diagnose_new_data.main()

    print()
    input("  Review output/diag_*.csv before continuing. Press Enter to proceed…")

    print()
    print("=" * 60)
    print("Step 2: Integrating datasets…")
    print("=" * 60)
    integrate_datasets.run_integration()

    print()
    print("✓ Ingestion complete.")
    print("  Next: run preprocess.py")


if __name__ == "__main__":
    main()
