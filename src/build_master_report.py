# =============================================================================
# build_master_report.py  –  merge all sub-reports into one consolidated PDF
#
# Combines, for a given dataset variant:
#   1. eda_report_all_users{_variant}.pdf
#   2. eda_report_multi_posters{_variant}.pdf
#   3. exploratory_report{_variant}.pdf        (time series + CDS)
#   4. cds_prevalence_report{_variant}.pdf
#   5. liwc_report{_variant}.pdf
#   6. user_longitudinal_report{_variant}.pdf
#
# into a single output/master_report{_variant}.pdf with a title page and a
# divider page before each section.
#
# By default this only merges – it assumes the individual scripts have already
# been run. Pass --run to have it call each script first.
#
# Usage:
#   python src/build_master_report.py --dataset combined
#   python src/build_master_report.py --dataset combined --run
#   python src/build_master_report.py --all-variants --run
# =============================================================================

from __future__ import annotations

import os
import argparse
import subprocess
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
from pypdf import PdfReader, PdfWriter

from dataset_io import DATASET_CHOICES, add_dataset_arg, variant_path, subtitle_for

OUTPUT_DIR = "output"
PRIMARY    = "#2E5E8E"
SECONDARY  = "#EEF3F8"

# (script_name, base_filename, section_title)  –  order = order in merged PDF
# Note: liwc22_cli_runner.py is a prerequisite for section 7 but produces only
# a CSV, not a PDF, so it is not listed here — see run_sub_reports() below.
SUB_REPORTS = [
    ("eda_report.py",               "eda_report_all_users.pdf",          "1. EDA – All Users"),
    ("eda_report.py",               "eda_report_multi_posters.pdf",      "2. EDA – Multi-Posters Only"),
    ("exploratory_analysis.py",     "exploratory_report.pdf",            "3. Time Series & CDS Prevalence"),
    ("cds_prevalence.py",           "cds_prevalence_report.pdf",         "4. Cognitive Distortion (CDS) Prevalence"),
    ("liwc_analysis.py",            "liwc_report.pdf",                   "5. LIWC Psycholinguistic Analysis"),
    ("user_longitudinal.py",        "user_longitudinal_report.pdf",      "6. Per-User Longitudinal Trends"),
    ("liwc_validation_report.py",   "liwc_validation_report.pdf",        "7. LIWC-22 Validation"),
    ("pandemic_period_analysis.py", "pandemic_period_report.pdf",        "8. Pandemic-Period Comparison"),
]


def _title_page(dataset: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor(PRIMARY)
    ax.set_facecolor(PRIMARY)
    ax.axis("off")
    ax.text(0.5, 0.62, "Depression Connect Forum", transform=ax.transAxes,
            ha="center", va="center", fontsize=26, fontweight="bold", color="white")
    ax.text(0.5, 0.50, "Master Analysis Report", transform=ax.transAxes,
            ha="center", va="center", fontsize=16, color="#DDDDDD")
    ax.text(0.5, 0.38, f"Dataset variant: {subtitle_for(dataset)}", transform=ax.transAxes,
            ha="center", va="center", fontsize=13, color="#DDDDDD")
    ax.text(0.5, 0.12,
            "EDA · Time series · CDS prevalence · LIWC · Longitudinal trends · LIWC-22 validation",
            transform=ax.transAxes, ha="center", va="center", fontsize=9, color="#AAAAAA")
    return fig


def _divider_page(title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 2.3))
    fig.patch.set_facecolor(SECONDARY)
    ax.set_facecolor(SECONDARY)
    ax.axis("off")
    ax.text(0.5, 0.5, title, transform=ax.transAxes, ha="center", va="center",
            fontsize=18, fontweight="bold", color=PRIMARY)
    return fig


def _save_temp_pdf(fig: plt.Figure, path: str):
    with pdf_backend.PdfPages(path) as pdf:
        pdf.savefig(fig, bbox_inches="tight")
    plt.close("all")


def run_sub_reports(dataset: str, top_n: int = 5):
    """Call each sub-report script for this dataset variant."""
    # liwc22_cli_runner.py is a prerequisite for section 7: run it first so
    # liwc22_scores.csv exists before liwc_validation_report.py is called.
    liwc22_scores = variant_path(OUTPUT_DIR, "liwc22_scores.csv", dataset)
    if not os.path.exists(liwc22_scores):
        print(f"\n=== Running liwc22_cli_runner.py (prerequisite for section 7) ===")
        subprocess.run(
            [sys.executable, os.path.join("src", "liwc22_cli_runner.py"),
             "--dataset", dataset],
            check=True,
        )

    for script, _, _ in SUB_REPORTS:
        cmd = [sys.executable, os.path.join("src", script), "--dataset", dataset]
        if script == "user_longitudinal.py":
            cmd += ["--top-n", str(top_n)]
        print(f"\n=== Running {' '.join(cmd)} ===")
        subprocess.run(cmd, check=True)


def merge_reports(dataset: str) -> str:
    writer = PdfWriter()
    tmp_dir = os.path.join(OUTPUT_DIR, "_tmp_master")
    os.makedirs(tmp_dir, exist_ok=True)

    title_pdf = os.path.join(tmp_dir, "title.pdf")
    _save_temp_pdf(_title_page(dataset), title_pdf)
    writer.append(PdfReader(title_pdf))

    missing = []
    for _script, base_filename, section_title in SUB_REPORTS:
        sub_path = variant_path(OUTPUT_DIR, base_filename, dataset)
        if not os.path.exists(sub_path):
            missing.append(sub_path)
            if "liwc_validation" in base_filename:
                ds_flag = f"DATASET={dataset}"
                print(f"\n  ⚠  SKIP — LIWC-22 validation report not found: {sub_path}")
                print(f"     Section 7 will be absent from the master report.")
                print(f"     To generate it, run:")
                print(f"       make liwc-validate {ds_flag}")
                print(f"     (requires LIWC-22 app installed and liwc_scores already present)\n")
            else:
                print(f"  SKIP (not found): {sub_path}")
            continue

        divider_pdf = os.path.join(tmp_dir, "divider.pdf")
        _save_temp_pdf(_divider_page(section_title), divider_pdf)
        writer.append(PdfReader(divider_pdf))

        print(f"  + {sub_path}")
        writer.append(PdfReader(sub_path))

    out_path = variant_path(OUTPUT_DIR, "master_report.pdf", dataset)
    with open(out_path, "wb") as f:
        writer.write(f)

    if missing:
        print(f"\n  NOTE: {len(missing)} sub-report(s) were missing and skipped.")
        for m in missing:
            print(f"    {m}")

    print(f"\n✓ Master report saved → {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Merge all per-stage PDF reports into one master report."
    )
    add_dataset_arg(parser)
    parser.add_argument("--all-variants", action="store_true",
                        help="Build for old, new_only, and combined in one go.")
    parser.add_argument("--run", action="store_true",
                        help="Run each sub-report script before merging.")
    parser.add_argument("--top-n", type=int, default=5,
                        help="Top-N posters for user_longitudinal.py (default: 5).")
    args = parser.parse_args()

    datasets = DATASET_CHOICES if args.all_variants else [args.dataset]
    for ds in datasets:
        print(f"\n{'=' * 70}\nDataset variant: {ds}\n{'=' * 70}")
        if args.run:
            run_sub_reports(ds, top_n=args.top_n)
        merge_reports(ds)


if __name__ == "__main__":
    main()
