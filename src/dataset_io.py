# =============================================================================
# dataset_io.py  –  shared dataset-variant resolution for the whole pipeline
#
# Every analysis/report script imports from here to resolve filenames for the
# three dataset variants:
#
#   "old"      – only the legacy forum export
#   "new_only" – only the new forum export (deduped against old)
#   "combined" – old + new merged (the default; keeps legacy filenames)
#
# "combined" output filenames are unchanged from before (no suffix), keeping
# any notebooks that hardcode e.g. "output/eda_report_all_users.pdf" working.
# =============================================================================

from __future__ import annotations

import os

DATASET_CHOICES = ["old", "new_only", "combined"]
DEFAULT_DATASET = "combined"


def add_dataset_arg(parser, default: str = DEFAULT_DATASET):
    """Adds a standard --dataset flag to an argparse.ArgumentParser."""
    parser.add_argument(
        "--dataset",
        choices=DATASET_CHOICES,
        default=default,
        help=f"Which dataset variant to run on (default: {default}).",
    )
    return parser


def suffix(dataset: str) -> str:
    """'' for combined (keeps legacy filenames), '_old' / '_new_only' otherwise."""
    if dataset not in DATASET_CHOICES:
        raise ValueError(f"Unknown dataset '{dataset}', expected one of {DATASET_CHOICES}")
    return "" if dataset == "combined" else f"_{dataset}"


def variant_path(output_dir: str, base_filename: str, dataset: str) -> str:
    """Insert the dataset suffix before the file extension."""
    root, ext = os.path.splitext(base_filename)
    return os.path.join(output_dir, f"{root}{suffix(dataset)}{ext}")


# ── Canonical filenames at each pipeline stage ────────────────────────────────

def integrated_input_path(output_dir: str, dataset: str) -> str:
    """The file integrate_datasets.py writes for this variant."""
    name = {
        "old":      "messages_old.csv",
        "new_only": "messages_new_only.csv",
        "combined": "integrated_messages.csv",
    }[dataset]
    return os.path.join(output_dir, name)


def preprocessed_community_path(preprocess_dir: str, dataset: str) -> str:
    """What preprocess.py writes / postprocess.py reads."""
    return variant_path(preprocess_dir, "messages_community.csv", dataset)


def community_path(output_dir: str, dataset: str) -> str:
    """Preprocessed CSV — written by preprocess.py, read by postprocess.py.
    Analysis scripts should use structured_path() instead."""
    preprocess_dir = os.path.join(output_dir, "preprocessed")
    return preprocessed_community_path(preprocess_dir, dataset)


def structured_path(output_dir: str, dataset: str) -> str:
    """Primary input for all analysis scripts — group-filtered and thread-structured.
    Written by postprocess.py (output/messages_structured*.csv)."""
    return variant_path(output_dir, "messages_structured.csv", dataset)


def subtitle_for(dataset: str) -> str:
    return {
        "old":      "Old Data Only",
        "new_only": "New Data Only",
        "combined": "Combined Dataset",
    }[dataset]
