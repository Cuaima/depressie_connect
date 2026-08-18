# =============================================================================
# liwc22_cli_runner.py  –  wrap the LIWC-22 CLI as a scored-messages CSV
#
# Runs the LIWC-22 CLI against the structured messages for a dataset variant
# and saves the raw LIWC-22 output (merged with PosterID / PostDate / role)
# to output/liwc22_scores{_variant}.csv.
#
# Prerequisites:
#   - LIWC-22 app installed and licence activated (open the GUI once to activate)
#   - Dutch .dicx dictionary file available at LIWC22_DICT path (see below)
#
# Override paths with environment variables if needed:
#   LIWC22_CLI   – path to the LIWC-22-cli executable
#   LIWC22_DICT  – path to the .dicx dictionary file
#
# Run with:  python src/liwc22_cli_runner.py [--dataset combined|old|new_only]
# =============================================================================

from __future__ import annotations

import os
import argparse
import subprocess
import tempfile
import pandas as pd

from utils.thread_utils import label_roles, strip_entity_placeholders_col, parse_post_dates
from dataset_io import add_dataset_arg, structured_path, variant_path

# ── Configuration ─────────────────────────────────────────────────────────────
LIWC22_CLI  = os.environ.get(
    "LIWC22_CLI",
    "/Applications/LIWC-22.app/Contents/MacOS/LIWC-22-cli",
)
LIWC22_DICT = os.environ.get(
    "LIWC22_DICT",
    "data/LIWC2015 Dictionary - Dutch.dicx",
)

OUTPUT_DIR  = "output"
POSTER_COL  = "PosterID"
TEXT_COL    = "MessageText"
DATE_COL    = "PostDate"
TOPIC_COL   = "ForumTopicID"
ROW_IDX_COL = "_row_idx"   # stable integer key used to re-join results

# LIWC-22 columns that are structural/diagnostic metadata, not content categories.
# Exported here so liwc_validation_report.py can import them without duplicating.
LIWC22_STRUCTURAL_COLS: frozenset[str] = frozenset({
    "Segment", "WC", "WPS", "BigWords", "Dic",
    "AllPunc", "Period", "Comma", "QMark", "Exclam", "Apostro", "OtherP", "Emoji",
})

# These four summary variables only appear with the built-in English LIWC-22
# dictionary. When an external .dicx file is supplied, they are absent from output.
LIWC22_SUMMARY_VARS: tuple[str, ...] = ("Analytic", "Clout", "Authentic", "Tone")


# =============================================================================
# Data loading
# =============================================================================

def load_messages(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df[DATE_COL] = parse_post_dates(df[DATE_COL])
    df = df.dropna(subset=[DATE_COL, TEXT_COL]).reset_index(drop=True)
    df = strip_entity_placeholders_col(df, TEXT_COL)
    print(f"  {len(df)} messages from {df[POSTER_COL].nunique()} users.")
    return df


# =============================================================================
# CLI wrapper
# =============================================================================

def run_liwc22_cli(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the LIWC-22 CLI on df[TEXT_COL] and return the raw scored DataFrame.

    Writes a 2-column temp CSV (_row_idx col 1, MessageText col 2) so the CLI
    receives clean input. The _row_idx column allows deterministic re-joining
    after scoring without relying on row order.

    Raises
    ------
    FileNotFoundError  – CLI executable or dictionary file not found.
    RuntimeError       – CLI exits with non-zero code (licence failure, bad
                         input, etc.). Full stderr is included — do not fall
                         back silently.
    """
    if not os.path.exists(LIWC22_CLI):
        raise FileNotFoundError(
            f"LIWC-22 CLI not found at: {LIWC22_CLI}\n"
            "Set the LIWC22_CLI environment variable to override the path."
        )
    if not os.path.exists(LIWC22_DICT):
        raise FileNotFoundError(
            f"LIWC-22 dictionary not found at: {LIWC22_DICT}\n"
            "Set the LIWC22_DICT environment variable to override the path."
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_in  = os.path.join(tmp_dir, "liwc22_input.csv")
        tmp_out = os.path.join(tmp_dir, "liwc22_output.csv")

        # Write 2-column CSV: _row_idx (column 1), MessageText (column 2)
        df[[ROW_IDX_COL, TEXT_COL]].to_csv(tmp_in, index=False)

        cmd = [
            LIWC22_CLI,
            "-m",    "wc",
            "-i",    tmp_in,
            "-o",    tmp_out,
            "-d",    LIWC22_DICT,
            "-id",   "1",    # _row_idx is column 1 (1-based)
            "-ci",   "2",    # MessageText is column 2 (1-based)
            "-ccol", "no",
        ]

        print(f"\nRunning LIWC-22 CLI on {len(df)} messages…")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            # Strip the benign JLine terminal warning that appears in headless mode
            stderr_clean = "\n".join(
                line for line in result.stderr.splitlines()
                if "jline" not in line.lower() and "dumb terminal" not in line.lower()
            )
            raise RuntimeError(
                f"LIWC-22 CLI failed (exit code {result.returncode}).\n"
                f"stderr:\n{stderr_clean}\n\n"
                "If this is a licence error, open the LIWC-22 app to re-activate it."
            )

        if not os.path.exists(tmp_out):
            raise RuntimeError(
                "LIWC-22 CLI exited with code 0 but produced no output file.\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )

        liwc22_df = pd.read_csv(tmp_out)
        print(f"  LIWC-22 scored {len(liwc22_df)} messages, "
              f"{len(liwc22_df.columns)} output columns.")
        return liwc22_df


# =============================================================================
# Merge and save
# =============================================================================

def merge_and_save(
    df: pd.DataFrame,
    liwc22_raw: pd.DataFrame,
    output_path: str,
) -> str:
    """
    Rename LIWC-22's 'Row ID' output column to _row_idx, join with
    PosterID / PostDate / ForumTopicID / role from the original DataFrame,
    and write to output_path.
    """
    liwc22 = liwc22_raw.rename(columns={"Row ID": ROW_IDX_COL})
    liwc22[ROW_IDX_COL] = liwc22[ROW_IDX_COL].astype(int)

    keep = [c for c in [ROW_IDX_COL, POSTER_COL, DATE_COL, TOPIC_COL, "role"]
            if c in df.columns]
    merged = df[keep].merge(liwc22, on=ROW_IDX_COL)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    merged.to_csv(output_path, index=False)
    print(f"  Saved → {output_path}  ({len(merged)} rows, {len(merged.columns)} columns)")
    return output_path


# =============================================================================
# Main
# =============================================================================

def main(dataset: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    input_path  = structured_path(OUTPUT_DIR, dataset)
    output_path = variant_path(OUTPUT_DIR, "liwc22_scores.csv", dataset)

    print(f"\nLoading messages from {input_path}…")
    df = load_messages(input_path)
    df = label_roles(df)
    df[ROW_IDX_COL] = df.index  # stable 0-based integer after reset_index in load_messages

    liwc22_raw = run_liwc22_cli(df)
    merge_and_save(df, liwc22_raw, output_path)

    print("\n✓ Done.")
    print(f"  {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run LIWC-22 CLI on structured forum messages."
    )
    add_dataset_arg(parser)
    args = parser.parse_args()
    main(dataset=args.dataset)
