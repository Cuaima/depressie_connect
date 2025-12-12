#!/usr/bin/env bash
# This tells the system to use bash to interpret this script

# Safety flags - these make your script more robust:
set -euo pipefail
# -e: Exit immediately if any command fails (no silent failures)
# -u: Treat unset variables as errors (catch typos)
# -o pipefail: Ensure pipeline commands fail properly (detect failures in pipes)Why these flags matter: Without these flags, your script might continue running even after errors occur, leading to inconsistent or corrupted results. This is especially critical for data processing scripts!

# Print status message to user
echo "[INFO] Preparing data directory structure..."

# Create data directory 
mkdir -p data
# -p flag: Create parent directories as needed, don't error if directory exists