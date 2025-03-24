#!/bin/bash
# Comprehensive data integrity check script for NativeLLM
# This script runs a series of checks to ensure data consistency and correctness
# It can be scheduled to run periodically or after data processing operations

set -e # Exit on error

# Check for --fix flag
FIX_ISSUES=false
if [[ "$1" == "--fix" ]]; then
    FIX_ISSUES=true
    echo "Running in FIX mode - issues will be automatically corrected"
fi

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure virtual environment is activated
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Make sure tabulate is installed
pip install tabulate > /dev/null

# Set output files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="data_integrity_${TIMESTAMP}.log"
RESULTS_FILE="data_integrity_results_${TIMESTAMP}.json"

echo "Starting data integrity checks at $(date)"
echo "Logs will be written to $LOG_FILE"

# AAPL - First test a single company that should be mostly correct
echo "Testing AAPL as benchmark company..."
if [[ "$FIX_ISSUES" == "true" ]]; then
    python validate_data_integrity.py --ticker AAPL --fix --output "${RESULTS_FILE%.json}_AAPL.json" 2>&1 | tee -a "$LOG_FILE"
else
    python validate_data_integrity.py --ticker AAPL --output "${RESULTS_FILE%.json}_AAPL.json" 2>&1 | tee -a "$LOG_FILE"
fi

# Run checks for all companies
echo ""
echo "Testing all companies..."
if [[ "$FIX_ISSUES" == "true" ]]; then
    python validate_data_integrity.py --all-companies --fix --output "$RESULTS_FILE" 2>&1 | tee -a "$LOG_FILE"
else
    python validate_data_integrity.py --all-companies --output "$RESULTS_FILE" 2>&1 | tee -a "$LOG_FILE"
fi

# Check exit code
if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    echo "Data integrity checks completed successfully"
    echo "Results saved to $RESULTS_FILE"
    echo "Log saved to $LOG_FILE"
    exit 0
else
    echo "Data integrity checks failed"
    echo "See $LOG_FILE for details"
    exit 1
fi