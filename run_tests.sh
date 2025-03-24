#!/bin/bash

# Run all tests from the tests directory
cd "$(dirname "$0")"

# Set PYTHONPATH to include project root
export PYTHONPATH=$(pwd):$PYTHONPATH

# Check for arguments
if [ $# -eq 0 ]; then
    python tests/run_all_tests.py
else
    python tests/run_all_tests.py "$@"
fi