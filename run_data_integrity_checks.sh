#!/bin/bash

# Script to run data integrity checks in CI pipeline
# Exits with non-zero status if any tests fail

# Change to project directory
cd "$(dirname "$0")"

# Run validation tests
echo "Running data integrity checks..."
./run_tests.sh --validation

# Store the exit code
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ All data integrity checks passed"
else
    echo "❌ Some data integrity checks failed"
fi

exit $EXIT_CODE