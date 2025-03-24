#!/bin/bash

# Script to run scheduled integrity checks
# Can be set up as a cron job

# Change to project directory
cd "$(dirname "$0")"

# Set environment variables if needed
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"

# Run validation tests only
./run_tests.sh --validation > logs/validation_$(date +"%Y%m%d_%H%M%S").log 2>&1

# Exit with the test result status
exit $?