#!/bin/bash
# Scheduled data integrity check script for NativeLLM
# This script is designed to be run as a cron job
# It runs data integrity checks and sends notifications if issues are found
# 
# Example cron entry (weekly on Sunday at 1 AM):
# 0 1 * * 0 /path/to/scheduled_integrity_check.sh >> /path/to/scheduled_check.log 2>&1

# Directory setup
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Setup environment
if [[ -f venv/bin/activate ]]; then
    source venv/bin/activate
fi

# Set timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="scheduled_integrity_${TIMESTAMP}.log"
RESULTS_FILE="scheduled_integrity_results_${TIMESTAMP}.json"

# Run validation without fixing
echo "Running scheduled data integrity check at $(date)" > "$LOG_FILE"
python validate_data_integrity.py --all-companies --output "$RESULTS_FILE" >> "$LOG_FILE" 2>&1

# Check if validation found critical issues (extraction from log file)
CRITICAL_ISSUES=$(grep -c "RECOMMENDATION: Significant consistency issues detected" "$LOG_FILE" || true)

if [[ $CRITICAL_ISSUES -gt 0 ]]; then
    # Send notification - modify this section based on your notification method
    SUBJECT="[ALERT] NativeLLM Data Integrity Issues Detected"
    MESSAGE="Critical data integrity issues detected in the NativeLLM system. Please review the log at $SCRIPT_DIR/$LOG_FILE"
    
    # Email notification example (uncomment and configure if needed)
    # echo "$MESSAGE" | mail -s "$SUBJECT" your-email@example.com
    
    # Log the notification
    echo "ALERT: Critical issues detected - notification sent" >> "$LOG_FILE"
    echo "$MESSAGE" >> "$LOG_FILE"
    
    # Exit with error code
    exit 1
else
    echo "Scheduled check completed successfully. No critical issues found." >> "$LOG_FILE"
    
    # Clean up old log and result files (keeping only the last 10)
    find "$SCRIPT_DIR" -name "scheduled_integrity_*.log" -type f -printf '%T@ %p\n' | sort -n | head -n -10 | cut -d' ' -f2- | xargs rm -f
    find "$SCRIPT_DIR" -name "scheduled_integrity_results_*.json" -type f -printf '%T@ %p\n' | sort -n | head -n -10 | cut -d' ' -f2- | xargs rm -f
    
    exit 0
fi