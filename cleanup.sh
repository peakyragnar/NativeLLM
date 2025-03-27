#!/bin/bash
# Cleanup script for NativeLLM project
# This script backs up unnecessary files then removes them

# Set the timestamp for backup directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_DIR="./backup_$TIMESTAMP"

# Create backup directory
mkdir -p "$BACKUP_DIR"

echo "Creating backup of files to be removed in $BACKUP_DIR"

# Read files from the removal list and back them up
while IFS= read -r line || [[ -n "$line" ]]; do
    # Skip comments and empty lines
    [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
    
    # Check if file/directory exists
    if [ -e "$line" ]; then
        # Create target directory structure
        mkdir -p "$BACKUP_DIR/$(dirname "$line")"
        
        # Back up the file/directory
        echo "Backing up: $line"
        cp -r "$line" "$BACKUP_DIR/$line"
    fi
done < "cleanup_lists/files_to_remove.txt"

echo "Backup complete. Now removing files..."

# Read files from the removal list and remove them
while IFS= read -r line || [[ -n "$line" ]]; do
    # Skip comments and empty lines
    [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
    
    # Check if file/directory exists
    if [ -e "$line" ]; then
        echo "Removing: $line"
        rm -rf "$line"
    fi
done < "cleanup_lists/files_to_remove.txt"

# Clean up content from directories we want to keep but empty
echo "Cleaning output directories..."
find sec_processed -mindepth 1 -not -path "*/sec_processed/MSFT*" -exec rm -rf {} \; 2>/dev/null || true
find sec_downloads -mindepth 1 -not -path "*/sec_downloads/MSFT*" -exec rm -rf {} \; 2>/dev/null || true

echo "Cleanup complete!"
echo "Files have been backed up to $BACKUP_DIR"
echo "If you need to restore any files, you can copy them from the backup directory."
echo "To verify everything still works, run:"
echo "  python3 -m src2.sec.batch_pipeline MSFT --start-year 2024 --end-year 2025 --gcp-bucket native-llm-filings --email info@exascale.capital"