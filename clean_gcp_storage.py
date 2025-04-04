#!/usr/bin/env python3
"""
Script to properly clean up all files in GCP storage and Firestore.
This script combines the functionality of clear_gcp_data.py and
specifically targets accession-based directories that were being missed.
"""

import os
import sys
import argparse
import logging
import re
from google.cloud import firestore, storage
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# GCP settings
GCS_BUCKET_NAME = "native-llm-filings"
FIRESTORE_DB = "nativellm"

# Check for credentials
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    # Try to find credentials file
    potential_paths = [
        "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json",
        "/Users/michael/nativellmfilings-e149eb3298de.json",
        "/Users/michael/Downloads/nativellmfilings-e149eb3298de.json",
        os.path.expanduser("~/nativellmfilings-e149eb3298de.json"),
        os.path.expanduser("~/Downloads/nativellmfilings-e149eb3298de.json")
    ]

    credentials_found = False
    for path in potential_paths:
        if os.path.exists(path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
            logging.info(f"Set GCP credentials from {path}")
            credentials_found = True
            break

    if not credentials_found:
        logging.error("GCP credentials not found. Please set GOOGLE_APPLICATION_CREDENTIALS environment variable.")
        logging.error("You can also place the credentials file in one of these locations:")
        for path in potential_paths:
            logging.error(f"  - {path}")
        sys.exit(1)

def list_ticker_files(ticker=None, dry_run=True):
    """
    List all files in Firestore and GCS for a specific ticker.

    Args:
        ticker: If provided, only check files for this ticker
        dry_run: If True, don't actually delete anything
    """
    db = firestore.Client(database=FIRESTORE_DB)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # Query filings in Firestore - try both collections
    filings = []
    collections = ['filings', 'nativellm']

    for collection in collections:
        if ticker:
            query = db.collection(collection).where('company_ticker', '==', ticker)
        else:
            query = db.collection(collection)

        filings.extend(query.get())

    # Track files
    files_to_remove = []

    for filing_doc in filings:
        filing_data = filing_doc.to_dict()
        filing_id = filing_doc.id
        company_ticker = filing_data.get('company_ticker')
        filing_type = filing_data.get('filing_type')
        fiscal_year = filing_data.get('fiscal_year')
        fiscal_period = filing_data.get('fiscal_period')

        # Get file paths
        text_file_path = filing_data.get('text_file_path')
        llm_file_path = filing_data.get('llm_file_path')

        # Add to list regardless of format - we want to clean all files for the ticker
        files_to_remove.append({
            'filing_id': filing_id,
            'company_ticker': company_ticker,
            'filing_type': filing_type,
            'file_type': 'Document',
            'fiscal_year': fiscal_year,
            'fiscal_period': fiscal_period,
            'text_path': text_file_path,
            'llm_path': llm_file_path
        })

    # If ticker is provided, also check for GCS files directly
    if ticker:
        # List all GCS objects with the ticker prefix
        prefix = f"companies/{ticker}/"
        blobs = list(bucket.list_blobs(prefix=prefix))

        logging.info(f"Found {len(blobs)} GCS objects with prefix {prefix}")

        # Create a map of file paths we've already found in Firestore
        known_paths = set()
        for file_info in files_to_remove:
            if file_info.get('text_path'):
                known_paths.add(file_info['text_path'])
            if file_info.get('llm_path'):
                known_paths.add(file_info['llm_path'])

        # Add any GCS files not already in our list
        for blob in blobs:
            if blob.name not in known_paths:
                files_to_remove.append({
                    'filing_id': None,
                    'company_ticker': ticker,
                    'filing_type': None,
                    'file_type': 'GCS Only',
                    'fiscal_year': None,
                    'fiscal_period': None,
                    'gcs_path': blob.name
                })

    return files_to_remove

def remove_ticker_files(files_to_remove, dry_run=True):
    """
    Remove files from GCP and Firestore for a specific ticker.

    Args:
        files_to_remove: List of file records to remove
        dry_run: If True, don't actually update anything

    Returns:
        int: Number of successfully deleted files
    """
    if not files_to_remove:
        print("No files found to remove.")
        return 0

    db = firestore.Client(database=FIRESTORE_DB)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # Display what will be removed
    print(f"\nFound {len(files_to_remove)} files to remove:")
    table_data = []
    headers = ["Filing ID", "Ticker", "Filing Type", "File Type", "Fiscal Year", "Fiscal Period"]

    for file in files_to_remove:
        table_data.append([
            file.get('filing_id', 'N/A'),
            file.get('company_ticker', 'N/A'),
            file.get('filing_type', 'N/A'),
            file.get('file_type', 'N/A'),
            file.get('fiscal_year', 'N/A'),
            file.get('fiscal_period', 'N/A')
        ])

    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    if dry_run:
        print("\nDRY RUN - No changes made. Run with --execute to remove these files.")
        return 0

    # Actually remove the files
    print("\nRemoving files...")
    successful_deletions = 0

    for file in files_to_remove:
        # Delete from Firestore - check both collections
        if file.get('filing_id'):
            for collection in ['filings', 'nativellm']:
                try:
                    doc_ref = db.collection(collection).document(file['filing_id'])
                    if doc_ref.get().exists:
                        doc_ref.delete()
                        print(f"Deleted Firestore document: {collection}/{file['filing_id']}")
                        successful_deletions += 1
                except Exception as e:
                    print(f"Error deleting Firestore document {collection}/{file['filing_id']}: {str(e)}")

        # Delete text file from GCS if it exists
        if file.get('text_path'):
            try:
                # Skip the exists() check which can be unreliable
                blob = bucket.blob(file['text_path'])
                try:
                    blob.delete()
                    print(f"Deleted GCS text file: {file['text_path']}")
                    successful_deletions += 1
                except Exception as delete_e:
                    if "Not Found" in str(delete_e):
                        print(f"GCS text file not found: {file['text_path']}")
                    else:
                        print(f"Error deleting GCS text file {file['text_path']}: {str(delete_e)}")
            except Exception as e:
                print(f"Error accessing GCS text file {file['text_path']}: {str(e)}")

        # Delete LLM file from GCS if it exists
        if file.get('llm_path'):
            try:
                # Skip the exists() check which can be unreliable
                blob = bucket.blob(file['llm_path'])
                try:
                    blob.delete()
                    print(f"Deleted GCS LLM file: {file['llm_path']}")
                    successful_deletions += 1
                except Exception as delete_e:
                    if "Not Found" in str(delete_e):
                        print(f"GCS LLM file not found: {file['llm_path']}")
                    else:
                        print(f"Error deleting GCS LLM file {file['llm_path']}: {str(delete_e)}")
            except Exception as e:
                print(f"Error accessing GCS LLM file {file['llm_path']}: {str(e)}")

        # Delete standalone GCS file if it exists
        if file.get('gcs_path'):
            try:
                # Skip the exists() check which can be unreliable
                blob = bucket.blob(file['gcs_path'])
                try:
                    blob.delete()
                    print(f"Deleted GCS file: {file['gcs_path']}")
                    successful_deletions += 1
                except Exception as delete_e:
                    if "Not Found" in str(delete_e):
                        print(f"GCS file not found: {file['gcs_path']}")
                    else:
                        print(f"Error deleting GCS file {file['gcs_path']}: {str(delete_e)}")
            except Exception as e:
                print(f"Error accessing GCS file {file['gcs_path']}: {str(e)}")

    print(f"\nCompleted removal with {successful_deletions} successful deletions.")
    return successful_deletions

def clean_accession_directories(ticker, dry_run=True):
    """
    Special function to find and clean accession-based directories in GCS
    which might be missed by the standard file listing.

    Args:
        ticker: The ticker to clean up
        dry_run: If True, don't actually delete anything

    Returns:
        int: Number of files deleted
    """
    if not ticker:
        print("Ticker is required for accession directory cleanup")
        return 0

    print(f"\n===== CLEANING ACCESSION DIRECTORIES FOR {ticker} =====")

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # Check all filing types
    filing_types = ["10-K", "10-Q"]
    deleted_count = 0

    # Pattern to identify accession numbers vs fiscal years
    accession_pattern = re.compile(r'^[0-9]{3,}$')  # Match numeric directories
    fiscal_year_pattern = re.compile(r'^20[0-9]{2}$')  # Match typical fiscal years (2000-2099)

    for filing_type in filing_types:
        # Generate GCS prefix
        prefix = f"companies/{ticker}/{filing_type}/"
        print(f"Checking prefix: {prefix}")

        # List all blobs with this prefix to find directories
        try:
            blobs = list(bucket.list_blobs(prefix=prefix))
            print(f"Found {len(blobs)} total blobs under {prefix}")

            # Extract all possible directory names
            directories = set()
            for blob in blobs:
                # Split path into components
                path_parts = blob.name.split('/')
                # Check if it has enough parts for a directory
                if len(path_parts) >= 4:  # companies/TICKER/FILING_TYPE/DIR/...
                    dir_name = path_parts[3]
                    directories.add(dir_name)

            print(f"Found directories: {directories}")

            # Check each directory to see if it matches an accession number pattern
            for dir_name in directories:
                if accession_pattern.match(dir_name) and not fiscal_year_pattern.match(dir_name):
                    print(f"Found suspected accession directory: {dir_name}")

                    # Count files in this directory
                    dir_prefix = f"{prefix}{dir_name}/"
                    dir_blobs = [b for b in blobs if b.name.startswith(dir_prefix)]

                    print(f"Directory {dir_name} contains {len(dir_blobs)} files")

                    # List each file to be deleted
                    for blob in dir_blobs:
                        print(f"  - {blob.name}")

                    if not dry_run:
                        # Delete all files in this directory
                        for blob in dir_blobs:
                            try:
                                print(f"Deleting file: {blob.name}")
                                blob.delete()
                                deleted_count += 1
                            except Exception as e:
                                print(f"Error deleting file {blob.name}: {str(e)}")
                    else:
                        print("DRY RUN - would delete these files")
        except Exception as e:
            print(f"Error listing blobs for {prefix}: {str(e)}")

    return deleted_count

def main():
    """Main function to remove files"""
    parser = argparse.ArgumentParser(description="Clean up files from Firestore and GCP Storage")
    parser.add_argument("--ticker", help="Clean up files for a specific ticker")
    parser.add_argument("--execute", action="store_true", help="Actually remove files (default is dry run)")
    parser.add_argument("--all", action="store_true", help="Clean up all files (not just incorrectly named ones)")
    parser.add_argument("--only-accession", action="store_true", help="Only clean accession-based directories (faster)")

    args = parser.parse_args()

    dry_run = not args.execute
    mode = "EXECUTE" if args.execute else "DRY RUN"

    # Track deletions
    total_deleted = 0

    # Check if we should only do accession directory cleanup
    if args.only_accession and args.ticker:
        print(f"\n===== CLEANING ONLY ACCESSION DIRECTORIES - {mode} =====")
        total_deleted += clean_accession_directories(args.ticker, dry_run)
    else:
        # Do full cleanup
        is_full_cleanup = args.all or args.ticker

        if is_full_cleanup:
            # Use the new ticker-based cleanup
            print(f"\n===== REMOVING FILES FROM GCP AND FIRESTORE - {mode} =====")

            ticker_str = f" for {args.ticker}" if args.ticker else " for ALL tickers"
            print(f"Searching for files{ticker_str}...")

            files_to_remove = list_ticker_files(args.ticker, dry_run)
            remove_ticker_files(files_to_remove, dry_run)

            # Additionally clean up accession directories since standard cleanup might miss some
            if args.ticker:
                print(f"\n===== ADDITIONAL CLEANUP OF ACCESSION DIRECTORIES - {mode} =====")
                total_deleted += clean_accession_directories(args.ticker, dry_run)
        else:
            # Error - need either a ticker or --all
            print("Error: You must specify either --ticker or --all")
            sys.exit(1)

    if dry_run:
        print("\nThis was a dry run. No changes were made.")
        print("To actually remove files, run with the --execute flag.")
    else:
        print(f"\nCleanup complete! {total_deleted} additional files removed.")

if __name__ == "__main__":
    main()
