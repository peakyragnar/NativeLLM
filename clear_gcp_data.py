#!/usr/bin/env python3
"""
Script to remove incorrectly named files from Firestore and Google Cloud Storage.
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('remove_incorrect_files.log'),
        logging.StreamHandler()
    ]
)

# Set credentials path
CREDENTIALS_PATH = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
GCS_BUCKET_NAME = "native-llm-filings"
FIRESTORE_DB = "nativellm"

# Set up GCP credentials
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    if os.path.exists(CREDENTIALS_PATH):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        logging.info(f"Set GCP credentials from {CREDENTIALS_PATH}")
    else:
        logging.error(f"Credentials file not found at {CREDENTIALS_PATH}")
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

# Legacy function for backward compatibility
def list_incorrect_files(ticker=None, dry_run=True):
    """
    Compatibility wrapper for list_ticker_files.
    
    Args:
        ticker: If provided, only check files for this ticker
        dry_run: If True, don't actually delete anything
    """
    return list_ticker_files(ticker, dry_run)

# Legacy function for backward compatibility
def remove_incorrect_files(files_to_remove, dry_run=True):
    """
    Compatibility wrapper for remove_ticker_files.
    
    Args:
        files_to_remove: List of file records to remove
        dry_run: If True, don't actually update anything
    """
    return remove_ticker_files(files_to_remove, dry_run)

def remove_ticker_files(files_to_remove, dry_run=True):
    """
    Remove files from GCS and Firestore for a specific ticker.
    
    Args:
        files_to_remove: List of file records to remove
        dry_run: If True, don't actually update anything
    """
    if not files_to_remove:
        print("No files found to remove.")
        return
    
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
        return
    
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
                blob = bucket.blob(file['text_path'])
                if blob.exists():
                    blob.delete()
                    print(f"Deleted GCS text file: {file['text_path']}")
                    successful_deletions += 1
            except Exception as e:
                print(f"Error deleting GCS text file {file['text_path']}: {str(e)}")
        
        # Delete LLM file from GCS if it exists
        if file.get('llm_path'):
            try:
                blob = bucket.blob(file['llm_path'])
                if blob.exists():
                    blob.delete()
                    print(f"Deleted GCS LLM file: {file['llm_path']}")
                    successful_deletions += 1
            except Exception as e:
                print(f"Error deleting GCS LLM file {file['llm_path']}: {str(e)}")
                
        # Delete standalone GCS file if it exists
        if file.get('gcs_path'):
            try:
                blob = bucket.blob(file['gcs_path'])
                if blob.exists():
                    blob.delete()
                    print(f"Deleted GCS file: {file['gcs_path']}")
                    successful_deletions += 1
            except Exception as e:
                print(f"Error deleting GCS file {file['gcs_path']}: {str(e)}")
    
    print(f"\nCompleted removal with {successful_deletions} successful deletions.")

def main():
    """Main function to remove files"""
    parser = argparse.ArgumentParser(description="Remove files from Firestore and GCS")
    parser.add_argument("--ticker", help="Remove files for a specific ticker")
    parser.add_argument("--execute", action="store_true", help="Actually remove files (default is dry run)")
    parser.add_argument("--all", action="store_true", help="Remove all files (not just incorrectly named ones)")
    
    args = parser.parse_args()
    
    dry_run = not args.execute
    mode = "EXECUTE" if args.execute else "DRY RUN"
    
    # Check if we should do the new complete cleanup
    is_full_cleanup = args.all or args.ticker
    
    if is_full_cleanup:
        # Use the new ticker-based cleanup
        print(f"\n===== REMOVING FILES FROM GCP - {mode} =====")
        
        ticker_str = f" for {args.ticker}" if args.ticker else " for ALL tickers"
        print(f"Searching for files{ticker_str}...")
        
        files_to_remove = list_ticker_files(args.ticker, dry_run)
        remove_ticker_files(files_to_remove, dry_run)
    else:
        # Use the legacy incorrect files cleanup
        print(f"\n===== REMOVING INCORRECT FILES - {mode} =====")
        print(f"Searching for incorrectly named files...")
        
        # Keep the old function to maintain backwards compatibility
        incorrect_files = []
        try:
            # Try to call the old function if it exists
            incorrect_files = list_incorrect_files(None, dry_run)
        except NameError:
            # Fall back to new function with empty ticker
            incorrect_files = list_ticker_files(None, dry_run)
        
        # Use appropriate removal function
        try:
            remove_incorrect_files(incorrect_files, dry_run)
        except NameError:
            remove_ticker_files(incorrect_files, dry_run)
    
    if dry_run:
        print("\nThis was a dry run. No changes were made.")
        print("To actually remove files, run with the --execute flag.")
    else:
        print("\nFiles have been removed.")

if __name__ == "__main__":
    main()
