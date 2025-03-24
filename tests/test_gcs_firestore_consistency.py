#!/usr/bin/env python3
"""
Test script to verify consistency between Google Cloud Storage paths and Firestore documents.
This ensures that each filing's GCS path is correctly constructed based on its fiscal period
and that the path in the Firestore document correctly points to the existing GCS file.
"""

import os
import sys
import argparse
import datetime
import logging
from google.cloud import storage, firestore
from tabulate import tabulate
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gcs_firestore_test.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def list_company_filings(ticker):
    """List all filings for a specific company in Firestore"""
    db = firestore.Client(database='nativellm')
    
    # Query all filings for the company
    filings_query = db.collection('filings').where('company_ticker', '==', ticker).get()
    
    all_filings = []
    for filing in filings_query:
        filing_data = filing.to_dict()
        
        # Convert fiscal_year to string for consistent sorting
        if 'fiscal_year' in filing_data and filing_data['fiscal_year'] is not None:
            filing_data['fiscal_year'] = str(filing_data['fiscal_year'])
            
        all_filings.append(filing_data)
    
    # Sort by fiscal year and period
    all_filings.sort(key=lambda x: (x.get('fiscal_year', ''), x.get('fiscal_period', '')))
    
    return all_filings

def check_gcs_path_exists(bucket_name, path):
    """Check if a file exists in Google Cloud Storage"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(path)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error checking GCS path {path}: {str(e)}")
        return False

def validate_path_format(path, ticker, filing_type, fiscal_year, fiscal_period):
    """Validate that the GCS path follows the expected format"""
    # Expected format: companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/{format}.txt
    expected_path_pattern = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period_to_folder(fiscal_period)}/"
    
    if expected_path_pattern not in path:
        return False, f"Expected path pattern '{expected_path_pattern}' not found in '{path}'"
    
    return True, "Path format is valid"

def fiscal_period_to_folder(period):
    """Convert fiscal period to folder name"""
    if period in ["FY", "annual"]:
        return "annual"
    return period

def test_gcs_firestore_consistency(ticker, bucket_name="native-llm-filings", fix=False):
    """Test consistency between GCS paths and Firestore documents"""
    # Get all filings for the company
    filings = list_company_filings(ticker)
    
    if not filings:
        logging.warning(f"No filings found for ticker {ticker}")
        return []
    
    logging.info(f"Found {len(filings)} filings for {ticker}")
    
    # Check each filing for consistency
    results = []
    
    for filing in filings:
        fiscal_year = filing.get('fiscal_year')
        fiscal_period = filing.get('fiscal_period')
        filing_type = filing.get('filing_type')
        filing_id = filing.get('filing_id')
        
        # Check text file path
        text_path = filing.get('text_file_path')
        text_exists = check_gcs_path_exists(bucket_name, text_path) if text_path else False
        text_valid_format, text_format_message = validate_path_format(
            text_path, ticker, filing_type, fiscal_year, fiscal_period
        ) if text_path else (False, "No text path")
        
        # Check LLM file path
        llm_path = filing.get('llm_file_path')
        llm_exists = check_gcs_path_exists(bucket_name, llm_path) if llm_path else False
        llm_valid_format, llm_format_message = validate_path_format(
            llm_path, ticker, filing_type, fiscal_year, fiscal_period
        ) if llm_path else (False, "No LLM path")
        
        # Create result record
        result = {
            'filing_id': filing_id,
            'fiscal_year': fiscal_year,
            'fiscal_period': fiscal_period,
            'filing_type': filing_type,
            'text_path': text_path,
            'text_exists': text_exists,
            'text_valid_format': text_valid_format,
            'text_format_message': text_format_message if not text_valid_format else "",
            'llm_path': llm_path,
            'llm_exists': llm_exists,
            'llm_valid_format': llm_valid_format,
            'llm_format_message': llm_format_message if not llm_valid_format else "",
            'status': 'valid' if (text_exists and text_valid_format and llm_exists and llm_valid_format) else 'invalid'
        }
        
        results.append(result)
        
        # Log issues
        if result['status'] == 'invalid':
            logging.warning(f"Inconsistency detected for {filing_id}:")
            if not text_exists and text_path:
                logging.warning(f"  Text file does not exist in GCS: {text_path}")
            if not text_valid_format and text_path:
                logging.warning(f"  Text path format is invalid: {text_format_message}")
            if not llm_exists and llm_path:
                logging.warning(f"  LLM file does not exist in GCS: {llm_path}")
            if not llm_valid_format and llm_path:
                logging.warning(f"  LLM path format is invalid: {llm_format_message}")
                
        # Fix issues if requested
        if fix and result['status'] == 'invalid':
            fix_path_issues(filing, bucket_name)
    
    return results

def fix_path_issues(filing, bucket_name):
    """Fix path issues in Firestore and GCS"""
    ticker = filing.get('company_ticker')
    filing_type = filing.get('filing_type')
    fiscal_year = filing.get('fiscal_year')
    fiscal_period = filing.get('fiscal_period')
    filing_id = filing.get('filing_id')
    
    # Determine correct paths
    folder_period = fiscal_period_to_folder(fiscal_period)
    correct_text_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{folder_period}/text.txt"
    correct_llm_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{folder_period}/llm.txt"
    
    text_path = filing.get('text_file_path')
    llm_path = filing.get('llm_file_path')
    
    # Check existing files
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    changes_made = False
    
    # Handle text file
    if text_path and text_path != correct_text_path:
        text_blob = bucket.blob(text_path)
        if text_blob.exists():
            # Copy file to correct path
            correct_text_blob = bucket.blob(correct_text_path)
            if not correct_text_blob.exists():
                logging.info(f"Copying text file from {text_path} to {correct_text_path}")
                bucket.copy_blob(text_blob, bucket, correct_text_path)
                changes_made = True
            
            # Update Firestore
            db = firestore.Client(database='nativellm')
            doc_ref = db.collection('filings').document(filing_id)
            doc_ref.update({'text_file_path': correct_text_path})
            logging.info(f"Updated text_file_path in Firestore for {filing_id}")
            changes_made = True
    
    # Handle LLM file
    if llm_path and llm_path != correct_llm_path:
        llm_blob = bucket.blob(llm_path)
        if llm_blob.exists():
            # Copy file to correct path
            correct_llm_blob = bucket.blob(correct_llm_path)
            if not correct_llm_blob.exists():
                logging.info(f"Copying LLM file from {llm_path} to {correct_llm_path}")
                bucket.copy_blob(llm_blob, bucket, correct_llm_path)
                changes_made = True
            
            # Update Firestore
            db = firestore.Client(database='nativellm')
            doc_ref = db.collection('filings').document(filing_id)
            doc_ref.update({'llm_file_path': correct_llm_path})
            logging.info(f"Updated llm_file_path in Firestore for {filing_id}")
            changes_made = True
    
    if changes_made:
        logging.info(f"Fixed path issues for {filing_id}")
    else:
        logging.info(f"No changes needed for {filing_id}")
    
    return changes_made

def print_results_table(results):
    """Print results in a formatted table"""
    table_data = []
    headers = ["Filing ID", "Type", "Year", "Period", "Text Exists", "Text Valid", "LLM Exists", "LLM Valid", "Status"]
    
    for result in results:
        table_data.append([
            result['filing_id'],
            result['filing_type'],
            result['fiscal_year'],
            result['fiscal_period'],
            "✓" if result['text_exists'] else "✗",
            "✓" if result['text_valid_format'] else "✗",
            "✓" if result['llm_exists'] else "✗",
            "✓" if result['llm_valid_format'] else "✗",
            "VALID" if result['status'] == 'valid' else "INVALID"
        ])
    
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    # Print summary
    valid_count = sum(1 for r in results if r['status'] == 'valid')
    print(f"\nSummary: {valid_count} of {len(results)} filings are valid ({valid_count/len(results)*100:.1f}%)")

def main():
    parser = argparse.ArgumentParser(description="Test GCS and Firestore consistency for company filings")
    parser.add_argument("--ticker", required=True, help="Company ticker to check")
    parser.add_argument("--bucket", default="native-llm-filings", help="GCS bucket name (default: native-llm-filings)")
    parser.add_argument("--fix", action="store_true", help="Fix path issues (copy files to correct paths and update Firestore)")
    
    args = parser.parse_args()
    
    logging.info(f"Testing GCS and Firestore consistency for {args.ticker}")
    
    # Run the test
    results = test_gcs_firestore_consistency(args.ticker, args.bucket, args.fix)
    
    # Print results
    print_results_table(results)

if __name__ == "__main__":
    main()