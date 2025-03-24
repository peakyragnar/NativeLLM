#!/usr/bin/env python3
"""
Script to fix Apple's Q2 2023 filing which should be classified as Q3 early.
Since we already have a Q3 filing for the later part of the quarter, we'll
rename this to Q3_early to maintain both filings while fixing the fiscal period.
"""

import os
import sys
import logging
from google.cloud import storage, firestore

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_apple_q2_2023.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def get_filing(filing_id):
    """Get a specific filing by ID"""
    db = firestore.Client(database='nativellm')
    doc_ref = db.collection('filings').document(filing_id)
    doc = doc_ref.get()
    
    if not doc.exists:
        return None
        
    return doc.to_dict()

def fix_apple_q2_2023(execute=False):
    """Fix the mislabeled Apple Q2 2023 filing by renaming it to Q3_early"""
    # Check if the problematic filing exists
    q2_filing = get_filing("AAPL-10-Q-2023-Q2")
    if not q2_filing:
        logging.error("AAPL-10-Q-2023-Q2 filing not found!")
        return False
    
    # Check the period end date to confirm it's the right one
    period_end_date = q2_filing.get('period_end_date')
    if period_end_date != "2023-04-01":
        logging.error(f"Unexpected period end date: {period_end_date}, expected 2023-04-01")
        return False
    
    # Check that the Q3 filing exists
    q3_filing = get_filing("AAPL-10-Q-2023-Q3")
    if not q3_filing:
        logging.error("AAPL-10-Q-2023-Q3 filing not found!")
        return False
    
    q3_period_end = q3_filing.get('period_end_date')
    if q3_period_end != "2023-07-01":
        logging.error(f"Unexpected Q3 period end date: {q3_period_end}, expected 2023-07-01")
        return False
    
    # Check if the custom ID already exists
    custom_id = "AAPL-10-Q-2023-Q3_early"
    existing_custom = get_filing(custom_id)
    if existing_custom:
        logging.error(f"Custom ID {custom_id} already exists!")
        return False
    
    # If we're just checking, report findings
    if not execute:
        logging.info("DRY RUN - No changes will be made")
        logging.info(f"Would rename AAPL-10-Q-2023-Q2 to {custom_id}")
        logging.info(f"Period end date: {period_end_date}")
        return True
    
    # Create a copy of the filing data with updated ID and fiscal period
    db = firestore.Client(database='nativellm')
    
    # Create modified copy of the filing data
    new_filing_data = q2_filing.copy()
    new_filing_data['filing_id'] = custom_id
    new_filing_data['fiscal_period'] = "Q3_early"
    
    # Update GCS paths if needed
    if 'text_file_path' in new_filing_data:
        new_filing_data['text_file_path'] = new_filing_data['text_file_path'].replace('Q2', 'Q3_early')
    
    if 'llm_file_path' in new_filing_data:
        new_filing_data['llm_file_path'] = new_filing_data['llm_file_path'].replace('Q2', 'Q3_early')
    
    # Fix GCS paths if needed
    storage_client = storage.Client()
    bucket = storage_client.bucket("native-llm-filings")
    
    # Check and fix text file path
    if 'text_file_path' in new_filing_data:
        old_text_path = q2_filing.get('text_file_path')
        new_text_path = new_filing_data.get('text_file_path')
        
        old_blob = bucket.blob(old_text_path)
        new_blob = bucket.blob(new_text_path)
        
        if old_blob.exists() and not new_blob.exists():
            # Copy the blob
            logging.info(f"Copying {old_text_path} to {new_text_path}")
            bucket.copy_blob(old_blob, bucket, new_text_path)
    
    # Check and fix LLM file path
    if 'llm_file_path' in new_filing_data:
        old_llm_path = q2_filing.get('llm_file_path')
        new_llm_path = new_filing_data.get('llm_file_path')
        
        old_blob = bucket.blob(old_llm_path)
        new_blob = bucket.blob(new_llm_path)
        
        if old_blob.exists() and not new_blob.exists():
            # Copy the blob
            logging.info(f"Copying {old_llm_path} to {new_llm_path}")
            bucket.copy_blob(old_blob, bucket, new_llm_path)
    
    # Create new document
    logging.info(f"Creating new document {custom_id}")
    db.collection('filings').document(custom_id).set(new_filing_data)
    
    # Delete old document
    logging.info(f"Deleting old document AAPL-10-Q-2023-Q2")
    db.collection('filings').document("AAPL-10-Q-2023-Q2").delete()
    
    logging.info("Successfully fixed Apple Q2 2023 filing by renaming to Q3_early")
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix Apple Q2 2023 filing")
    parser.add_argument("--execute", action="store_true", help="Execute the fix (default is dry run)")
    
    args = parser.parse_args()
    
    fix_apple_q2_2023(args.execute)