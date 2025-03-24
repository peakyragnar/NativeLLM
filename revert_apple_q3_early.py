#!/usr/bin/env python3
"""
Script to revert the incorrect change to Apple's Q2 2023 filing.
We incorrectly renamed AAPL-10-Q-2023-Q2 to AAPL-10-Q-2023-Q3_early,
but this was an error. This script reverts that change.
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
        logging.FileHandler('revert_apple_q3_early.log'),
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

def revert_apple_q3_early(execute=False):
    """Revert the incorrect change to Apple's Q2 2023 filing"""
    # Check if the Q3_early filing exists
    q3_early_filing = get_filing("AAPL-10-Q-2023-Q3_early")
    if not q3_early_filing:
        logging.error("AAPL-10-Q-2023-Q3_early filing not found!")
        return False
    
    # Check the period end date to confirm it's the right one
    period_end_date = q3_early_filing.get('period_end_date')
    if period_end_date != "2023-04-01":
        logging.error(f"Unexpected period end date: {period_end_date}, expected 2023-04-01")
        return False
    
    # Check if Q2 already exists
    q2_id = "AAPL-10-Q-2023-Q2"
    existing_q2 = get_filing(q2_id)
    if existing_q2:
        logging.error(f"Q2 ID {q2_id} already exists!")
        return False
    
    # If we're just checking, report findings
    if not execute:
        logging.info("DRY RUN - No changes will be made")
        logging.info(f"Would rename AAPL-10-Q-2023-Q3_early back to {q2_id}")
        logging.info(f"Period end date: {period_end_date}")
        return True
    
    # Create a copy of the filing data with updated ID and fiscal period
    db = firestore.Client(database='nativellm')
    
    # Create modified copy of the filing data
    new_filing_data = q3_early_filing.copy()
    new_filing_data['filing_id'] = q2_id
    new_filing_data['fiscal_period'] = "Q2"  # Correct fiscal period
    
    # Update GCS paths if needed
    if 'text_file_path' in new_filing_data:
        new_filing_data['text_file_path'] = new_filing_data['text_file_path'].replace('Q3_early', 'Q2')
    
    if 'llm_file_path' in new_filing_data:
        new_filing_data['llm_file_path'] = new_filing_data['llm_file_path'].replace('Q3_early', 'Q2')
    
    # Fix GCS paths if needed
    storage_client = storage.Client()
    bucket = storage_client.bucket("native-llm-filings")
    
    # Check and fix text file path
    if 'text_file_path' in new_filing_data:
        old_text_path = q3_early_filing.get('text_file_path')
        new_text_path = new_filing_data.get('text_file_path')
        
        old_blob = bucket.blob(old_text_path)
        new_blob = bucket.blob(new_text_path)
        
        if old_blob.exists() and not new_blob.exists():
            # Copy the blob
            logging.info(f"Copying {old_text_path} to {new_text_path}")
            bucket.copy_blob(old_blob, bucket, new_text_path)
    
    # Check and fix LLM file path
    if 'llm_file_path' in new_filing_data:
        old_llm_path = q3_early_filing.get('llm_file_path')
        new_llm_path = new_filing_data.get('llm_file_path')
        
        old_blob = bucket.blob(old_llm_path)
        new_blob = bucket.blob(new_llm_path)
        
        if old_blob.exists() and not new_blob.exists():
            # Copy the blob
            logging.info(f"Copying {old_llm_path} to {new_llm_path}")
            bucket.copy_blob(old_blob, bucket, new_llm_path)
    
    # Create new document
    logging.info(f"Creating new document {q2_id}")
    db.collection('filings').document(q2_id).set(new_filing_data)
    
    # Delete old document
    logging.info(f"Deleting old document AAPL-10-Q-2023-Q3_early")
    db.collection('filings').document("AAPL-10-Q-2023-Q3_early").delete()
    
    logging.info("Successfully reverted Apple Q3_early filing back to Q2")
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Revert incorrect change to Apple Q2 2023 filing")
    parser.add_argument("--execute", action="store_true", help="Execute the revert (default is dry run)")
    
    args = parser.parse_args()
    
    revert_apple_q3_early(args.execute)