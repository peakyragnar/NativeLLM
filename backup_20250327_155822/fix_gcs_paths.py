#!/usr/bin/env python3
"""
Utility to fix GCS file paths for filings
"""
import os
import sys
import argparse
import logging
from google.cloud import firestore, storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_gcs_paths.log'),
        logging.StreamHandler()
    ]
)

# Set the path to service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def fix_apple_q1_2023_gcs_paths(dry_run=True):
    """
    Fix GCS paths for Apple Q1 2023 filing that was previously labeled as Q4 2022
    
    Args:
        dry_run: If True, don't actually update GCS
    """
    db = firestore.Client(database='nativellm')
    storage_client = storage.Client()
    bucket_name = "native-llm-filings"
    bucket = storage_client.bucket(bucket_name)
    
    # The correct filing ID (already fixed in Firestore)
    filing_id = "AAPL-10-Q-2023-Q1"
    
    # Get the document
    doc_ref = db.collection('filings').document(filing_id)
    doc = doc_ref.get()
    
    if not doc.exists:
        print(f"Filing {filing_id} not found in Firestore")
        return
    
    # Get the data
    filing_data = doc.to_dict()
    
    # Print the current state
    print(f"Found filing: {filing_id}")
    print(f"  Period End Date: {filing_data.get('period_end_date')}")
    print(f"  Filing Date: {filing_data.get('filing_date')}")
    print(f"  Current: FY{filing_data.get('fiscal_year')} {filing_data.get('fiscal_period')}")
    
    # Get current GCS paths
    text_file_path = filing_data.get('text_file_path')
    llm_file_path = filing_data.get('llm_file_path')
    
    print(f"  Current text GCS path: {text_file_path}")
    print(f"  Current LLM GCS path: {llm_file_path}")
    
    # The paths should be fixed to reflect the correct fiscal period Q1 2023
    new_text_path = text_file_path.replace('/2022/Q4/', '/2023/Q1/') if text_file_path and '/2022/Q4/' in text_file_path else text_file_path
    new_llm_path = llm_file_path.replace('/2022/Q4/', '/2023/Q1/') if llm_file_path and '/2022/Q4/' in llm_file_path else llm_file_path
    
    print(f"  New text GCS path should be: {new_text_path}")
    print(f"  New LLM GCS path should be: {new_llm_path}")
    
    # Keep track of whether we need to update Firestore
    update_firestore = False
    
    # Check if files exist at current paths
    if text_file_path and text_file_path != new_text_path:
        text_blob = bucket.blob(text_file_path)
        if text_blob.exists():
            print(f"  Text file exists at current path: {text_file_path}")
            
            if dry_run:
                print(f"  DRY RUN: Would copy text file to new path: {new_text_path}")
                print(f"  DRY RUN: Would update Firestore text_file_path")
            else:
                # Copy to new path
                new_text_blob = bucket.blob(new_text_path)
                bucket.copy_blob(text_blob, bucket, new_text_path)
                print(f"  Copied text file from {text_file_path} to {new_text_path}")
                
                # Delete old file
                text_blob.delete()
                print(f"  Deleted old text file at {text_file_path}")
                
                # Update Firestore path
                filing_data['text_file_path'] = new_text_path
                update_firestore = True
        else:
            print(f"  Text file NOT found at current path: {text_file_path}")
    
    if llm_file_path and llm_file_path != new_llm_path:
        llm_blob = bucket.blob(llm_file_path)
        if llm_blob.exists():
            print(f"  LLM file exists at current path: {llm_file_path}")
            
            if dry_run:
                print(f"  DRY RUN: Would copy LLM file to new path: {new_llm_path}")
                print(f"  DRY RUN: Would update Firestore llm_file_path")
            else:
                # Copy to new path
                new_llm_blob = bucket.blob(new_llm_path)
                bucket.copy_blob(llm_blob, bucket, new_llm_path)
                print(f"  Copied LLM file from {llm_file_path} to {new_llm_path}")
                
                # Delete old file
                llm_blob.delete()
                print(f"  Deleted old LLM file at {llm_file_path}")
                
                # Update Firestore path
                filing_data['llm_file_path'] = new_llm_path
                update_firestore = True
        else:
            print(f"  LLM file NOT found at current path: {llm_file_path}")
    
    # Update Firestore document if needed
    if update_firestore and not dry_run:
        db.collection('filings').document(filing_id).set(filing_data)
        print(f"  Updated Firestore document with new GCS paths")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix GCS paths for mislabeled filings")
    parser.add_argument("--execute", action="store_true", help="Actually update GCS (default is dry run)")
    
    args = parser.parse_args()
    
    dry_run = not args.execute
    mode = "EXECUTE" if not dry_run else "DRY RUN"
    print(f"\n===== FIXING GCS PATHS - {mode} =====")
    
    fix_apple_q1_2023_gcs_paths(dry_run)
    
    if dry_run:
        print("\nThis was a dry run. No changes were made.")
        print("To actually update GCS, run with the --execute flag.")
    else:
        print("\nGCS paths have been updated.")