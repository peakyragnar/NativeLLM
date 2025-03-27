#!/usr/bin/env python3
"""
Utility to fix specific mislabeled filings in the database
"""
import os
import sys
import argparse
import logging
from google.cloud import firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_specific_filings.log'),
        logging.StreamHandler()
    ]
)

# Set the path to service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def fix_apple_q4_2022_filing(dry_run=True):
    """
    Fix specifically the Apple Q4 2022 filing that should be Q1 2023
    
    Args:
        dry_run: If True, don't actually update the database
    """
    from google.cloud import storage
    
    db = firestore.Client(database='nativellm')
    storage_client = storage.Client()
    bucket_name = "native-llm-filings"
    bucket = storage_client.bucket(bucket_name)
    
    # The specific filing ID to fix
    old_filing_id = "AAPL-10-Q-2022-Q4"
    new_filing_id = "AAPL-10-Q-2023-Q1"
    
    # Get the document
    doc_ref = db.collection('filings').document(old_filing_id)
    doc = doc_ref.get()
    
    if not doc.exists:
        print(f"Filing {old_filing_id} not found in database")
        return
    
    # Get the data
    filing_data = doc.to_dict()
    
    # Print the current state
    print(f"Found filing: {old_filing_id}")
    print(f"  Period End Date: {filing_data.get('period_end_date')}")
    print(f"  Filing Date: {filing_data.get('filing_date')}")
    print(f"  Current: FY{filing_data.get('fiscal_year')} {filing_data.get('fiscal_period')}")
    
    # Get GCS paths
    text_file_path = filing_data.get('text_file_path')
    llm_file_path = filing_data.get('llm_file_path')
    
    # Print GCS paths
    print(f"  Text GCS path: {text_file_path}")
    print(f"  LLM GCS path: {llm_file_path}")
    
    # Generate new GCS paths
    new_text_file_path = None
    new_llm_file_path = None
    
    if text_file_path:
        new_text_file_path = text_file_path.replace('/2022/Q4/', '/2023/Q1/')
        print(f"  New text GCS path: {new_text_file_path}")
    
    if llm_file_path:
        new_llm_file_path = llm_file_path.replace('/2022/Q4/', '/2023/Q1/')
        print(f"  New LLM GCS path: {new_llm_file_path}")
    
    # Check if we're in dry run mode
    if dry_run:
        print(f"\nDRY RUN: Would rename filing to {new_filing_id}")
        print(f"  New: FY2023 Q1")
        print(f"  Would also move GCS files to new paths")
        return
    
    # Check if target already exists
    if db.collection('filings').document(new_filing_id).get().exists:
        print(f"ERROR: Target filing ID {new_filing_id} already exists")
        return
    
    # Copy GCS files to new locations
    if text_file_path and new_text_file_path:
        try:
            source_blob = bucket.blob(text_file_path)
            dest_blob = bucket.blob(new_text_file_path)
            
            # Check if source exists
            if source_blob.exists():
                # Copy to new location
                bucket.copy_blob(source_blob, bucket, new_text_file_path)
                print(f"Copied text file from {text_file_path} to {new_text_file_path}")
                
                # Delete old file
                source_blob.delete()
                print(f"Deleted old text file at {text_file_path}")
                
                # Update path in filing data
                filing_data['text_file_path'] = new_text_file_path
            else:
                print(f"WARNING: Source text file {text_file_path} not found in GCS")
        except Exception as e:
            print(f"ERROR copying text file: {str(e)}")
    
    if llm_file_path and new_llm_file_path:
        try:
            source_blob = bucket.blob(llm_file_path)
            dest_blob = bucket.blob(new_llm_file_path)
            
            # Check if source exists
            if source_blob.exists():
                # Copy to new location
                bucket.copy_blob(source_blob, bucket, new_llm_file_path)
                print(f"Copied LLM file from {llm_file_path} to {new_llm_file_path}")
                
                # Delete old file
                source_blob.delete()
                print(f"Deleted old LLM file at {llm_file_path}")
                
                # Update path in filing data
                filing_data['llm_file_path'] = new_llm_file_path
            else:
                print(f"WARNING: Source LLM file {llm_file_path} not found in GCS")
        except Exception as e:
            print(f"ERROR copying LLM file: {str(e)}")
    
    # Update the data
    filing_data['fiscal_year'] = '2023'
    filing_data['fiscal_period'] = 'Q1'
    filing_data['filing_id'] = new_filing_id
    
    # Create new document with correct ID
    db.collection('filings').document(new_filing_id).set(filing_data)
    
    # Delete old document
    db.collection('filings').document(old_filing_id).delete()
    
    print(f"\nSUCCESS: Renamed filing from {old_filing_id} to {new_filing_id}")
    print(f"  Updated: FY{filing_data.get('fiscal_year')} {filing_data.get('fiscal_period')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix specific mislabeled filings")
    parser.add_argument("--execute", action="store_true", help="Actually update the database (default is dry run)")
    
    args = parser.parse_args()
    
    if args.execute:
        print("WARNING: This will update the database. Do you want to continue? (yes/no)")
        response = input().strip().lower()
        if response != 'yes':
            print("Aborted.")
            sys.exit(0)
    
    dry_run = not args.execute
    mode = "EXECUTE" if not dry_run else "DRY RUN"
    print(f"\n===== FIXING SPECIFIC FILINGS - {mode} =====")
    
    fix_apple_q4_2022_filing(dry_run)
    
    if dry_run:
        print("\nThis was a dry run. No changes were made.")
        print("To actually update the database, run with the --execute flag.")
    else:
        print("\nDatabase has been updated with corrected filings.")