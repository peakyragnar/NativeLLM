#!/usr/bin/env python3
"""
Script to fix missing files in GCS by uploading local files to the correct GCS path
as specified in Firestore.
"""

import os
import sys
import argparse
import logging
from google.cloud import storage, firestore

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_missing_gcs.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def get_filing_from_firestore(filing_id):
    """Get a filing document from Firestore by ID"""
    db = firestore.Client(database='nativellm')
    doc_ref = db.collection('filings').document(filing_id)
    doc = doc_ref.get()
    
    if not doc.exists:
        logging.error(f"Filing {filing_id} not found in Firestore")
        return None
    
    return doc.to_dict()

def upload_to_gcs(local_file_path, gcs_path, bucket_name="native-llm-filings"):
    """Upload a local file to Google Cloud Storage"""
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob and check if it exists
        blob = bucket.blob(gcs_path)
        if blob.exists():
            logging.info(f"File already exists in GCS: gs://{bucket_name}/{gcs_path}")
            return True
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Successfully uploaded {local_file_path} to gs://{bucket_name}/{gcs_path}")
        return True
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {str(e)}")
        return False

def fix_missing_file(filing_id, local_file, file_type="text", bucket_name="native-llm-filings"):
    """Fix a missing file in GCS by uploading a local file"""
    # Get filing info from Firestore
    filing = get_filing_from_firestore(filing_id)
    if not filing:
        return False
    
    # Determine GCS path from Firestore
    if file_type == "text":
        gcs_path = filing.get("text_file_path")
    elif file_type == "llm":
        gcs_path = filing.get("llm_file_path")
    else:
        logging.error(f"Invalid file type: {file_type}")
        return False
    
    if not gcs_path:
        logging.error(f"No {file_type}_file_path found in Firestore for {filing_id}")
        return False
    
    # Check if local file exists
    if not os.path.exists(local_file):
        logging.error(f"Local file not found: {local_file}")
        return False
    
    # Upload the file to GCS
    return upload_to_gcs(local_file, gcs_path, bucket_name)

def main():
    parser = argparse.ArgumentParser(description="Fix missing files in GCS by uploading local files")
    parser.add_argument("--filing-id", required=True, help="Filing ID in Firestore")
    parser.add_argument("--local-file", required=True, help="Path to local file to upload")
    parser.add_argument("--file-type", choices=["text", "llm"], default="text", help="File type (text or llm)")
    parser.add_argument("--bucket", default="native-llm-filings", help="GCS bucket name")
    
    args = parser.parse_args()
    
    # Fix the missing file
    success = fix_missing_file(args.filing_id, args.local_file, args.file_type, args.bucket)
    
    if success:
        print(f"Successfully fixed {args.file_type} file for {args.filing_id}")
    else:
        print(f"Failed to fix {args.file_type} file for {args.filing_id}")
        sys.exit(1)

if __name__ == "__main__":
    main()