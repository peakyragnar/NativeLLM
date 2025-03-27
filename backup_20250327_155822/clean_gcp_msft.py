#!/usr/bin/env python3
"""
Clean up and organize MSFT files in GCP bucket.

This script will:
1. List all MSFT files in the GCS bucket
2. Delete files that don't follow the right naming structure
3. Ensure files are in the proper fiscal year folders
"""

import os
import sys
import logging
from google.cloud import storage, firestore
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_gcp_credentials():
    """Set up GCP credentials"""
    credentials_path = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        logging.info(f"Using GCP credentials from: {credentials_path}")
        return True
    else:
        logging.error(f"Credentials file not found at {credentials_path}")
        return False

def list_msft_files(bucket_name):
    """List all MSFT files in the bucket"""
    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # List all blobs with MSFT prefix
        prefix = "companies/MSFT"
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        logging.info(f"Found {len(blobs)} files under {prefix}")
        
        # Sort blobs by name
        blobs.sort(key=lambda x: x.name)
        
        # Print blobs with numbering
        for i, blob in enumerate(blobs, 1):
            print(f"{i}. {blob.name} ({blob.size} bytes)")
        
        return blobs
    except Exception as e:
        logging.error(f"Error listing files: {str(e)}")
        return []

def delete_misnamed_files(bucket_name):
    """Delete files that don't follow the right naming structure"""
    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # List all blobs with MSFT prefix
        prefix = "companies/MSFT"
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Keep track of files to delete
        to_delete = []
        
        # Check each file
        for blob in blobs:
            # Files should be in companies/MSFT/10-K/2024/text.txt or 
            # companies/MSFT/10-Q/2024/Q1/text.txt format
            path_parts = blob.name.split('/')
            
            # Quick check for valid path
            if len(path_parts) < 5:
                continue
            
            # Get components
            company = path_parts[1]
            filing_type = path_parts[2]
            fiscal_year = path_parts[3]
            
            # Check if the components are valid 
            if company != "MSFT" or filing_type not in ["10-K", "10-Q"]:
                continue
                
            # If it has a date format (YYYYMMDD) instead of fiscal year, mark for deletion
            if len(fiscal_year) == 8 and fiscal_year.isdigit():
                to_delete.append(blob.name)
                
        # Confirm deletion
        if to_delete:
            print(f"\nFound {len(to_delete)} files to delete:")
            for i, name in enumerate(to_delete, 1):
                print(f"{i}. {name}")
                
            confirm = input("\nDelete these files? (y/n): ")
            if confirm.lower() == 'y':
                for name in to_delete:
                    blob = bucket.blob(name)
                    blob.delete()
                    logging.info(f"Deleted {name}")
                print(f"Deleted {len(to_delete)} files")
            else:
                print("Deletion cancelled")
        else:
            print("No files to delete")
        
        return len(to_delete)
    except Exception as e:
        logging.error(f"Error deleting files: {str(e)}")
        return 0

def ensure_proper_structure(bucket_name):
    """Ensure files are in proper fiscal year folders"""
    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Define the proper structure
        proper_text_path = "companies/MSFT/10-K/2024/text.txt"
        proper_llm_path = "companies/MSFT/10-K/2024/llm.txt"
        
        # Check if proper files already exist
        text_exists = bucket.blob(proper_text_path).exists()
        llm_exists = bucket.blob(proper_llm_path).exists()
        
        print(f"Proper text file exists: {text_exists}")
        print(f"Proper LLM file exists: {llm_exists}")
        
        # If both exist, nothing to do
        if text_exists and llm_exists:
            print("Both files already exist in the proper structure")
            return True
            
        # Find most recent text and LLM files
        text_blobs = list(bucket.list_blobs(prefix="companies/MSFT", delimiter="/"))
        text_blob = None
        llm_blob = None
        
        for blob in text_blobs:
            if blob.name.endswith("text.txt"):
                if text_blob is None or blob.updated > text_blob.updated:
                    text_blob = blob
            elif blob.name.endswith("llm.txt"):
                if llm_blob is None or blob.updated > llm_blob.updated:
                    llm_blob = blob
        
        # Move files to proper structure if needed
        if not text_exists and text_blob:
            print(f"Moving {text_blob.name} to {proper_text_path}")
            bucket.copy_blob(text_blob, bucket, proper_text_path)
            logging.info(f"Copied {text_blob.name} to {proper_text_path}")
            text_blob.delete()
            logging.info(f"Deleted original {text_blob.name}")
            
        if not llm_exists and llm_blob:
            print(f"Moving {llm_blob.name} to {proper_llm_path}")
            bucket.copy_blob(llm_blob, bucket, proper_llm_path)
            logging.info(f"Copied {llm_blob.name} to {proper_llm_path}")
            llm_blob.delete()
            logging.info(f"Deleted original {llm_blob.name}")
            
        return True
    except Exception as e:
        logging.error(f"Error ensuring proper structure: {str(e)}")
        return False
        
def update_firestore_metadata(bucket_name):
    """Update Firestore metadata to reflect the proper file structure"""
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Define the proper file paths
        proper_text_path = "companies/MSFT/10-K/2024/text.txt"
        proper_llm_path = "companies/MSFT/10-K/2024/llm.txt"
        
        # Check GCS for file sizes
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        text_blob = bucket.blob(proper_text_path)
        llm_blob = bucket.blob(proper_llm_path)
        
        # Only proceed if files exist
        if not (text_blob.exists() and llm_blob.exists()):
            logging.warning("Files don't exist in GCS - can't update Firestore")
            return False
        
        # Get file sizes
        text_size = text_blob.size
        llm_size = llm_blob.size
        
        # Create a unique filing ID
        filing_id = f"MSFT-10-K-2024-annual"
        
        # Check if filing exists
        filing_ref = db.collection('filings').document(filing_id)
        filing_doc = filing_ref.get()
        
        if filing_doc.exists:
            # Update existing document
            update_data = {
                'text_file_path': proper_text_path,
                'text_file_size': text_size,
                'llm_file_path': proper_llm_path,
                'llm_file_size': llm_size,
                'has_llm_format': True,
                'last_updated': firestore.SERVER_TIMESTAMP
            }
            filing_ref.update(update_data)
            logging.info(f"Updated Firestore document {filing_id}")
        else:
            # Create new filing document
            filing_data = {
                'filing_id': filing_id,
                'company_ticker': 'MSFT',
                'company_name': 'Microsoft Corporation',
                'filing_type': '10-K',
                'fiscal_year': '2024',
                'fiscal_period': 'annual',
                'text_file_path': proper_text_path,
                'text_file_size': text_size,
                'llm_file_path': proper_llm_path,
                'llm_file_size': llm_size,
                'has_llm_format': True,
                'storage_class': 'STANDARD',
                'last_updated': firestore.SERVER_TIMESTAMP,
                'access_count': 0
            }
            filing_ref.set(filing_data)
            logging.info(f"Created new Firestore document {filing_id}")
            
        # Delete any other MSFT 10-K documents in Firestore
        msft_docs = db.collection('filings').where('company_ticker', '==', 'MSFT').where('filing_type', '==', '10-K').stream()
        
        for doc in msft_docs:
            if doc.id != filing_id:
                logging.info(f"Deleting duplicate Firestore document {doc.id}")
                doc.reference.delete()
                
        return True
    except Exception as e:
        logging.error(f"Error updating Firestore: {str(e)}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Clean up MSFT files in GCP")
    parser.add_argument("--bucket", default="native-llm-filings", help="GCS bucket name")
    parser.add_argument("--list", action="store_true", help="List all MSFT files")
    parser.add_argument("--delete", action="store_true", help="Delete misnamed files")
    parser.add_argument("--organize", action="store_true", help="Ensure proper file structure")
    parser.add_argument("--update-metadata", action="store_true", help="Update Firestore metadata")
    
    args = parser.parse_args()
    
    # Set up GCP credentials
    if not setup_gcp_credentials():
        sys.exit(1)
    
    # Default to list if no action specified
    if not (args.list or args.delete or args.organize or args.update_metadata):
        args.list = True
    
    # Execute requested actions
    if args.list:
        list_msft_files(args.bucket)
    
    if args.delete:
        delete_misnamed_files(args.bucket)
    
    if args.organize:
        ensure_proper_structure(args.bucket)
    
    if args.update_metadata:
        update_firestore_metadata(args.bucket)

if __name__ == "__main__":
    main()