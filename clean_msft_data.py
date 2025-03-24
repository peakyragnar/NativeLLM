#!/usr/bin/env python3
"""
Clean Microsoft Data

This script deletes all Microsoft entries from Firestore and GCP storage,
providing a clean slate for re-downloading with the correct fiscal periods.
"""

import os
import logging
from google.cloud import storage, firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clean_msft_data.log'),
        logging.StreamHandler()
    ]
)

# Set the path to service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def delete_msft_from_firestore():
    """Delete all Microsoft entries from Firestore"""
    print("Deleting all Microsoft entries from Firestore...")
    
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Query for all Microsoft entries
        query = db.collection('filings').where('company_ticker', '==', 'MSFT')
        docs = query.stream()
        
        # Track entries
        entries = []
        for doc in docs:
            entries.append({
                'filing_id': doc.id,
                'fiscal_year': doc.to_dict().get('fiscal_year'),
                'fiscal_period': doc.to_dict().get('fiscal_period')
            })
            
            # Delete entry
            db.collection('filings').document(doc.id).delete()
        
        print(f"Deleted {len(entries)} Microsoft entries from Firestore")
        
        # Log details of deleted entries
        by_period = {}
        for entry in entries:
            period = entry.get('fiscal_period')
            if period not in by_period:
                by_period[period] = []
            by_period[period].append(entry)
        
        for period, period_entries in by_period.items():
            print(f"  - {period}: {len(period_entries)} entries")
        
        return entries
    
    except Exception as e:
        print(f"Error deleting from Firestore: {str(e)}")
        return []

def delete_msft_from_gcp():
    """Delete all Microsoft files from GCP storage"""
    print("\nDeleting all Microsoft files from GCP storage...")
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket("native-llm-filings")
        
        # List all Microsoft files
        prefix = "companies/MSFT/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        # Track files by type
        files_by_type = {}
        
        # Delete each file
        for blob in blobs:
            # Categorize file by type
            path_parts = blob.name.split('/')
            if len(path_parts) >= 5:
                filing_type = path_parts[3]
                period = path_parts[5]
                
                key = f"{filing_type}/{period}"
                if key not in files_by_type:
                    files_by_type[key] = []
                files_by_type[key].append(blob.name)
            
            # Delete file
            blob.delete()
        
        total_files = sum(len(files) for files in files_by_type.values())
        print(f"Deleted {total_files} Microsoft files from GCP storage")
        
        # Log details of deleted files
        for key, files in files_by_type.items():
            print(f"  - {key}: {len(files)} files")
        
        return total_files
    
    except Exception as e:
        print(f"Error deleting from GCP: {str(e)}")
        return 0

def clean_msft_data():
    """Clean all Microsoft data from Firestore and GCP"""
    print("Starting Microsoft data cleanup...")
    
    # Delete from Firestore
    entries = delete_msft_from_firestore()
    
    # Delete from GCP
    files = delete_msft_from_gcp()
    
    print("\nMicrosoft data cleanup complete!")
    print(f"Deleted {len(entries)} entries from Firestore and {files} files from GCP")
    print("\nYou can now run calendar_download.py to re-download all Microsoft filings with correct fiscal periods:")
    print("python calendar_download.py 2022 2024 --tickers MSFT")

if __name__ == "__main__":
    clean_msft_data()