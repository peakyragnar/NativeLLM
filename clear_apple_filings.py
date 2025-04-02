#\!/usr/bin/env python3
"""
Clear Apple's 2024 and 2025 filings from GCP Storage and Firestore.
"""

import os
import logging
import argparse
from google.cloud import storage, firestore

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

def clear_firestore_filings(ticker, years):
    """Clear filings from Firestore"""
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Get reference to filings collection
        filings_ref = db.collection('filings')
        
        # Query for documents matching ticker and years
        query = filings_ref.where('company_ticker', '==', ticker)
        docs = query.stream()
        
        deleted_count = 0
        for doc in docs:
            data = doc.to_dict()
            fiscal_year = data.get('fiscal_year')
            
            # Check if fiscal year is in the years list
            if fiscal_year in years:
                logging.info(f"Deleting Firestore document: {doc.id}")
                doc.reference.delete()
                deleted_count += 1
        
        logging.info(f"Deleted {deleted_count} Firestore documents for {ticker} in years {years}")
        return deleted_count
    
    except Exception as e:
        logging.error(f"Error clearing Firestore: {e}")
        return 0

def clear_gcs_filings(bucket_name, ticker, years):
    """Clear filings from GCS"""
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Pattern for company filings
        prefix = f"companies/{ticker}/"
        
        # List all blobs with the prefix
        blobs = bucket.list_blobs(prefix=prefix)
        
        deleted_count = 0
        for blob in blobs:
            # Check if year is in the path
            for year in years:
                year_str = f"/{year}/"
                if year_str in blob.name:
                    logging.info(f"Deleting GCS blob: {blob.name}")
                    blob.delete()
                    deleted_count += 1
                    break
        
        logging.info(f"Deleted {deleted_count} GCS objects for {ticker} in years {years}")
        return deleted_count
    
    except Exception as e:
        logging.error(f"Error clearing GCS: {e}")
        return 0

def main():
    """Main function"""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Clear Apple's 2024 and 2025 filings from GCP")
    parser.add_argument("--bucket", default="native-llm-filings", help="GCS bucket name")
    parser.add_argument("--ticker", default="AAPL", help="Company ticker")
    parser.add_argument("--years", default="2024,2025", help="Years to delete (comma-separated)")
    parser.add_argument("--firestore-only", action="store_true", help="Only delete from Firestore")
    parser.add_argument("--gcs-only", action="store_true", help="Only delete from GCS")
    
    args = parser.parse_args()
    
    # Set up GCP credentials
    if not setup_gcp_credentials():
        return 1
    
    # Parse years
    years = [y.strip() for y in args.years.split(",")]
    logging.info(f"Preparing to delete {args.ticker} filings for years: {years}")
    
    # Clear Firestore
    if not args.gcs_only:
        firestore_count = clear_firestore_filings(args.ticker, years)
        logging.info(f"Deleted {firestore_count} documents from Firestore")
    
    # Clear GCS
    if not args.firestore_only:
        gcs_count = clear_gcs_filings(args.bucket, args.ticker, years)
        logging.info(f"Deleted {gcs_count} objects from GCS bucket {args.bucket}")
    
    logging.info("Cleanup complete")
    return 0

if __name__ == "__main__":
    main()
