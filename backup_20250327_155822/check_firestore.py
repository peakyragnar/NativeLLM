#!/usr/bin/env python3
"""
Check Firestore documents for Microsoft.

This script lists all Firestore documents for Microsoft.
"""

import os
import logging
from google.cloud import firestore

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

def check_firestore_documents():
    """Check Firestore documents for Microsoft"""
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Query for MSFT documents
        docs = db.collection('filings').where('company_ticker', '==', 'MSFT').stream()
        
        print("\nMicrosoft Firestore documents:")
        print("------------------------------")
        count = 0
        
        for doc in docs:
            count += 1
            data = doc.to_dict()
            
            print(f"\nDocument ID: {doc.id}")
            print(f"Filing Type: {data.get('filing_type')}")
            print(f"Fiscal Year: {data.get('fiscal_year')}")
            print(f"Fiscal Period: {data.get('fiscal_period')}")
            
            # Print file paths
            text_path = data.get('text_file_path', 'None')
            llm_path = data.get('llm_file_path', 'None')
            
            print(f"Text file: {text_path}")
            print(f"LLM file: {llm_path}")
        
        print(f"\nTotal documents: {count}")
        
        return count
    except Exception as e:
        logging.error(f"Error checking Firestore: {str(e)}")
        return 0

def main():
    """Main function"""
    if not setup_gcp_credentials():
        return 1
    
    check_firestore_documents()
    return 0

if __name__ == "__main__":
    main()