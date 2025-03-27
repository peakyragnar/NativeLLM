#!/usr/bin/env python3
"""
Check Firestore documents in detail.

This script shows all fields in a specific Firestore document.
"""

import os
import json
import logging
from google.cloud import firestore
from tabulate import tabulate

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

def check_specific_document(ticker, filing_type, fiscal_year, fiscal_period=None):
    """Check a specific Firestore document"""
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Determine document ID
        if fiscal_period and filing_type == "10-Q":
            document_id = f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}"
        else:
            document_id = f"{ticker}_{filing_type}_{fiscal_year}"
        
        print(f"\nLooking for document: {document_id}")
        
        # Get the document
        doc_ref = db.collection('filings').document(document_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            print(f"Document not found: {document_id}")
            return
        
        # Get document data
        data = doc.to_dict()
        
        # Print basic info
        print(f"\nDocument Details for {document_id}:")
        print("=" * 50)
        
        # Format data for display
        table_data = []
        
        for key, value in data.items():
            # Format complex values
            if isinstance(value, dict):
                formatted_value = json.dumps(value, indent=2)
            elif isinstance(value, list):
                formatted_value = json.dumps(value, indent=2)
            else:
                formatted_value = str(value)
            
            # Add to table
            table_data.append([key, formatted_value])
        
        # Sort by key
        table_data.sort(key=lambda x: x[0])
        
        # Print formatted table
        print(tabulate(table_data, headers=["Field", "Value"], tablefmt="grid"))
        
        return data
        
    except Exception as e:
        logging.error(f"Error checking Firestore: {str(e)}")
        return None

def main():
    """Main function"""
    if not setup_gcp_credentials():
        return 1
    
    # Get user input
    ticker = input("Enter ticker symbol (e.g., MSFT): ").strip().upper()
    filing_type = input("Enter filing type (10-K or 10-Q): ").strip()
    fiscal_year = input("Enter fiscal year: ").strip()
    
    fiscal_period = None
    if filing_type == "10-Q":
        fiscal_period = input("Enter fiscal period (Q1, Q2, Q3): ").strip()
    
    # Check the specific document
    check_specific_document(ticker, filing_type, fiscal_year, fiscal_period)
    
    return 0

if __name__ == "__main__":
    main()