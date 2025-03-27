#!/usr/bin/env python3
"""
Validate all Microsoft documents in Firestore.

This script runs the file integrity validator on all Microsoft documents.
"""

import os
import sys
import logging
import subprocess
from google.cloud import firestore
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def get_all_msft_documents():
    """Get all Microsoft documents from Firestore"""
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Query for MSFT documents
        docs = db.collection('filings').where('company_ticker', '==', 'MSFT').stream()
        
        # Collect document info
        msft_docs = []
        
        for doc in docs:
            data = doc.to_dict()
            
            # Extract needed fields
            msft_docs.append({
                'document_id': doc.id,
                'filing_type': data.get('filing_type'),
                'fiscal_year': data.get('fiscal_year'),
                'fiscal_period': data.get('fiscal_period')
            })
        
        return msft_docs
        
    except Exception as e:
        logging.error(f"Error getting Microsoft documents: {str(e)}")
        return []

def run_validation_for_doc(doc):
    """Run validation for a single document"""
    filing_type = doc['filing_type']
    fiscal_year = doc['fiscal_year']
    fiscal_period = doc['fiscal_period']
    
    cmd = [
        "python3", "verify_file_integrity.py",
        "--ticker", "MSFT",
        "--filing-type", filing_type,
        "--fiscal-year", fiscal_year
    ]
    
    # Add fiscal period for 10-Q
    if filing_type == "10-Q" and fiscal_period:
        cmd.extend(["--fiscal-period", fiscal_period])
    
    # Run the validation and capture output
    logging.info(f"Running validation for {doc['document_id']}")
    try:
        process = subprocess.run(
            cmd, 
            capture_output=True,
            text=True,
            check=False
        )
        
        output = process.stdout
        error = process.stderr
        
        # Extract status from output
        status = "UNKNOWN"
        if "Overall Status: PASS" in output:
            status = "PASS"
        elif "Overall Status: FAIL" in output:
            status = "FAIL"
        
        return {
            'document_id': doc['document_id'],
            'status': status,
            'output': output,
            'error': error,
            'exit_code': process.returncode
        }
        
    except Exception as e:
        logging.error(f"Error running validation: {str(e)}")
        return {
            'document_id': doc['document_id'],
            'status': "ERROR",
            'output': "",
            'error': str(e),
            'exit_code': -1
        }

def main():
    """Main function"""
    print("Validating all Microsoft documents...")
    
    # Get all MSFT documents
    msft_docs = get_all_msft_documents()
    
    if not msft_docs:
        print("No Microsoft documents found")
        return
    
    print(f"Found {len(msft_docs)} Microsoft documents")
    
    # Run validation for each document
    results = []
    
    for doc in msft_docs:
        result = run_validation_for_doc(doc)
        results.append(result)
    
    # Print summary table
    table_data = []
    
    for result in results:
        doc_id = result['document_id']
        status = result['status']
        exit_code = result['exit_code']
        
        table_data.append([doc_id, status, exit_code])
    
    # Sort by document ID
    table_data.sort(key=lambda x: x[0])
    
    print("\n===== VALIDATION SUMMARY =====")
    print(tabulate(table_data, headers=["Document ID", "Status", "Exit Code"], tablefmt="grid"))
    
    # Print overall stats
    pass_count = sum(1 for r in results if r['status'] == "PASS")
    fail_count = sum(1 for r in results if r['status'] == "FAIL")
    error_count = sum(1 for r in results if r['status'] == "ERROR" or r['status'] == "UNKNOWN")
    
    print(f"\nResults: {pass_count} PASS, {fail_count} FAIL, {error_count} ERROR")
    print(f"Success Rate: {pass_count/len(results)*100:.1f}%")
    
    # Print details of failed validations
    if fail_count > 0 or error_count > 0:
        print("\n===== FAILED VALIDATIONS =====")
        for result in results:
            if result['status'] in ["FAIL", "ERROR", "UNKNOWN"]:
                print(f"\nDocument: {result['document_id']}")
                print(f"Status: {result['status']}")
                print(f"Error: {result['error']}")

if __name__ == "__main__":
    main()