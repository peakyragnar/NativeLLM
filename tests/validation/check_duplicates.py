#!/usr/bin/env python3
"""
Script to check for duplicate filings in Firestore based on period end dates.
"""

import os
import sys
import logging
from google.cloud import firestore
from datetime import datetime
from tabulate import tabulate

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('check_duplicates.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def get_company_filings(ticker):
    """Get all filings for a company from Firestore"""
    db = firestore.Client(database='nativellm')
    filings_query = db.collection('filings').where('company_ticker', '==', ticker).get()
    
    all_filings = []
    for filing in filings_query:
        filing_data = filing.to_dict()
        all_filings.append(filing_data)
    
    return all_filings

def find_duplicates(ticker):
    """Find potential duplicate filings based on period end dates"""
    filings = get_company_filings(ticker)
    
    # Group by filing type and period end date
    filing_groups = {}
    for filing in filings:
        filing_type = filing.get('filing_type')
        period_end = filing.get('period_end_date')
        
        if not period_end:
            continue
            
        key = f"{filing_type}_{period_end}"
        if key not in filing_groups:
            filing_groups[key] = []
            
        filing_groups[key].append(filing)
    
    # Find groups with more than one filing
    duplicates = {}
    for key, group in filing_groups.items():
        if len(group) > 1:
            duplicates[key] = group
    
    return duplicates

def display_duplicates(duplicates):
    """Display information about duplicate filings"""
    if not duplicates:
        print("No duplicate filings found.")
        return
        
    print(f"Found {len(duplicates)} potential duplicate filing groups:")
    
    for i, (key, filings) in enumerate(duplicates.items(), 1):
        print(f"\nDuplicate Group {i}: {key}")
        
        table_data = []
        for filing in filings:
            table_data.append([
                filing.get('filing_id', 'N/A'),
                filing.get('fiscal_year', 'N/A'),
                filing.get('fiscal_period', 'N/A'),
                filing.get('period_end_date', 'N/A'),
                filing.get('filing_date', 'N/A'),
                filing.get('text_file_path', 'N/A').split('/')[-2] if filing.get('text_file_path') else 'N/A'
            ])
        
        headers = ["Filing ID", "Fiscal Year", "Fiscal Period", "Period End", "Filing Date", "GCS Folder"]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

def merge_duplicates(ticker, execute=False):
    """Merge duplicate filings by keeping the most correct one"""
    duplicates = find_duplicates(ticker)
    
    if not duplicates:
        print("No duplicate filings found to merge.")
        return
    
    db = firestore.Client(database='nativellm')
    
    for key, filings in duplicates.items():
        print(f"\nAnalyzing duplicate group: {key}")
        
        # Check if one of the filings matches the expected fiscal period
        from src.edgar.company_fiscal import fiscal_registry
        
        # Take the first filing's period end date as reference
        period_end_date = filings[0].get('period_end_date')
        filing_type = filings[0].get('filing_type')
        
        # Get expected fiscal info
        expected = fiscal_registry.determine_fiscal_period(ticker, period_end_date, filing_type)
        expected_year = expected.get('fiscal_year')
        expected_period = expected.get('fiscal_period')
        
        print(f"Expected fiscal info: {expected_year}-{expected_period}")
        
        # Find the filing that matches the expected fiscal info
        correct_filing = None
        for filing in filings:
            if (filing.get('fiscal_year') == expected_year and 
                filing.get('fiscal_period') == expected_period):
                correct_filing = filing
                break
        
        if not correct_filing:
            print("No filing matches the expected fiscal info. Skipping.")
            continue
        
        # Identify filings to be deleted
        filings_to_delete = [f for f in filings if f != correct_filing]
        
        print(f"Keeping: {correct_filing.get('filing_id')}")
        for filing in filings_to_delete:
            print(f"Removing: {filing.get('filing_id')}")
        
        if execute:
            # Delete incorrect filings
            for filing in filings_to_delete:
                filing_id = filing.get('filing_id')
                doc_ref = db.collection('filings').document(filing_id)
                doc_ref.delete()
                print(f"Deleted {filing_id}")
        else:
            print("Dry run - no changes made. Run with --execute to apply changes.")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Check for duplicate filings in Firestore")
    parser.add_argument("--ticker", required=True, help="Company ticker to check")
    parser.add_argument("--merge", action="store_true", help="Analyze and prepare for merging duplicates")
    parser.add_argument("--execute", action="store_true", help="Execute the merge (only with --merge)")
    
    args = parser.parse_args()
    
    # Find and display duplicates
    duplicates = find_duplicates(args.ticker)
    display_duplicates(duplicates)
    
    # Merge duplicates if requested
    if args.merge:
        merge_duplicates(args.ticker, args.execute)