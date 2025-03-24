#!/usr/bin/env python3
"""
Script to examine specific filings in Firestore.
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
        logging.FileHandler('examine_filing.log'),
        logging.StreamHandler()
    ]
)

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def get_filing(filing_id):
    """Get a specific filing by ID"""
    db = firestore.Client(database='nativellm')
    doc_ref = db.collection('filings').document(filing_id)
    doc = doc_ref.get()
    
    if not doc.exists:
        return None
        
    return doc.to_dict()

def examine_filing(filing_id):
    """Examine a specific filing and its metadata"""
    filing = get_filing(filing_id)
    
    if not filing:
        print(f"Filing {filing_id} not found in Firestore.")
        return
    
    print(f"Examining filing: {filing_id}")
    print("=" * 50)
    
    # Extract important fields
    important_fields = [
        "filing_id", "company_ticker", "company_name", "filing_type", 
        "fiscal_year", "fiscal_period", "period_end_date", "filing_date"
    ]
    
    for field in important_fields:
        print(f"{field}: {filing.get(field, 'N/A')}")
    
    # Show file paths
    print("\nFile Paths:")
    print(f"Text file: {filing.get('text_file_path', 'N/A')}")
    print(f"LLM file: {filing.get('llm_file_path', 'N/A')}")
    
    # Analyze fiscal period correctness
    ticker = filing.get('company_ticker')
    period_end_date = filing.get('period_end_date')
    filing_type = filing.get('filing_type')
    
    if ticker and period_end_date and filing_type:
        # Import fiscal registry
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
        from src.edgar.company_fiscal import fiscal_registry
        
        # Get expected fiscal period
        expected = fiscal_registry.determine_fiscal_period(ticker, period_end_date, filing_type)
        expected_year = expected.get('fiscal_year')
        expected_period = expected.get('fiscal_period')
        
        current_year = filing.get('fiscal_year')
        current_period = filing.get('fiscal_period')
        
        print("\nFiscal Period Analysis:")
        print(f"Current: {current_year}-{current_period}")
        print(f"Expected: {expected_year}-{expected_period}")
        
        if current_year == expected_year and current_period == expected_period:
            print("✓ Fiscal period is CORRECT")
        else:
            print("✗ Fiscal period is INCORRECT")
    
    # Look for potentially conflicting filing
    potential_conflict_id = None
    if ticker and filing_type and expected_year and expected_period:
        potential_conflict_id = f"{ticker}-{filing_type}-{expected_year}-{expected_period}"
        if potential_conflict_id != filing_id:
            conflict = get_filing(potential_conflict_id)
            
            print(f"\nChecking for potential conflict: {potential_conflict_id}")
            if conflict:
                print("✗ CONFLICT DETECTED - A filing with the expected fiscal period already exists:")
                print(f"  ID: {conflict.get('filing_id')}")
                print(f"  Period End: {conflict.get('period_end_date')}")
                
                # Compare period end dates
                conflict_period_end = conflict.get('period_end_date')
                if conflict_period_end and period_end_date:
                    if conflict_period_end == period_end_date:
                        print("  ⚠️ Both filings have the SAME period end date!")
                    else:
                        print(f"  ℹ️ Different period end dates: {period_end_date} vs {conflict_period_end}")
            else:
                print("✓ No conflict found - Safe to fix by updating fiscal period")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Examine a specific filing in Firestore")
    parser.add_argument("filing_id", help="Filing ID to examine")
    
    args = parser.parse_args()
    
    examine_filing(args.filing_id)