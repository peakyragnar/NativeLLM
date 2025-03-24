#!/usr/bin/env python3
"""
Utility to fix mislabeled fiscal periods in the database
"""
import os
import sys
import argparse
import logging
import datetime
from google.cloud import firestore

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.edgar.company_fiscal import fiscal_registry
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_fiscal_periods.log'),
        logging.StreamHandler()
    ]
)

# Set the path to service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def scan_filings(ticker=None, dry_run=True):
    """
    Scan filings and identify/fix mislabeled fiscal periods
    
    Args:
        ticker: Optional specific ticker to fix
        dry_run: If True, don't actually update the database
        
    Returns:
        Dict with results
    """
    db = firestore.Client(database='nativellm')
    
    # Query filings
    if ticker:
        query = db.collection('filings').where('company_ticker', '==', ticker)
    else:
        query = db.collection('filings')
    
    filings = list(query.stream())
    logging.info(f"Found {len(filings)} filings to check")
    
    # Results tracking
    results = {
        'total': len(filings),
        'checked': 0,
        'correct': 0,
        'fixed': 0,
        'errors': 0,
        'details': []
    }
    
    # Check each filing
    for filing_doc in filings:
        filing_id = filing_doc.id
        filing_data = filing_doc.to_dict()
        results['checked'] += 1
        
        try:
            # Extract key fields
            ticker = filing_data.get('company_ticker')
            filing_type = filing_data.get('filing_type')
            period_end_date = filing_data.get('period_end_date')
            current_fiscal_year = filing_data.get('fiscal_year')
            current_fiscal_period = filing_data.get('fiscal_period')
            
            if not period_end_date:
                logging.warning(f"Missing period_end_date for {filing_id}, skipping")
                results['errors'] += 1
                results['details'].append({
                    'filing_id': filing_id,
                    'status': 'error',
                    'reason': 'Missing period_end_date'
                })
                continue
            
            # Determine correct fiscal period using company fiscal calendar
            logging.info(f"Checking {filing_id} - {period_end_date}")
            
            # Special case for Apple's Q4 2022 issue (2022-12-31 should be Q1 2023)
            if ticker == "AAPL" and period_end_date == "2022-12-31" and current_fiscal_period == "Q4":
                logging.info(f"Special case for AAPL 2022-12-31 - should be Q1 2023, not Q4 2022")
                correct_fiscal_year = "2023"
                correct_fiscal_period = "Q1"
            else:
                fiscal_info = fiscal_registry.determine_fiscal_period(ticker, period_end_date)
                correct_fiscal_year = fiscal_info.get('fiscal_year')
                correct_fiscal_period = "annual" if filing_type == "10-K" else fiscal_info.get('fiscal_period')
            
            # Compare with current values
            if (correct_fiscal_year == current_fiscal_year and correct_fiscal_period == current_fiscal_period):
                logging.info(f"Fiscal period correct for {filing_id}")
                results['correct'] += 1
                results['details'].append({
                    'filing_id': filing_id,
                    'status': 'correct',
                    'current': f"{current_fiscal_year}-{current_fiscal_period}",
                })
                continue
                
            # Print current and correct values for debugging
            logging.info(f"Potential fix for {filing_id}:")
            logging.info(f"  Current: {current_fiscal_year}-{current_fiscal_period}")
            logging.info(f"  Correct: {correct_fiscal_year}-{correct_fiscal_period}")
            
            # Double-check if this is actually a different value (avoid false "fixes" with same value)
            if str(correct_fiscal_year) == str(current_fiscal_year) and str(correct_fiscal_period) == str(current_fiscal_period):
                logging.info(f"Values are identical for {filing_id}, marking as correct")
                results['correct'] += 1
                results['details'].append({
                    'filing_id': filing_id,
                    'status': 'correct',
                    'current': f"{current_fiscal_year}-{current_fiscal_period}",
                })
                continue
            
            # Need to fix this entry
            logging.info(f"Fixing {filing_id}: {current_fiscal_year}-{current_fiscal_period} -> {correct_fiscal_year}-{correct_fiscal_period}")
            
            # Skip update if dry run
            if dry_run:
                results['details'].append({
                    'filing_id': filing_id,
                    'status': 'would_fix',
                    'current': f"{current_fiscal_year}-{current_fiscal_period}",
                    'correct': f"{correct_fiscal_year}-{correct_fiscal_period}"
                })
                continue
            
            # Create new filing ID with correct fiscal info
            old_filing_id = filing_id
            new_filing_id = f"{ticker}-{filing_type}-{correct_fiscal_year}-{correct_fiscal_period}"
            
            # Check if target ID already exists
            if db.collection('filings').document(new_filing_id).get().exists:
                logging.warning(f"Target filing ID {new_filing_id} already exists, skipping")
                results['errors'] += 1
                results['details'].append({
                    'filing_id': filing_id,
                    'status': 'error',
                    'reason': f"Target filing ID {new_filing_id} already exists",
                    'current': f"{current_fiscal_year}-{current_fiscal_period}",
                    'correct': f"{correct_fiscal_year}-{correct_fiscal_period}"
                })
                continue
            
            # Update the document
            filing_data['fiscal_year'] = correct_fiscal_year
            filing_data['fiscal_period'] = correct_fiscal_period
            filing_data['filing_id'] = new_filing_id
            
            # Create new document with correct ID
            db.collection('filings').document(new_filing_id).set(filing_data)
            
            # Delete old document
            db.collection('filings').document(old_filing_id).delete()
            
            results['fixed'] += 1
            results['details'].append({
                'filing_id': old_filing_id,
                'status': 'fixed',
                'new_id': new_filing_id,
                'old': f"{current_fiscal_year}-{current_fiscal_period}",
                'new': f"{correct_fiscal_year}-{correct_fiscal_period}"
            })
            
        except Exception as e:
            logging.error(f"Error processing {filing_id}: {str(e)}")
            results['errors'] += 1
            results['details'].append({
                'filing_id': filing_id,
                'status': 'error',
                'reason': str(e)
            })
    
    # Log summary
    logging.info(f"Summary: {results['checked']} checked, {results['correct']} correct, {results['fixed']} fixed, {results['errors']} errors")
    return results

def print_summary(results):
    """Print a summary of the results"""
    print("\n===== FISCAL PERIOD FIX SUMMARY =====")
    print(f"Total filings checked: {results['checked']}")
    print(f"Correct fiscal periods: {results['correct']}")
    print(f"Fixed fiscal periods: {results['fixed']}")
    print(f"Errors: {results['errors']}")
    print("\nDetails:")
    
    # Group by status
    by_status = {}
    for detail in results['details']:
        status = detail['status']
        if status not in by_status:
            by_status[status] = []
        by_status[status].append(detail)
    
    # Print details by status
    for status in ['fixed', 'would_fix', 'error']:
        if status in by_status:
            print(f"\n-- {status.upper()} ({len(by_status[status])}) --")
            for detail in by_status[status]:
                if status in ['fixed', 'would_fix']:
                    old_period = detail.get('old', detail.get('current'))
                    new_period = detail.get('new', detail.get('correct'))
                    print(f"{detail['filing_id']}: {old_period} -> {new_period}")
                elif status == 'error':
                    print(f"{detail['filing_id']}: {detail.get('reason', 'Unknown error')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix mislabeled fiscal periods in the database")
    parser.add_argument("--ticker", help="Specific ticker to fix")
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
    print(f"\n===== FISCAL PERIOD FIX - {mode} =====")
    
    if args.ticker:
        print(f"Checking filings for {args.ticker}")
    else:
        print("Checking all filings")
    
    results = scan_filings(args.ticker, dry_run)
    print_summary(results)
    
    if dry_run:
        print("\nThis was a dry run. No changes were made.")
        print("To actually update the database, run with the --execute flag.")
    else:
        print("\nDatabase has been updated with corrected fiscal periods.")