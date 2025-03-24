from google.cloud import firestore
import os
import datetime
import argparse

# Set the path to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"

def list_company_filings(ticker):
    """List all filings for a specific company"""
    db = firestore.Client(database='nativellm')
    
    # Query all filings for the company
    filings_query = db.collection('filings').where('company_ticker', '==', ticker).get()
    
    all_filings = []
    for filing in filings_query:
        filing_data = filing.to_dict()
        all_filings.append(filing_data)
    
    # Sort by fiscal year and period
    # Convert fiscal_year to string for consistent sorting
    for filing in all_filings:
        if 'fiscal_year' in filing and filing['fiscal_year'] is not None:
            filing['fiscal_year'] = str(filing['fiscal_year'])
    
    all_filings.sort(key=lambda x: (x.get('fiscal_year', ''), x.get('fiscal_period', '')))
    
    return all_filings

def find_q4_10q_filings(ticker):
    """Find any 10-Q filings labeled as Q4 (which is incorrect)"""
    filings = list_company_filings(ticker)
    
    q4_10q_filings = []
    for filing in filings:
        if filing.get('filing_type') == '10-Q' and filing.get('fiscal_period') == 'Q4':
            q4_10q_filings.append(filing)
    
    return q4_10q_filings

def print_filing_info(filings):
    """Print information about filings in a formatted way"""
    print(f"Found {len(filings)} filings:")
    print("-" * 80)
    print(f"{'Filing ID':<30} {'Type':<6} {'Year':<6} {'Period':<8} {'End Date':<12} {'Filing Date':<12}")
    print("-" * 80)
    
    for filing in filings:
        print(f"{filing.get('filing_id', 'N/A'):<30} "
              f"{filing.get('filing_type', 'N/A'):<6} "
              f"{filing.get('fiscal_year', 'N/A'):<6} "
              f"{filing.get('fiscal_period', 'N/A'):<8} "
              f"{filing.get('period_end_date', 'N/A'):<12} "
              f"{filing.get('filing_date', 'N/A'):<12}")

def fix_q4_10q_filings(ticker, execute=False):
    """Find and fix any 10-Q filings labeled as Q4"""
    q4_10q_filings = find_q4_10q_filings(ticker)
    
    if not q4_10q_filings:
        print(f"No Q4 10-Q filings found for {ticker}. All filings are correctly labeled.")
        return
    
    print(f"Found {len(q4_10q_filings)} incorrectly labeled Q4 10-Q filings for {ticker}:")
    print_filing_info(q4_10q_filings)
    
    if not execute:
        print("\nThis was a dry run. To fix these filings, run with --execute flag.")
        return
    
    db = firestore.Client(database='nativellm')
    for filing in q4_10q_filings:
        try:
            # Get the period end date to determine the correct fiscal period
            period_end_date = filing.get('period_end_date')
            if not period_end_date:
                print(f"Warning: No period end date for {filing.get('filing_id')}. Skipping.")
                continue
                
            # Parse the period end date
            period_date = datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
            
            # For Apple with 2023-07-01 period end date, this is a Q3 filing (mislabeled as Q4)
            if ticker == "AAPL" and period_date.month == 7:
                correct_fiscal_period = "Q3"
                correct_fiscal_year = str(period_date.year)
            # For Apple, Q4 should actually be Q1 of the next fiscal year if it's in December
            elif ticker == "AAPL" and period_date.month in [10, 11, 12]:
                correct_fiscal_period = "Q1"
                correct_fiscal_year = str(period_date.year + 1)
            else:
                print(f"Warning: Unexpected date pattern for {filing.get('filing_id')}. Skipping.")
                continue
            
            old_filing_id = filing.get('filing_id')
            new_filing_id = f"{ticker}-{filing.get('filing_type')}-{correct_fiscal_year}-{correct_fiscal_period}"
            
            print(f"Fixing: {old_filing_id} -> {new_filing_id}")
            
            # Create new document with correct fiscal info
            new_filing_data = filing.copy()
            new_filing_data['filing_id'] = new_filing_id
            new_filing_data['fiscal_year'] = correct_fiscal_year
            new_filing_data['fiscal_period'] = correct_fiscal_period
            
            # Update paths
            for path_field in ['text_file_path', 'llm_file_path']:
                if path_field in new_filing_data:
                    path = new_filing_data[path_field]
                    # Update path with new fiscal info
                    path = path.replace(f"/{filing.get('fiscal_year')}/Q4/", f"/{correct_fiscal_year}/{correct_fiscal_period}/")
                    new_filing_data[path_field] = path
            
            # Create new document
            db.collection('filings').document(new_filing_id).set(new_filing_data)
            
            # Delete old document
            db.collection('filings').document(old_filing_id).delete()
            
            print(f"Successfully fixed {old_filing_id} -> {new_filing_id}")
            
        except Exception as e:
            print(f"Error fixing {filing.get('filing_id')}: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check and fix company filings in Firestore")
    parser.add_argument("--ticker", required=True, help="Company ticker to check")
    parser.add_argument("--list", action="store_true", help="List all filings for the company")
    parser.add_argument("--check-q4", action="store_true", help="Check for incorrect Q4 10-Q filings")
    parser.add_argument("--fix-q4", action="store_true", help="Fix incorrect Q4 10-Q filings")
    parser.add_argument("--execute", action="store_true", help="Actually execute the fixes (otherwise dry run)")
    
    args = parser.parse_args()
    
    if args.list:
        filings = list_company_filings(args.ticker)
        print_filing_info(filings)
    
    if args.check_q4:
        q4_filings = find_q4_10q_filings(args.ticker)
        if q4_filings:
            print_filing_info(q4_filings)
        else:
            print(f"No Q4 10-Q filings found for {args.ticker}. All filings are correctly labeled.")
    
    if args.fix_q4:
        fix_q4_10q_filings(args.ticker, execute=args.execute)
    
    if not (args.list or args.check_q4 or args.fix_q4):
        parser.print_help()