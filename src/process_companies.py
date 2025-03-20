# src/process_companies.py
import os
import sys
import json
import time
import argparse

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.process_company import process_company
from src.config import INITIAL_COMPANIES, PROCESSED_DATA_DIR

def process_companies(tickers=None):
    """Process a list of companies"""
    if tickers is None:
        tickers = [company["ticker"] for company in INITIAL_COMPANIES]
    
    results = []
    errors = []
    
    for ticker in tickers:
        try:
            result = process_company(ticker)
            results.append(result)
            if "error" in result:
                errors.append({"ticker": ticker, "error": result["error"]})
        except Exception as e:
            print(f"Exception processing {ticker}: {str(e)}")
            errors.append({"ticker": ticker, "error": str(e)})
        
        # Add delay between companies to respect SEC rate limits
        time.sleep(2)
    
    # Save summary report
    report = {
        "companies_processed": len(results),
        "successful_companies": len(results) - len(errors),
        "errors": errors,
        "results": results
    }
    
    # Create report directory
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    report_path = os.path.join(PROCESSED_DATA_DIR, "processing_report.json")
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Processing complete: {len(results) - len(errors)}/{len(results)} companies successful")
    print(f"Report saved to {report_path}")
    
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process SEC filings for companies")
    parser.add_argument('--tickers', nargs='+', help='List of tickers to process')
    
    args = parser.parse_args()
    
    if args.tickers:
        process_companies(args.tickers)
    else:
        process_companies()