# src/parallel_processor.py
import os
import sys
import json
import time
import argparse
import concurrent.futures
from tqdm import tqdm

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.process_company import process_company
from src.company_list import get_top_companies, get_companies_by_sector
from src.config import PROCESSED_DATA_DIR

def process_company_safe(ticker):
    """Process a company with error handling"""
    try:
        return process_company(ticker)
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}

def process_companies_parallel(tickers, max_workers=3):
    """Process companies in parallel with limited concurrency"""
    results = []
    errors = []
    
    # Use ThreadPoolExecutor to limit concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and track them with tqdm for progress
        future_to_ticker = {executor.submit(process_company_safe, ticker): ticker for ticker in tickers}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(tickers), desc="Processing companies"):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                results.append(result)
                if "error" in result:
                    errors.append({"ticker": ticker, "error": result["error"]})
            except Exception as e:
                print(f"Exception processing {ticker}: {str(e)}")
                errors.append({"ticker": ticker, "error": str(e)})
    
    # Save summary report
    report = {
        "companies_processed": len(results),
        "successful_companies": len(results) - len(errors),
        "errors": errors,
        "results": results
    }
    
    # Create report directory
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(PROCESSED_DATA_DIR, f"processing_report_{timestamp}.json")
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Processing complete: {len(results) - len(errors)}/{len(results)} companies successful")
    print(f"Report saved to {report_path}")
    
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process SEC filings for companies in parallel")
    parser.add_argument('--tickers', nargs='+', help='List of tickers to process')
    parser.add_argument('--sector', help='Process companies in a specific sector')
    parser.add_argument('--top', type=int, default=10, help='Process top N companies')
    parser.add_argument('--workers', type=int, default=3, help='Maximum number of concurrent workers')
    
    args = parser.parse_args()
    
    if args.tickers:
        tickers = args.tickers
    elif args.sector:
        tickers = [c["ticker"] for c in get_companies_by_sector(args.sector)]
    else:
        tickers = [c["ticker"] for c in get_top_companies(args.top)]
    
    process_companies_parallel(tickers, args.workers)