#!/usr/bin/env python3
"""
Parallel Processing Module

This module provides functionality for processing multiple companies in parallel
with proper concurrency control and error handling.
"""

import os
import sys
import json
import time
import argparse
import logging
import concurrent.futures
from tqdm import tqdm

# Import from src2 modules
from .. import config
from ..sec.company_list import get_top_companies, get_companies_by_sector 
from ..sec.pipeline import SECPipeline

def process_company_safe(ticker, pipeline=None):
    """
    Process a company with error handling
    
    Args:
        ticker: Company ticker symbol
        pipeline: Optional SECPipeline instance (creates one if not provided)
        
    Returns:
        dict: Processing result with status information
    """
    try:
        if pipeline is None:
            pipeline = SECPipeline()
        return pipeline.process_company(ticker)
    except Exception as e:
        logging.error(f"Error processing {ticker}: {str(e)}")
        return {"ticker": ticker, "error": str(e)}

def process_companies_parallel(tickers, max_workers=3, pipeline=None):
    """
    Process companies in parallel with limited concurrency
    
    Args:
        tickers: List of company ticker symbols to process
        max_workers: Maximum number of concurrent threads
        pipeline: Optional SECPipeline instance (creates one if not provided)
        
    Returns:
        dict: Summary report of processing results
    """
    results = []
    errors = []
    
    # Create pipeline instance if not provided
    if pipeline is None:
        pipeline = SECPipeline()
    
    # Use ThreadPoolExecutor to limit concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and track them with tqdm for progress
        future_to_ticker = {executor.submit(process_company_safe, ticker, pipeline): ticker for ticker in tickers}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(tickers), desc="Processing companies"):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                results.append(result)
                if "error" in result:
                    errors.append({"ticker": ticker, "error": result["error"]})
            except Exception as e:
                error_msg = f"Exception processing {ticker}: {str(e)}"
                logging.error(error_msg)
                print(error_msg)
                errors.append({"ticker": ticker, "error": str(e)})
    
    # Save summary report
    report = {
        "companies_processed": len(results),
        "successful_companies": len(results) - len(errors),
        "errors": errors,
        "results": results
    }
    
    # Create report directory
    output_dir = config.PROCESSED_DATA_DIR
    os.makedirs(output_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(output_dir, f"processing_report_{timestamp}.json")
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Processing complete: {len(results) - len(errors)}/{len(results)} companies successful")
    print(f"Report saved to {report_path}")
    
    return report

def main():
    """Command-line interface for parallel processing"""
    parser = argparse.ArgumentParser(description="Process SEC filings for companies in parallel")
    parser.add_argument('--tickers', nargs='+', help='List of tickers to process')
    parser.add_argument('--sector', help='Process companies in a specific sector')
    parser.add_argument('--top', type=int, default=10, help='Process top N companies')
    parser.add_argument('--workers', type=int, default=3, help='Maximum number of concurrent workers')
    parser.add_argument('--filing-types', nargs='+', default=['10-K', '10-Q'], 
                        help='Filing types to process (default: 10-K and 10-Q)')
    
    args = parser.parse_args()
    
    if args.tickers:
        tickers = args.tickers
    elif args.sector:
        tickers = [c["ticker"] for c in get_companies_by_sector(args.sector)]
    else:
        tickers = [c["ticker"] for c in get_top_companies(args.top)]
    
    # Create pipeline with specified filing types
    pipeline = SECPipeline(filing_types=args.filing_types)
    
    # Process companies in parallel
    process_companies_parallel(tickers, args.workers, pipeline)

if __name__ == "__main__":
    main()