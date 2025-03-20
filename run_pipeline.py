# run_pipeline.py
import os
import sys
import argparse
import time

def setup_directories():
    """Set up project directories"""
    from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
    
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    print(f"Set up directories: {RAW_DATA_DIR}, {PROCESSED_DATA_DIR}")

def run_initial_companies():
    """Run the pipeline for initial companies"""
    from src.process_companies import process_companies
    
    print("Processing initial companies...")
    result = process_companies()
    return result

def run_specific_company(ticker):
    """Run the pipeline for a specific company"""
    from src.process_company import process_company
    
    print(f"Processing company: {ticker}")
    result = process_company(ticker)
    return result

def run_parallel_processing(count, workers):
    """Run parallel processing for top N companies"""
    from src.parallel_processor import process_companies_parallel
    from src.company_list import get_top_companies
    
    companies = get_top_companies(count)
    tickers = [c["ticker"] for c in companies]
    
    print(f"Processing top {len(tickers)} companies with {workers} workers...")
    result = process_companies_parallel(tickers, workers)
    return result

def main():
    parser = argparse.ArgumentParser(description="Run the SEC filing to LLM format pipeline")
    parser.add_argument('--setup', action='store_true', help='Set up project directories')
    parser.add_argument('--initial', action='store_true', help='Process initial companies from config')
    parser.add_argument('--company', help='Process a specific company by ticker')
    parser.add_argument('--top', type=int, help='Process top N companies in parallel')
    parser.add_argument('--workers', type=int, default=3, help='Number of parallel workers')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_directories()
    
    if args.initial:
        run_initial_companies()
    
    if args.company:
        run_specific_company(args.company)
    
    if args.top:
        run_parallel_processing(args.top, args.workers)
    
    # If no action specified, show help
    if not (args.setup or args.initial or args.company or args.top):
        parser.print_help()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")