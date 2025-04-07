#!/usr/bin/env python3
"""
Batch Test XBRL Hierarchy Extraction

This script runs the XBRL hierarchy extraction test on multiple companies and filings
to ensure it works consistently across different companies and time periods.
"""

import os
import sys
import json
import logging
import argparse
import subprocess
import glob
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def find_filings(base_dir, tickers=None, filing_types=None, years=None):
    """
    Find filings for the specified tickers, filing types, and years.
    
    Args:
        base_dir: Base directory for SEC downloads
        tickers: List of tickers to include (None for all)
        filing_types: List of filing types to include (None for all)
        years: List of years to include (None for all)
        
    Returns:
        List of filing paths
    """
    filings = []
    
    # Default to all tickers if none specified
    if not tickers:
        tickers = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    
    # Default to 10-K and 10-Q if no filing types specified
    if not filing_types:
        filing_types = ['10-K', '10-Q']
    
    # Process each ticker
    for ticker in tickers:
        ticker_dir = os.path.join(base_dir, ticker)
        if not os.path.exists(ticker_dir):
            logging.warning(f"Directory not found for ticker {ticker}: {ticker_dir}")
            continue
        
        # Process each filing type
        for filing_type in filing_types:
            filing_type_dir = os.path.join(ticker_dir, filing_type)
            if not os.path.exists(filing_type_dir):
                logging.warning(f"Directory not found for {ticker} {filing_type}: {filing_type_dir}")
                continue
            
            # Get all accession directories
            accession_dirs = [d for d in os.listdir(filing_type_dir) if os.path.isdir(os.path.join(filing_type_dir, d))]
            
            for accession in accession_dirs:
                accession_dir = os.path.join(filing_type_dir, accession)
                
                # Find HTML files
                html_files = glob.glob(os.path.join(accession_dir, "*.htm")) + glob.glob(os.path.join(accession_dir, "*.html"))
                html_files = [f for f in html_files if not os.path.basename(f).startswith('index')]
                
                if not html_files:
                    logging.warning(f"No HTML files found in {accession_dir}")
                    continue
                
                # Check if filing_info.json exists
                filing_info_path = os.path.join(accession_dir, 'filing_info.json')
                filing_year = None
                
                if os.path.exists(filing_info_path):
                    try:
                        with open(filing_info_path, 'r') as f:
                            filing_info = json.load(f)
                        
                        # Extract filing date
                        filing_date = filing_info.get('filing_date')
                        if filing_date:
                            try:
                                filing_year = datetime.strptime(filing_date, '%Y-%m-%d').year
                            except ValueError:
                                pass
                    except Exception as e:
                        logging.error(f"Error reading filing_info.json: {str(e)}")
                
                # If we couldn't get the year from filing_info.json, try to extract from filename
                if not filing_year:
                    for html_file in html_files:
                        filename = os.path.basename(html_file)
                        year_match = re.search(r'20\d{2}', filename)
                        if year_match:
                            filing_year = int(year_match.group(0))
                            break
                
                # Skip if year doesn't match
                if years and filing_year and filing_year not in years:
                    continue
                
                # Add the first HTML file to the list
                if html_files:
                    filings.append({
                        'ticker': ticker,
                        'filing_type': filing_type,
                        'year': filing_year,
                        'accession': accession,
                        'html_path': html_files[0]
                    })
    
    return filings

def run_test_on_filing(filing, output_dir):
    """
    Run the XBRL hierarchy extraction test on a filing.
    
    Args:
        filing: Filing information dictionary
        output_dir: Directory to save output files
        
    Returns:
        True if successful, False otherwise
    """
    ticker = filing['ticker']
    filing_type = filing['filing_type']
    year = filing['year']
    html_path = filing['html_path']
    
    # Create output filename
    output_filename = f"{ticker}_{filing_type}_{year}_hierarchy.json"
    output_path = os.path.join(output_dir, output_filename)
    
    # Run the test script
    cmd = [
        'python3',
        '/Users/michael/NativeLLM/test_xbrl_hierarchy.py',
        '--html', html_path,
        '--output', output_path
    ]
    
    logging.info(f"Running test on {ticker} {filing_type} {year}")
    logging.info(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logging.info(f"Test successful for {ticker} {filing_type} {year}")
            logging.info(f"Output saved to {output_path}")
            
            # Check if the output file exists and has content
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return True
            else:
                logging.error(f"Output file is empty or doesn't exist: {output_path}")
                return False
        else:
            logging.error(f"Test failed for {ticker} {filing_type} {year}")
            logging.error(f"Error: {result.stderr}")
            return False
    
    except subprocess.TimeoutExpired:
        logging.error(f"Test timed out for {ticker} {filing_type} {year}")
        return False
    except Exception as e:
        logging.error(f"Error running test for {ticker} {filing_type} {year}: {str(e)}")
        return False

def analyze_results(output_dir):
    """
    Analyze the results of the batch test.
    
    Args:
        output_dir: Directory with output files
        
    Returns:
        Dictionary with analysis results
    """
    results = {
        'total_filings': 0,
        'successful_filings': 0,
        'failed_filings': 0,
        'by_ticker': {},
        'by_filing_type': {},
        'by_year': {}
    }
    
    # Get all output files
    output_files = glob.glob(os.path.join(output_dir, '*_hierarchy.json'))
    
    for output_file in output_files:
        filename = os.path.basename(output_file)
        parts = filename.split('_')
        
        if len(parts) >= 3:
            ticker = parts[0]
            filing_type = parts[1]
            year = parts[2]
            
            # Initialize counters if needed
            if ticker not in results['by_ticker']:
                results['by_ticker'][ticker] = {'total': 0, 'successful': 0, 'failed': 0}
            
            if filing_type not in results['by_filing_type']:
                results['by_filing_type'][filing_type] = {'total': 0, 'successful': 0, 'failed': 0}
            
            if year not in results['by_year']:
                results['by_year'][year] = {'total': 0, 'successful': 0, 'failed': 0}
            
            # Check if the file has content
            file_size = os.path.getsize(output_file)
            results['total_filings'] += 1
            results['by_ticker'][ticker]['total'] += 1
            results['by_filing_type'][filing_type]['total'] += 1
            results['by_year'][year]['total'] += 1
            
            if file_size > 0:
                # Try to load the file to check if it's valid JSON
                try:
                    with open(output_file, 'r') as f:
                        data = json.load(f)
                    
                    # Check if it has the expected structure
                    if 'concepts' in data and 'presentation_hierarchy' in data and 'facts' in data:
                        results['successful_filings'] += 1
                        results['by_ticker'][ticker]['successful'] += 1
                        results['by_filing_type'][filing_type]['successful'] += 1
                        results['by_year'][year]['successful'] += 1
                    else:
                        results['failed_filings'] += 1
                        results['by_ticker'][ticker]['failed'] += 1
                        results['by_filing_type'][filing_type]['failed'] += 1
                        results['by_year'][year]['failed'] += 1
                except Exception:
                    results['failed_filings'] += 1
                    results['by_ticker'][ticker]['failed'] += 1
                    results['by_filing_type'][filing_type]['failed'] += 1
                    results['by_year'][year]['failed'] += 1
            else:
                results['failed_filings'] += 1
                results['by_ticker'][ticker]['failed'] += 1
                results['by_filing_type'][filing_type]['failed'] += 1
                results['by_year'][year]['failed'] += 1
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Batch test XBRL hierarchy extraction")
    parser.add_argument("--base-dir", default="/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads", help="Base directory for SEC downloads")
    parser.add_argument("--output-dir", default="/Users/michael/NativeLLM/hierarchy_test_results", help="Directory to save output files")
    parser.add_argument("--tickers", nargs="+", help="Tickers to include (default: all)")
    parser.add_argument("--filing-types", nargs="+", help="Filing types to include (default: 10-K, 10-Q)")
    parser.add_argument("--years", nargs="+", type=int, help="Years to include (default: all)")
    parser.add_argument("--max-filings", type=int, default=10, help="Maximum number of filings to process (default: 10)")
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Find filings
    filings = find_filings(args.base_dir, args.tickers, args.filing_types, args.years)
    
    # Limit the number of filings if needed
    if args.max_filings and len(filings) > args.max_filings:
        filings = filings[:args.max_filings]
    
    logging.info(f"Found {len(filings)} filings to process")
    
    # Run tests
    successful_filings = 0
    failed_filings = 0
    
    for filing in filings:
        if run_test_on_filing(filing, args.output_dir):
            successful_filings += 1
        else:
            failed_filings += 1
    
    logging.info(f"Batch test completed")
    logging.info(f"Successful filings: {successful_filings}")
    logging.info(f"Failed filings: {failed_filings}")
    
    # Analyze results
    results = analyze_results(args.output_dir)
    
    # Save results
    results_path = os.path.join(args.output_dir, 'batch_test_results.json')
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Results saved to {results_path}")
    
    # Print summary
    print("\nBatch Test Summary:")
    print(f"  Total Filings: {results['total_filings']}")
    print(f"  Successful Filings: {results['successful_filings']}")
    print(f"  Failed Filings: {results['failed_filings']}")
    
    print("\nResults by Ticker:")
    for ticker, ticker_results in results['by_ticker'].items():
        success_rate = ticker_results['successful'] / ticker_results['total'] * 100 if ticker_results['total'] > 0 else 0
        print(f"  {ticker}: {ticker_results['successful']}/{ticker_results['total']} ({success_rate:.1f}%)")
    
    print("\nResults by Filing Type:")
    for filing_type, filing_type_results in results['by_filing_type'].items():
        success_rate = filing_type_results['successful'] / filing_type_results['total'] * 100 if filing_type_results['total'] > 0 else 0
        print(f"  {filing_type}: {filing_type_results['successful']}/{filing_type_results['total']} ({success_rate:.1f}%)")
    
    print("\nResults by Year:")
    for year, year_results in results['by_year'].items():
        success_rate = year_results['successful'] / year_results['total'] * 100 if year_results['total'] > 0 else 0
        print(f"  {year}: {year_results['successful']}/{year_results['total']} ({success_rate:.1f}%)")
    
    return 0

if __name__ == "__main__":
    import re  # Import here to avoid issues with the global scope
    sys.exit(main())
