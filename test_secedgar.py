#!/usr/bin/env python3
"""
Test Script for secedgar pipeline implementation
"""

import os
import sys
import logging
import argparse
import time
from typing import List, Dict, Any
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_secedgar.log'),
        logging.StreamHandler()
    ]
)

# Import secedgar pipeline
from secedgar_pipeline import process_ticker, process_company_filing_secedgar
from secedgar.filings import FilingType

def test_single_filing(ticker: str, filing_type_str: str) -> Dict[str, Any]:
    """
    Test processing a single filing for a company
    
    Args:
        ticker: Company ticker symbol
        filing_type_str: Filing type as string ("10-K" or "10-Q")
        
    Returns:
        Dictionary with test results
    """
    logging.info(f"Testing {ticker} {filing_type_str}")
    
    # Convert string to FilingType
    filing_type = FilingType.FILING_10K if filing_type_str == "10-K" else FilingType.FILING_10Q
    
    # Process the filing
    start_time = time.time()
    result = process_company_filing_secedgar(ticker, filing_type)
    end_time = time.time()
    
    # Add timing information
    result["processing_time"] = end_time - start_time
    
    # Print results
    print(f"\nTest results for {ticker} {filing_type_str}:")
    print(f"Success: {'✅' if result.get('success') else '❌'}")
    print(f"XBRL processed: {'✅' if result.get('xbrl_processed') else '❌'}")
    print(f"HTML processed: {'✅' if result.get('html_processed') else '❌'}")
    
    if result.get("fact_count"):
        print(f"Facts extracted: {result.get('fact_count')}")
    
    if result.get("error"):
        print(f"Error: {result.get('error')}")
        
    print(f"Processing time: {result.get('processing_time'):.2f} seconds")
    
    return result

def test_multiple_companies(tickers: List[str], filing_type_str: str = None) -> Dict[str, Any]:
    """
    Test processing multiple companies
    
    Args:
        tickers: List of company ticker symbols
        filing_type_str: Optional filing type to test (if None, test both 10-K and 10-Q)
        
    Returns:
        Dictionary with test results
    """
    results = {
        "total_tests": 0,
        "successful_tests": 0,
        "xbrl_processed": 0,
        "html_processed": 0,
        "failed_tests": [],
        "processing_times": [],
        "fact_counts": [],
        "company_results": {}
    }
    
    for ticker in tickers:
        if filing_type_str:
            # Test specific filing type
            result = test_single_filing(ticker, filing_type_str)
            
            results["total_tests"] += 1
            if result.get("success"):
                results["successful_tests"] += 1
            if result.get("xbrl_processed"):
                results["xbrl_processed"] += 1
            if result.get("html_processed"):
                results["html_processed"] += 1
            if result.get("fact_count"):
                results["fact_counts"].append(result.get("fact_count"))
            if result.get("processing_time"):
                results["processing_times"].append(result.get("processing_time"))
            if not result.get("success"):
                results["failed_tests"].append(f"{ticker} {filing_type_str}: {result.get('error')}")
                
            results["company_results"][f"{ticker}_{filing_type_str}"] = result
        else:
            # Test both 10-K and 10-Q
            ticker_result = process_ticker(ticker)
            
            for filing in ticker_result.get("filings_processed", []):
                filing_type = filing.get("filing_type")
                
                results["total_tests"] += 1
                if filing.get("success"):
                    results["successful_tests"] += 1
                if filing.get("xbrl_processed"):
                    results["xbrl_processed"] += 1
                if filing.get("html_processed"):
                    results["html_processed"] += 1
                if filing.get("fact_count"):
                    results["fact_counts"].append(filing.get("fact_count"))
                if not filing.get("success"):
                    results["failed_tests"].append(f"{ticker} {filing_type}: {filing.get('error')}")
                    
                results["company_results"][f"{ticker}_{filing_type}"] = filing
    
    # Calculate summary stats
    if results["processing_times"]:
        results["avg_processing_time"] = sum(results["processing_times"]) / len(results["processing_times"])
    if results["fact_counts"]:
        results["avg_fact_count"] = sum(results["fact_counts"]) / len(results["fact_counts"])
    
    # Print summary
    print("\n===== TEST SUMMARY =====")
    print(f"Total tests: {results['total_tests']}")
    if results["total_tests"] > 0:
        print(f"Success rate: {results['successful_tests'] / results['total_tests'] * 100:.1f}%")
        print(f"XBRL processing rate: {results['xbrl_processed'] / results['total_tests'] * 100:.1f}%")
        print(f"HTML processing rate: {results['html_processed'] / results['total_tests'] * 100:.1f}%")
    
    if "avg_processing_time" in results:
        print(f"Average processing time: {results['avg_processing_time']:.2f} seconds")
    if "avg_fact_count" in results:
        print(f"Average fact count: {results['avg_fact_count']:.1f}")
    
    if results["failed_tests"]:
        print("\n===== FAILED TESTS =====")
        for failure in results["failed_tests"]:
            print(f"- {failure}")
    
    return results

def main():
    """Main entry point for the test script"""
    parser = argparse.ArgumentParser(description="Test secedgar Pipeline Implementation")
    
    # Test options
    parser.add_argument("--ticker", help="Test a single company ticker")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], help="Test a specific filing type")
    parser.add_argument("--comprehensive", action="store_true", help="Run comprehensive test with multiple companies")
    
    args = parser.parse_args()
    
    if args.ticker and args.filing_type:
        # Test a specific company and filing type
        test_single_filing(args.ticker, args.filing_type)
    elif args.ticker:
        # Test both filing types for a company
        process_ticker(args.ticker)
    elif args.comprehensive:
        # Run comprehensive test
        test_companies = ["MSFT", "AAPL", "GOOGL", "TM", "PYPL", "LOW"]
        test_multiple_companies(test_companies)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()