"""
Test script for extracting iXBRL data from SEC filings.
"""

import os
import sys
import json
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.xbrl.enhanced_processor import process_company_filing, process_and_format_filing

def test_google_extraction(filing_type=None, force_download=False):
    """Test extraction of Google's financial data"""
    # Set default filing type if not provided
    if not filing_type:
        filing_type = "10-K"
    
    # Extract and process Google's filing
    result = extract_process_and_format("GOOGL", filing_type, force_download)
    
    if "error" in result:
        print(f"Error processing Google's {filing_type}: {result['error']}")
        return
    
    print("\nSuccessfully processed Google's filing:")
    print(f"Format: {result.get('format', 'unknown')}")
    print(f"Facts: {result.get('facts_count', 0)}")
    print(f"Contexts: {result.get('contexts_count', 0)}")
    print(f"Output file: {result.get('llm_file_path', 'unknown')}")
    print(f"Content size: {result.get('llm_content_size', 0)} bytes")
    
    # Check if we successfully extracted data
    if result.get('facts_count', 0) == 0:
        print("\nWARNING: No facts were extracted!")
    else:
        print("\nSuccess! Facts were successfully extracted.")
    
    return result

def test_apple_extraction(filing_type=None, force_download=False):
    """Test extraction of Apple's financial data for comparison"""
    # Set default filing type if not provided
    if not filing_type:
        filing_type = "10-K"
    
    # Extract and process Apple's filing
    result = extract_process_and_format("AAPL", filing_type, force_download)
    
    if "error" in result:
        print(f"Error processing Apple's {filing_type}: {result['error']}")
        return
    
    print("\nSuccessfully processed Apple's filing:")
    print(f"Format: {result.get('format', 'unknown')}")
    print(f"Facts: {result.get('facts_count', 0)}")
    print(f"Contexts: {result.get('contexts_count', 0)}")
    print(f"Output file: {result.get('llm_file_path', 'unknown')}")
    print(f"Content size: {result.get('llm_content_size', 0)} bytes")
    
    return result

def extract_process_and_format(ticker, filing_type, force_download=False):
    """Extract, process, and format financial data for a specific filing"""
    print(f"Processing {ticker} {filing_type}...")
    
    # Step 1: Find the company's filing
    filing_result = find_company_filings(ticker, [filing_type])
    
    if "error" in filing_result:
        return {"error": filing_result["error"]}
    
    # Step 2: Check if we found the filing
    if filing_type not in filing_result.get("filings", {}):
        return {"error": f"No {filing_type} filing found for {ticker}"}
    
    # Step 3: Get the filing metadata
    filing_metadata = filing_result["filings"][filing_type]
    
    # Add ticker and company name to metadata
    filing_metadata["ticker"] = ticker
    filing_metadata["company_name"] = filing_result.get("company_name")
    
    # Step 4: Process and format the filing
    result = process_and_format_filing(filing_metadata, force_download)
    
    return result

def compare_extraction_results(companies=None, filing_type="10-K", force_download=False):
    """Compare extraction results across multiple companies"""
    if not companies:
        companies = ["GOOGL", "AAPL", "MSFT", "AMZN", "META"]
    
    results = {}
    
    for ticker in companies:
        print(f"\n===== Processing {ticker} {filing_type} =====")
        result = extract_process_and_format(ticker, filing_type, force_download)
        results[ticker] = result
        
        if "error" in result:
            print(f"Error processing {ticker}: {result['error']}")
        else:
            print(f"Successfully processed {ticker}:")
            print(f"Format: {result.get('format', 'unknown')}")
            print(f"Facts: {result.get('facts_count', 0)}")
            print(f"Contexts: {result.get('contexts_count', 0)}")
    
    # Print comparison summary
    print("\n===== Extraction Results Comparison =====")
    print(f"{'Company':<10} {'Format':<10} {'Facts':<10} {'Contexts':<10}")
    print("-" * 50)
    
    for ticker, result in results.items():
        if "error" in result:
            print(f"{ticker:<10} {'ERROR':<10} {'N/A':<10} {'N/A':<10}")
        else:
            print(f"{ticker:<10} {result.get('format', 'unknown'):<10} {result.get('facts_count', 0):<10} {result.get('contexts_count', 0):<10}")
    
    return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test iXBRL extraction")
    parser.add_argument("--ticker", default="GOOGL", help="Ticker symbol to test")
    parser.add_argument("--filing-type", default="10-K", help="Filing type to test")
    parser.add_argument("--force-download", action="store_true", help="Force download of documents")
    parser.add_argument("--compare", action="store_true", help="Compare extraction across multiple companies")
    parser.add_argument("--companies", nargs="+", help="List of companies to compare")
    
    args = parser.parse_args()
    
    if args.compare:
        compare_extraction_results(args.companies, args.filing_type, args.force_download)
    else:
        if args.ticker == "GOOGL" or args.ticker == "GOOG":
            test_google_extraction(args.filing_type, args.force_download)
        elif args.ticker == "AAPL":
            test_apple_extraction(args.filing_type, args.force_download)
        else:
            extract_process_and_format(args.ticker, args.filing_type, args.force_download)