"""
Run a test of our enhanced XBRL processing pipeline with Google's filings.
"""

import os
import sys
import argparse

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.edgar.filing_finder import find_company_filings
from src.xbrl.enhanced_processor import process_and_format_filing

def main():
    parser = argparse.ArgumentParser(description="Test XBRL processing")
    parser.add_argument("--ticker", default="GOOGL", help="Ticker symbol to process")
    parser.add_argument("--filing-type", default="10-K", help="Filing type to process")
    parser.add_argument("--force-download", action="store_true", help="Force download of documents")
    
    args = parser.parse_args()
    
    print(f"Processing {args.ticker} {args.filing_type}...")
    
    # Find the filing
    filing_result = find_company_filings(args.ticker, [args.filing_type])
    
    if "error" in filing_result:
        print(f"Error finding filing: {filing_result['error']}")
        return
    
    if args.filing_type not in filing_result.get("filings", {}):
        print(f"No {args.filing_type} filing found for {args.ticker}")
        return
    
    # Process the filing
    filing_metadata = filing_result["filings"][args.filing_type]
    filing_metadata["ticker"] = args.ticker
    filing_metadata["company_name"] = filing_result.get("company_name")
    
    result = process_and_format_filing(filing_metadata, args.force_download)
    
    if "error" in result:
        print(f"Error processing filing: {result['error']}")
        return
    
    print("\nProcessing Summary:")
    print(f"Format: {result.get('format', 'unknown')}")
    print(f"Facts: {result.get('facts_count', 0)}")
    print(f"Contexts: {result.get('contexts_count', 0)}")
    print(f"Output file: {result.get('llm_file_path', 'unknown')}")
    print(f"Content size: {result.get('llm_content_size', 0)} bytes")
    
    if result.get('facts_count', 0) > 0:
        print("\nSuccess! Facts were successfully extracted.")
    else:
        print("\nWarning: No facts were extracted.")

if __name__ == "__main__":
    main()