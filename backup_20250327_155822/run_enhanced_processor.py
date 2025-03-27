"""
Run the enhanced XBRL processor with different companies to test
both XBRL and iXBRL handling capabilities.
"""

import os
import sys
import argparse

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.xbrl.enhanced_processor import process_and_format_filing

def process_company(ticker, filing_type="10-K", force_download=False):
    """Process a specific company's filing"""
    
    # Get CIK for common companies (in a real system, this would be looked up dynamically)
    cik_map = {
        "AAPL": "0000320193",
        "MSFT": "0000789019", 
        "GOOGL": "0001652044",
        "AMZN": "0001018724",
        "META": "0001326801"
    }
    
    # Get company name for common companies
    name_map = {
        "AAPL": "Apple Inc.",
        "MSFT": "Microsoft Corporation",
        "GOOGL": "Alphabet Inc.", 
        "AMZN": "Amazon.com, Inc.",
        "META": "Meta Platforms, Inc."
    }
    
    # Get period end date for recent filings (in a real system, this would be looked up)
    period_map = {
        "AAPL": "2023-09-30",
        "MSFT": "2023-12-31",
        "GOOGL": "2023-12-31",
        "AMZN": "2023-12-31", 
        "META": "2023-12-31"
    }
    
    # Prepare filing metadata
    filing_metadata = {
        "ticker": ticker,
        "cik": cik_map.get(ticker, "unknown"),
        "company_name": name_map.get(ticker, "Unknown Company"),
        "filing_type": filing_type,
        "period_end_date": period_map.get(ticker, "2023-12-31")
    }
    
    # For Google, set a specific accession number known to work
    if ticker == "GOOGL":
        filing_metadata["accession_number"] = "000165204425-000014"
        filing_metadata["filing_date"] = "2024-01-30"
    
    # Process the filing
    print(f"\nProcessing {ticker} ({filing_metadata['company_name']})")
    print(f"Filing type: {filing_type}")
    print(f"Period end: {filing_metadata['period_end_date']}")
    
    result = process_and_format_filing(filing_metadata, force_download)
    
    if "error" in result:
        print(f"Error: {result['error']}")
        return False
    
    print(f"\nSuccess! Filing processed using {result.get('format')} format")
    print(f"Facts extracted: {result.get('fact_count', 0)}")
    print(f"LLM file saved to: {result.get('llm_file_path')}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Process company filings with enhanced XBRL processor")
    parser.add_argument('--ticker', default="GOOGL", help='Company ticker symbol')
    parser.add_argument('--filing-type', default="10-K", help='Filing type (10-K, 10-Q)')
    parser.add_argument('--force', action='store_true', help='Force download even if file exists')
    
    args = parser.parse_args()
    
    success = process_company(args.ticker, args.filing_type, args.force)
    
    if success:
        print("\nProcessing completed successfully!")
    else:
        print("\nProcessing failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()