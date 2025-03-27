"""
Test script to verify the enhanced processor with both Google and Apple filings.
"""

import os
import sys
import time

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.xbrl.enhanced_processor import process_company_filing
from src.formatter.llm_formatter import generate_llm_format, save_llm_format

def test_filing_download(ticker, cik, accession_number=None, filing_type="10-K", period_end="2023-12-31"):
    """Test downloading and processing a filing with the enhanced processor"""
    
    # Create filing metadata
    filing_metadata = {
        "ticker": ticker,
        "cik": cik,
        "filing_type": filing_type,
        "period_end_date": period_end
    }
    
    # Add accession number if provided
    if accession_number:
        filing_metadata["accession_number"] = accession_number
    
    print(f"\nTesting enhanced processor with {ticker} {filing_type}")
    print(f"CIK: {cik}")
    print(f"Period end: {period_end}")
    
    # Process the filing with the enhanced processor
    result = process_company_filing(filing_metadata, force_download=True)
    
    if "error" in result:
        print(f"\nError processing filing: {result['error']}")
        return False
    
    # Print basic statistics about the extraction
    print(f"\nSuccess! Filing processed using {result.get('processing_path', 'unknown')} approach")
    print(f"Facts extracted: {len(result.get('facts', []))}")
    print(f"Contexts: {len(result.get('contexts', {}))}")
    print(f"Units: {len(result.get('units', {}))}")
    
    # Generate and save LLM format
    print("\nGenerating LLM format...")
    llm_content = generate_llm_format(result, filing_metadata)
    
    save_result = save_llm_format(llm_content, filing_metadata)
    if "error" in save_result:
        print(f"Error saving LLM format: {save_result['error']}")
    else:
        print(f"LLM format saved to: {save_result.get('file_path')}")
        print(f"Size: {save_result.get('size', 0):,} bytes")
    
    return True

def main():
    """Test the enhanced processor with both Google and Apple filings"""
    
    # Test with Google filing
    google_success = test_filing_download(
        ticker="GOOGL",
        cik="1652044",
        accession_number="000165204425-000014",
        filing_type="10-K",
        period_end="2023-12-31"
    )
    
    time.sleep(2)  # Delay between tests
    
    # Test with Apple filing
    apple_success = test_filing_download(
        ticker="AAPL",
        cik="0000320193",
        filing_type="10-K",
        period_end="2023-09-30"
    )
    
    # Print summary
    print("\n===== TEST SUMMARY =====")
    print(f"Google filing: {'SUCCESS' if google_success else 'FAILED'}")
    print(f"Apple filing: {'SUCCESS' if apple_success else 'FAILED'}")

if __name__ == "__main__":
    main()