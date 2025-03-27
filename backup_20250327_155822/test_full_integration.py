"""
Test script for the full integration of the enhanced XBRL processor.

This script tests the complete pipeline from finding filings to processing
and generating LLM format, ensuring all components work together properly.
"""

import os
import sys
import time

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.edgar.edgar_utils import get_cik_from_ticker
from src.edgar.filing_finder import find_company_filings  
from src.process_company import process_company

def test_process_company(ticker):
    """Test the entire processing pipeline for a company"""
    print(f"\n=== Testing Full Pipeline for {ticker} ===")
    
    # First, find all filings
    print(f"Step 1: Getting CIK for {ticker}")
    cik = get_cik_from_ticker(ticker)
    if not cik:
        print(f"Error: Could not find CIK for {ticker}")
        return False
    
    print(f"Found CIK: {cik}")
    
    # Process the company through the main pipeline
    print(f"\nStep 2: Processing {ticker} through the main pipeline")
    result = process_company(ticker)
    
    # Check for errors
    if "error" in result:
        print(f"Error processing {ticker}: {result['error']}")
        return False
    
    # Print summary of results
    print(f"\n=== Processing Results for {ticker} ===")
    print(f"Company: {result.get('company_name')}")
    print(f"CIK: {result.get('cik')}")
    print(f"Filings processed: {len(result.get('filings_processed', []))}")
    
    for filing in result.get('filings_processed', []):
        filing_type = filing.get('filing_type')
        period_end = filing.get('period_end_date')
        
        print(f"\n  {filing_type} ({period_end}):")
        
        # Structured data results
        structured_data = filing.get('structured_data', {})
        if structured_data.get('success'):
            print(f"    Structured data: SUCCESS")
            print(f"    Processing path: {structured_data.get('processing_path', 'unknown')}")
            print(f"    Facts extracted: {structured_data.get('facts_count', 0)}")
            print(f"    File saved: {structured_data.get('file_path')}")
            print(f"    Size: {structured_data.get('size', 0):,} bytes")
        else:
            print(f"    Structured data: FAILED - {structured_data.get('error', 'Unknown error')}")
        
        # Text data results
        text_data = filing.get('text_data', {})
        if text_data.get('success'):
            print(f"    Text data: SUCCESS")
            files_saved = text_data.get('files_saved', {})
            for file_type, file_info in files_saved.items():
                print(f"      {file_type}: {file_info.get('file_path')} ({file_info.get('size', 0):,} bytes)")
        else:
            print(f"    Text data: FAILED - {text_data.get('error', 'Unknown error')}")
    
    print("\nProcessing complete!")
    return True

def main():
    """Test the full integration with both Google and Apple"""
    
    # Test with Google (known to use iXBRL)
    google_success = test_process_company("GOOGL")
    
    # Add a delay between tests to respect SEC rate limits
    time.sleep(5)
    
    # Test with Apple (known to use traditional XBRL)
    apple_success = test_process_company("AAPL")
    
    # Print final summary
    print("\n\n=== FINAL TEST SUMMARY ===")
    print(f"Google (GOOGL): {'SUCCESS' if google_success else 'FAILED'}")
    print(f"Apple (AAPL): {'SUCCESS' if apple_success else 'FAILED'}")
    
    if google_success and apple_success:
        print("\nAll tests passed! The enhanced XBRL processor is fully integrated and working correctly.")
    else:
        print("\nSome tests failed. Please check the logs for more information.")

if __name__ == "__main__":
    main()