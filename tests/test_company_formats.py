"""
Test script for company-specific XBRL format handling.
This validates that the company formats registry is working correctly.
"""

import os
import sys
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_company_formats.log'),
        logging.StreamHandler()
    ]
)

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.company_formats import get_all_company_formats, register_company_format
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.xbrl.xbrl_downloader import download_xbrl_instance
from src.xbrl.xbrl_parser import parse_xbrl_file

def test_company_format(ticker, download_latest=False, override_format=None):
    """
    Test XBRL parsing for a specific company
    
    Args:
        ticker: Company ticker symbol
        download_latest: Whether to download the latest filing
        override_format: Override the format detection
        
    Returns:
        Test results dictionary
    """
    logging.info(f"Testing company format for {ticker}")
    
    # Get the currently registered format for this company
    formats = get_all_company_formats()
    company_format = formats.get(ticker, {"default_format": "standard"})
    
    logging.info(f"Current registered format: {company_format}")
    
    # If requested, download the latest 10-Q filing
    if download_latest:
        logging.info(f"Downloading latest 10-Q for {ticker}")
        
        # Get company metadata
        cik = get_cik_from_ticker(ticker)
        company_name = get_company_name_from_cik(cik)
        
        # Find latest 10-Q filing
        filings_result = find_company_filings(ticker, ["10-Q"])
        
        if "error" in filings_result:
            return {"error": filings_result["error"]}
            
        if "filings" in filings_result and "10-Q" in filings_result["filings"]:
            filing_metadata = filings_result["filings"]["10-Q"]
            filing_metadata["ticker"] = ticker
            filing_metadata["company_name"] = company_name
            
            # Download XBRL file
            download_result = download_xbrl_instance(filing_metadata)
            
            if "error" in download_result:
                return {"error": download_result["error"]}
                
            xbrl_file_path = download_result.get("file_path")
            
            # Test parsing with our format system
            parsed_result = parse_xbrl_file(xbrl_file_path, ticker=ticker, filing_metadata=filing_metadata)
            
            # Return complete test results
            return {
                "ticker": ticker,
                "company_format": company_format,
                "filing_metadata": filing_metadata,
                "xbrl_file": xbrl_file_path,
                "format_used": parsed_result.get("xbrl_format"),
                "success": parsed_result.get("success", False),
                "facts_count": parsed_result.get("facts_count", 0),
                "contexts_count": len(parsed_result.get("contexts", {})),
                "parse_results": parsed_result
            }
        else:
            return {"error": f"No 10-Q filings found for {ticker}"}
    
    # If we're just testing the format registry
    return {
        "ticker": ticker,
        "company_format": company_format
    }

def add_company_format(ticker, default_format="standard", format_patterns=None):
    """
    Add a new company format to the registry
    
    Args:
        ticker: Company ticker symbol
        default_format: Default format type
        format_patterns: Dictionary mapping file patterns to format types
        
    Returns:
        True if successful
    """
    format_info = {
        "default_format": default_format,
        "format_patterns": format_patterns or {}
    }
    
    result = register_company_format(ticker, format_info)
    logging.info(f"Added format for {ticker}: {result}")
    
    return result

def test_all_companies(companies=None):
    """
    Test formats for all registered companies or a specified list
    
    Args:
        companies: List of company tickers to test
        
    Returns:
        Dictionary with test results
    """
    # Get all registered companies
    formats = get_all_company_formats()
    
    # Use provided list or all registered companies
    test_tickers = companies or list(formats.keys())
    
    results = {}
    for ticker in test_tickers:
        results[ticker] = test_company_format(ticker)
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test company XBRL format handling")
    parser.add_argument("--ticker", help="Test a specific company ticker")
    parser.add_argument("--download", action="store_true", help="Download and parse the latest filing")
    parser.add_argument("--add", help="Add a company with format type (e.g., 'MSFT:standard')")
    parser.add_argument("--all", action="store_true", help="Test all registered companies")
    
    args = parser.parse_args()
    
    if args.add and ":" in args.add:
        ticker, format_type = args.add.split(":", 1)
        add_company_format(ticker, default_format=format_type)
    elif args.ticker:
        result = test_company_format(args.ticker, download_latest=args.download)
        print(f"Test result for {args.ticker}:")
        print(f"  Registered format: {result.get('company_format', {}).get('default_format', 'standard')}")
        if 'format_used' in result:
            print(f"  Format detected: {result.get('format_used')}")
            print(f"  Success: {result.get('success', False)}")
            print(f"  Facts count: {result.get('facts_count', 0)}")
            print(f"  Contexts count: {result.get('contexts_count', 0)}")
    elif args.all:
        results = test_all_companies()
        print(f"Tested {len(results)} companies")
        for ticker, result in results.items():
            print(f"  {ticker}: {result.get('company_format', {}).get('default_format', 'standard')}")
    else:
        parser.print_help()