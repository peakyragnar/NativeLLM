"""
Test script for adaptive XBRL processing system.
This demonstrates the company-specific format handling capabilities.
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
        logging.FileHandler('test_adaptive_xbrl.log'),
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
from src.formatter.llm_formatter import generate_llm_format, save_llm_format

def test_process_filing(ticker, filing_type="10-Q"):
    """
    Test the full processing pipeline for a company filing
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (default: 10-Q)
        
    Returns:
        Test results dictionary
    """
    logging.info(f"Testing full pipeline for {ticker} {filing_type}")
    
    # Get company info
    cik = get_cik_from_ticker(ticker)
    company_name = get_company_name_from_cik(cik)
    
    logging.info(f"Company: {company_name} (CIK: {cik})")
    
    # Find latest filing
    filings_result = find_company_filings(ticker, [filing_type])
    
    if "error" in filings_result:
        return {"error": filings_result["error"]}
        
    if "filings" not in filings_result or filing_type not in filings_result["filings"]:
        return {"error": f"No {filing_type} filings found for {ticker}"}
        
    filing_metadata = filings_result["filings"][filing_type]
    filing_metadata["ticker"] = ticker
    filing_metadata["company_name"] = company_name
    
    # Ensure fiscal year and period are set (for testing)
    if "period_end_date" in filing_metadata:
        period_end = filing_metadata["period_end_date"]
        if period_end and "-" in period_end:
            filing_metadata["fiscal_year"] = period_end.split("-")[0]
            
            # Set quarter based on month
            try:
                month = int(period_end.split("-")[1])
                quarter_map = {1: "Q1", 2: "Q1", 3: "Q1", 4: "Q2", 5: "Q2", 6: "Q2", 
                              7: "Q3", 8: "Q3", 9: "Q3", 10: "Q4", 11: "Q4", 12: "Q4"}
                filing_metadata["fiscal_period"] = quarter_map.get(month, "Q1")
            except:
                filing_metadata["fiscal_period"] = "Q1"
    
    # Download XBRL
    download_result = download_xbrl_instance(filing_metadata)
    
    if "error" in download_result:
        return {"error": f"Error downloading XBRL: {download_result['error']}"}
        
    xbrl_file_path = download_result.get("file_path")
    logging.info(f"Downloaded XBRL to: {xbrl_file_path}")
    
    # Parse XBRL
    parsed_result = parse_xbrl_file(xbrl_file_path, ticker=ticker, filing_metadata=filing_metadata)
    
    if "error" in parsed_result:
        return {"error": f"Error parsing XBRL: {parsed_result['error']}"}
    
    xbrl_format = parsed_result.get("xbrl_format", "unknown")
    facts_count = len(parsed_result.get("facts", []))
    contexts_count = len(parsed_result.get("contexts", {}))
    
    logging.info(f"Parsed XBRL using format: {xbrl_format}")
    logging.info(f"Found {facts_count} facts and {contexts_count} contexts")
    
    # Generate LLM format
    try:
        llm_content = generate_llm_format(parsed_result, filing_metadata)
        llm_size = len(llm_content) if llm_content else 0
        
        logging.info(f"Generated LLM format: {llm_size} bytes")
        
        # Save to file if content was generated
        if llm_content:
            save_result = save_llm_format(llm_content, filing_metadata)
            local_file_path = save_result.get("file_path")
            logging.info(f"Saved LLM format to: {local_file_path}")
        else:
            local_file_path = None
            logging.warning("No LLM content generated")
            
        # Return results
        return {
            "ticker": ticker,
            "filing_type": filing_type,
            "xbrl_format": xbrl_format,
            "facts_count": facts_count,
            "contexts_count": contexts_count,
            "llm_size": llm_size,
            "llm_file_path": local_file_path,
            "success": facts_count > 0 and llm_size > 0
        }
    except Exception as e:
        logging.error(f"Error generating LLM format: {str(e)}")
        return {
            "ticker": ticker,
            "filing_type": filing_type,
            "xbrl_format": xbrl_format,
            "facts_count": facts_count,
            "contexts_count": contexts_count,
            "error": str(e),
            "success": False
        }

def print_company_formats():
    """Print all known company formats from the registry"""
    formats = get_all_company_formats()
    
    print("\nRegistered Company Formats:")
    print("==========================")
    
    for ticker, format_info in sorted(formats.items()):
        print(f"\n{ticker}:")
        print(f"  Default format: {format_info.get('default_format', 'standard')}")
        
        if 'format_patterns' in format_info and format_info['format_patterns']:
            print("  Format patterns:")
            for pattern, format_type in format_info['format_patterns'].items():
                print(f"    {pattern} → {format_type}")
        else:
            print("  No format patterns defined")

def test_multiple_companies(tickers):
    """
    Test the adaptive XBRL parser on multiple companies
    
    Args:
        tickers: List of company ticker symbols
        
    Returns:
        Dictionary with results for each company
    """
    results = {}
    
    for ticker in tickers:
        print(f"\nTesting {ticker}...")
        result = test_process_filing(ticker)
        results[ticker] = result
        
        if "error" in result:
            print(f"❌ Error: {result['error']}")
        elif result.get("success", False):
            print(f"✅ Success: {result['facts_count']} facts, {result['llm_size']} bytes")
        else:
            print(f"⚠️ Partial success: {result['facts_count']} facts")
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the adaptive XBRL processing system")
    parser.add_argument("--ticker", help="Process a specific company ticker")
    parser.add_argument("--all", action="store_true", help="Test all companies in the format registry")
    parser.add_argument("--show-formats", action="store_true", help="Show all registered company formats")
    
    args = parser.parse_args()
    
    if args.show_formats:
        print_company_formats()
    elif args.ticker:
        result = test_process_filing(args.ticker)
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(f"Results for {args.ticker}:")
            print(f"  XBRL Format: {result.get('xbrl_format', 'unknown')}")
            print(f"  Facts: {result.get('facts_count', 0)}")
            print(f"  Contexts: {result.get('contexts_count', 0)}")
            print(f"  LLM Size: {result.get('llm_size', 0)} bytes")
            print(f"  Success: {result.get('success', False)}")
            if "llm_file_path" in result and result["llm_file_path"]:
                print(f"  LLM File: {result['llm_file_path']}")
    elif args.all:
        formats = get_all_company_formats()
        test_multiple_companies(list(formats.keys()))
    else:
        # Default test case
        test_tickers = ["AAPL", "MSFT", "GOOGL"]
        print(f"Testing {len(test_tickers)} companies: {', '.join(test_tickers)}")
        test_multiple_companies(test_tickers)