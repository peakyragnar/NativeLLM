# src/process_company.py
import os
import sys
import json
import time

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.xbrl.xbrl_downloader import download_xbrl_instance
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import FILING_TYPES

def process_company(ticker):
    """Process all specified filings for a company"""
    print(f"Processing company: {ticker}")
    
    # Step 1: Find company filings
    filings_result = find_company_filings(ticker, FILING_TYPES)
    if "error" in filings_result:
        return {"error": filings_result["error"]}
    
    print(f"Found {len(filings_result.get('filings', {}))} filings for {ticker}")
    
    results = {
        "ticker": ticker,
        "cik": filings_result.get("cik"),
        "company_name": filings_result.get("company_name"),
        "filings_processed": []
    }
    
    # Step 2: Process each filing
    for filing_type, filing_metadata in filings_result.get("filings", {}).items():
        print(f"Processing {ticker} {filing_type}")
        
        # Add ticker and company name to metadata
        filing_metadata["ticker"] = ticker
        filing_metadata["company_name"] = filings_result.get("company_name")
        
        # Download XBRL instance
        download_result = download_xbrl_instance(filing_metadata)
        if "error" in download_result:
            print(f"Error downloading XBRL for {ticker} {filing_type}: {download_result['error']}")
            continue
        
        file_path = download_result.get("file_path")
        
        # Parse XBRL
        parsed_result = parse_xbrl_file(file_path)
        if "error" in parsed_result:
            print(f"Error parsing XBRL for {ticker} {filing_type}: {parsed_result['error']}")
            continue
        
        # Generate LLM format
        llm_content = generate_llm_format(parsed_result, filing_metadata)
        
        # Save LLM format
        save_result = save_llm_format(llm_content, filing_metadata)
        if "error" in save_result:
            print(f"Error saving LLM format for {ticker} {filing_type}: {save_result['error']}")
            continue
        
        results["filings_processed"].append({
            "filing_type": filing_type,
            "filing_date": filing_metadata.get("filing_date"),
            "period_end_date": filing_metadata.get("period_end_date"),
            "llm_file_path": save_result.get("file_path")
        })
        
        print(f"Successfully processed {ticker} {filing_type}")
        
        # Rate limiting
        time.sleep(1)
    
    return results