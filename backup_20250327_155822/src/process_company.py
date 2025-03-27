# src/process_company.py
import os
import sys
import json
import time
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.xbrl.xbrl_downloader import download_xbrl_instance
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.xbrl.html_text_extractor import process_html_filing
from src.xbrl.enhanced_processor import process_company_filing
from src.config import FILING_TYPES

def process_company(ticker):
    """Process all specified filings for a company"""
    print(f"Processing company: {ticker}")
    
    # Step 1: Find company filings
    filings_result = find_company_filings(ticker, FILING_TYPES)
    if "error" in filings_result:
        return {"error": filings_result["error"]}
    
    print(f"Found {len(filings_result.get('filings', {}))} filings for {ticker}")
    
    # Use a default company name if none was found
    company_name = filings_result.get("company_name")
    if not company_name:
        company_name = f"{ticker} Inc."  # Fallback company name
        print(f"Using fallback company name: {company_name}")
    
    results = {
        "ticker": ticker,
        "cik": filings_result.get("cik"),
        "company_name": company_name,
        "filings_processed": []
    }
    
    # Step 2: Process each filing
    for filing_type, filing_metadata in filings_result.get("filings", {}).items():
        print(f"Processing {ticker} {filing_type}")
        
        # Add ticker and company name to metadata
        filing_metadata["ticker"] = ticker
        filing_metadata["company_name"] = company_name
        
        # Create a filing result to track both XBRL and HTML processing
        filing_result = {
            "filing_type": filing_type,
            "filing_date": filing_metadata.get("filing_date"),
            "period_end_date": filing_metadata.get("period_end_date"),
            "structured_data": {},
            "text_data": {}
        }
        
        # Process structured financial data (XBRL/iXBRL) using enhanced processor
        print(f"Processing structured financial data for {ticker} {filing_type}")
        try:
            # Use the enhanced processor to handle both XBRL and iXBRL formats
            processor_result = process_company_filing(filing_metadata)
            
            if "error" in processor_result:
                print(f"Error processing financial data for {ticker} {filing_type}: {processor_result['error']}")
                filing_result["structured_data"]["error"] = processor_result["error"]
            else:
                # Generate LLM format
                llm_content = generate_llm_format(processor_result, filing_metadata)
                
                # Save LLM format
                save_result = save_llm_format(llm_content, filing_metadata)
                if "error" in save_result:
                    print(f"Error saving LLM format for {ticker} {filing_type}: {save_result['error']}")
                    filing_result["structured_data"]["error"] = save_result["error"]
                else:
                    filing_result["structured_data"]["success"] = True
                    filing_result["structured_data"]["file_path"] = save_result.get("file_path")
                    filing_result["structured_data"]["size"] = save_result.get("size")
                    filing_result["structured_data"]["processing_path"] = processor_result.get("processing_path", "unknown")
                    filing_result["structured_data"]["facts_count"] = len(processor_result.get("facts", []))
                    print(f"Successfully processed financial data for {ticker} {filing_type} using {processor_result.get('processing_path', 'unknown')} path")
                    print(f"Extracted {len(processor_result.get('facts', []))} facts")
        except Exception as e:
            error_msg = f"Exception processing financial data for {ticker} {filing_type}: {str(e)}"
            print(error_msg)
            filing_result["structured_data"]["error"] = error_msg
        
        # Process HTML filing - Raw text data
        print(f"Processing HTML text for {ticker} {filing_type}")
        try:
            html_result = process_html_filing(filing_metadata)
            if "error" in html_result:
                print(f"Error processing HTML for {ticker} {filing_type}: {html_result['error']}")
                filing_result["text_data"]["error"] = html_result["error"]
            else:
                filing_result["text_data"]["success"] = True
                filing_result["text_data"]["files_saved"] = html_result.get("files_saved", {})
                print(f"Successfully processed HTML text for {ticker} {filing_type}")
        except Exception as e:
            error_msg = f"Exception processing HTML for {ticker} {filing_type}: {str(e)}"
            print(error_msg)
            filing_result["text_data"]["error"] = error_msg
        
        # Add filing result to the overall results
        results["filings_processed"].append(filing_result)
        
        # Rate limiting between filings
        time.sleep(1)
    
    return results