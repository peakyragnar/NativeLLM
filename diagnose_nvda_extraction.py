#!/usr/bin/env python3
"""
Diagnostic tool for NVDA extraction issues
This script attempts to diagnose why NVDA filings are failing during extraction
"""

import os
import sys
import logging
import traceback
import asyncio
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import necessary modules
try:
    from src2.processor.html_processor import HTMLProcessor
    from src2.processor.xbrl_processor import XBRLProcessor
    from src2.processor.ixbrl_extractor import IXBRLExtractor  # Fixed class name
    from src2.formatter.llm_formatter import LLMFormatter
except ImportError as e:
    logging.error(f"Error importing modules: {e}")
    sys.exit(1)

def check_filing_exists(ticker, filing_type, filing_id):
    """Check if a filing exists in the downloads directory"""
    base_path = Path(f"sec_processed/tmp/sec_downloads/{ticker}/{filing_type}/{filing_id}")
    
    if not base_path.exists():
        logging.error(f"Filing not found: {base_path}")
        return False, None
    
    # Find main document (usually ends with .htm)
    htm_files = list(base_path.glob("*.htm"))
    if not htm_files:
        logging.error(f"No .htm file found in {base_path}")
        return True, None
    
    main_doc = htm_files[0]
    return True, main_doc

def test_ixbrl_extraction(file_path):
    """Test inline XBRL extraction"""
    logging.info(f"Testing iXBRL extraction on {file_path}")
    try:
        extractor = IXBRLExtractor()
        result = asyncio.run(extractor.extract_from_file(str(file_path)))
        
        if result and "facts" in result:
            logging.info(f"✅ Successfully extracted {len(result['facts'])} iXBRL facts")
            return True, result
        else:
            logging.error(f"❌ iXBRL extraction returned no facts")
            return False, result
    except Exception as e:
        logging.error(f"❌ iXBRL extraction failed: {e}")
        traceback.print_exc()
        return False, str(e)

def test_html_processing(file_path):
    """Test HTML processing"""
    logging.info(f"Testing HTML processing on {file_path}")
    try:
        processor = HTMLProcessor()
        result = processor.process_filing(str(file_path))
        
        if result and "content" in result:
            logging.info(f"✅ Successfully processed HTML: {len(result['content'])} characters")
            return True, result
        else:
            logging.error(f"❌ HTML processing returned no content")
            return False, result
    except Exception as e:
        logging.error(f"❌ HTML processing failed: {e}")
        traceback.print_exc()
        return False, str(e)

def test_llm_formatting(html_content=None, xbrl_data=None):
    """Test LLM formatting"""
    logging.info("Testing LLM formatting")
    try:
        formatter = LLMFormatter()
        result = formatter.format_filing(html_content=html_content, xbrl_data=xbrl_data)
        
        if result and "content" in result:
            logging.info(f"✅ Successfully formatted for LLM: {len(result['content'])} characters")
            return True, result
        else:
            logging.error(f"❌ LLM formatting returned no content")
            return False, result
    except Exception as e:
        logging.error(f"❌ LLM formatting failed: {e}")
        traceback.print_exc()
        return False, str(e)

def diagnose_filing(ticker, filing_type, filing_id):
    """Run diagnostic tests on a specific filing"""
    logging.info(f"Diagnosing {ticker} {filing_type} filing: {filing_id}")
    
    # Step 1: Check if filing exists
    exists, file_path = check_filing_exists(ticker, filing_type, filing_id)
    if not exists or not file_path:
        return False
    
    # Step 2: Test iXBRL extraction
    ixbrl_success, ixbrl_data = test_ixbrl_extraction(file_path)
    
    # Step 3: Test HTML processing
    html_success, html_data = test_html_processing(file_path)
    
    # Step 4: Test LLM formatting (only if previous steps succeeded)
    llm_success = False
    if ixbrl_success and html_success:
        html_content = html_data.get("content") if html_success else None
        xbrl_facts = ixbrl_data.get("facts") if ixbrl_success else None
        
        llm_success, llm_data = test_llm_formatting(
            html_content=html_content,
            xbrl_data=xbrl_data
        )
    
    # Summarize results
    logging.info("\n== Diagnostic Summary ==")
    logging.info(f"Filing: {ticker} {filing_type} {filing_id}")
    logging.info(f"iXBRL Extraction: {'✅ Success' if ixbrl_success else '❌ Failed'}")
    logging.info(f"HTML Processing: {'✅ Success' if html_success else '❌ Failed'}")
    logging.info(f"LLM Formatting: {'✅ Success' if llm_success else '❌ Failed'}")
    
    if not (ixbrl_success and html_success and llm_success):
        logging.info("⚠️ Recommendation: Check the specific error messages above")
        
    return ixbrl_success and html_success and llm_success

def main():
    """Main function"""
    # NVDA 10-K and problematic 10-Q filings to test
    filings_to_test = [
        ("NVDA", "10-K", "000104581024000029"),  # 2024 10-K
        ("NVDA", "10-Q", "000104581024000124"),  # 2024 Q1
    ]
    
    # Also test a successful filing for comparison
    successful = [
        ("NVDA", "10-Q", "000104581023000227"),  # 2024 Q3 (works)
    ]
    
    # Run diagnostics
    logging.info("=== Starting Filing Diagnostics ===")
    
    logging.info("\n=== Testing Problematic Filings ===")
    for ticker, filing_type, filing_id in filings_to_test:
        diagnose_filing(ticker, filing_type, filing_id)
        logging.info("---")
    
    logging.info("\n=== Testing Successfully Processed Filings ===")
    for ticker, filing_type, filing_id in successful:
        diagnose_filing(ticker, filing_type, filing_id)
        logging.info("---")

if __name__ == "__main__":
    main()