#!/usr/bin/env python
# Run a test processing a real file with our HTML extraction enhancements

import sys
import os
import logging
import time
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

def run_test_on_file(file_path):
    """Run a test on a file to demonstrate the file size reduction"""
    try:
        from src.xbrl.xbrl_parser import parse_xbrl_file
        from src.formatter.llm_formatter import generate_llm_format
        
        # Create test metadata
        metadata = {
            "ticker": "TEST",
            "filing_type": "10-K",
            "company_name": "Test Company",
            "filing_date": "2025-03-25",
            "period_end_date": "2025-03-25",
            "cik": "0000123456"
        }
        
        # Parse the XBRL file
        logging.info(f"Parsing XBRL file: {file_path}")
        start_time = time.time()
        parsed = parse_xbrl_file(file_path, ticker="TEST", filing_metadata=metadata)
        parse_time = time.time() - start_time
        
        # Check if parsing was successful
        if "error" in parsed:
            logging.error(f"Error parsing file: {parsed['error']}")
            return False
            
        logging.info(f"Successfully parsed file in {parse_time:.2f} seconds")
        logging.info(f"Extracted {len(parsed.get('contexts', {}))} contexts")
        logging.info(f"Extracted {len(parsed.get('units', {}))} units")
        logging.info(f"Extracted {len(parsed.get('facts', []))} facts")
        
        # Generate LLM format
        logging.info("Generating LLM format...")
        start_time = time.time()
        llm_content = generate_llm_format(parsed, metadata)
        format_time = time.time() - start_time
        
        # Save to test output
        test_output_file = os.path.join(os.path.dirname(file_path), "test_output_html_cleaned.txt")
        with open(test_output_file, 'w', encoding='utf-8') as f:
            f.write(llm_content)
            
        # Get sizes for comparison
        test_size = os.path.getsize(test_output_file)
        original_size = os.path.getsize(file_path)
        
        logging.info(f"Generated LLM format in {format_time:.2f} seconds")
        logging.info(f"Original file size: {original_size:,} bytes")
        logging.info(f"New file size: {test_size:,} bytes")
        
        if original_size > 0:
            reduction = (original_size - test_size) / original_size * 100
            logging.info(f"Size reduction: {reduction:.1f}%")
            
        logging.info(f"Test output saved to: {test_output_file}")
        return True
        
    except Exception as e:
        logging.error(f"Error running test: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Check if file path is provided as an argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # Find a file in data directory to test with
        data_dir = "/Users/michael/NativeLLM/data"
        test_file = None
        
        # Look for any xml files to use
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith(".xml"):
                    test_file = os.path.join(root, file)
                    break
            if test_file:
                break
                
        if not test_file:
            logging.error("No test file found. Please provide a file path.")
            sys.exit(1)
            
        file_path = test_file
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        sys.exit(1)
        
    success = run_test_on_file(file_path)
    sys.exit(0 if success else 1)