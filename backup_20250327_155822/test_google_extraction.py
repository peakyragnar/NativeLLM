"""
Test script to extract and format data from the Google iXBRL filing.
"""

import os
import sys
import json
from pprint import pprint

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.xbrl.ixbrl_extractor import process_ixbrl_file
from src.formatter.ixbrl_formatter import generate_llm_format_from_ixbrl, save_llm_format

def test_google_ixbrl_extraction():
    """Test extracting data from the Google iXBRL filing"""
    
    # Use Google's metadata
    filing_metadata = {
        "ticker": "GOOGL",
        "cik": "1652044",
        "company_name": "Alphabet Inc.",
        "accession_number": "000165204425-000014",
        "filing_type": "10-K",
        "filing_date": "2024-01-30",
        "period_end_date": "2023-12-31"
    }
    
    # Path to the downloaded Google filing
    file_path = "/Users/michael/NativeLLM/data/raw/GOOGL/10-K/000165204425-000014-goog-20241231.htm"
    
    print(f"Testing extraction from: {file_path}")
    
    # Process the file to extract iXBRL data
    extracted_data = process_ixbrl_file(file_path, filing_metadata)
    
    if "error" in extracted_data:
        print(f"Error extracting data: {extracted_data['error']}")
        return False
    
    print(f"Successfully extracted data:")
    print(f"- Contexts: {extracted_data.get('context_count', 0)}")
    print(f"- Units: {extracted_data.get('unit_count', 0)}")
    print(f"- Facts: {extracted_data.get('fact_count', 0)}")
    
    # Generate LLM format
    print("\nGenerating LLM format...")
    llm_content = generate_llm_format_from_ixbrl(extracted_data, filing_metadata)
    
    # Save the LLM format file
    save_result = save_llm_format(llm_content, filing_metadata)
    
    if "error" in save_result:
        print(f"Error saving LLM format: {save_result['error']}")
        return False
    
    print(f"Saved LLM format to: {save_result.get('file_path')}")
    print(f"File size: {save_result.get('size', 0):,} bytes")
    
    # Print a sample of the LLM format
    print("\nSample of LLM format:")
    print(llm_content[:1000] + "...\n")
    
    return True

if __name__ == "__main__":
    success = test_google_ixbrl_extraction()
    print(f"\nTest {'successful' if success else 'failed'}")