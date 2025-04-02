#\!/usr/bin/env python3
"""
Test script to directly test LLMFormatter class without going through the pipeline
"""

import os
import sys
import json
from src2.formatter.llm_formatter import LLMFormatter

def test_formatter_directly():
    print("Testing LLMFormatter class directly")
    
    # Find an XBRL file to use
    sample_paths = [
        "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/AAPL/10-Q/000032019322000059/_xbrl_raw.json",
        "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/MSFT/10-Q/000095017024008814/_xbrl_raw.json",
        "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/MSFT/10-K/000095017024087843/_xbrl_raw.json",
        "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/NVDA/10-K/000104581023000017/_xbrl_raw.json",
        "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/NVDA/10-Q/000104581022000147/_xbrl_raw.json",
        "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/NVDA/10-Q/000104581022000166/_xbrl_raw.json"
    ]
    
    # Find all files that exist (prefer NVDA for testing)
    nvda_file = None
    other_file = None
    
    for path in sample_paths:
        if os.path.exists(path):
            if 'NVDA' in path and not nvda_file:
                nvda_file = path
            elif not other_file:
                other_file = path
    
    # Prioritize NVDA files for this test
    xbrl_file = nvda_file if nvda_file else other_file
    
    if not xbrl_file:
        print("No XBRL file found to test with")
        return
    
    print(f"Using XBRL file: {xbrl_file}")
    
    # Load XBRL data
    with open(xbrl_file, 'r') as f:
        xbrl_data = json.load(f)
    
    # Handle list vs dictionary input format
    if isinstance(xbrl_data, list):
        print("Converting list to expected dictionary format")
        facts = xbrl_data
        xbrl_data = {
            "contexts": {},
            "facts": facts,
            "units": {}
        }
    
    # Create formatter instance directly
    formatter = LLMFormatter()
    
    # Create metadata
    ticker = "TEST"
    filing_type = "10-K"
    if "AAPL" in xbrl_file:
        ticker = "AAPL"
    elif "MSFT" in xbrl_file:
        ticker = "MSFT"
    elif "NVDA" in xbrl_file:
        ticker = "NVDA"
    
    if "10-Q" in xbrl_file:
        filing_type = "10-Q"
    
    metadata = {
        "ticker": ticker,
        "filing_type": filing_type,
        "fiscal_year": "2024",
        "fiscal_period": "FY" if filing_type == "10-K" else "Q1"
    }
    
    # Generate LLM format directly
    print("Generating LLM format")
    formatted_output = formatter.generate_llm_format(xbrl_data, metadata)
    
    # Save output to a file
    output_file = "/Users/michael/NativeLLM/direct_formatter_output.txt"
    with open(output_file, 'w') as f:
        f.write(formatted_output)
    
    print(f"Saved output to {output_file}")
    
    # Check for context reference guide
    context_guide_present = "@CONTEXT_REFERENCE_GUIDE" in formatted_output
    print(f"Context reference guide present: {context_guide_present}")
    
    # Search for NVDA context handling if it's an NVDA file
    if 'NVDA' in xbrl_file:
        print("\nChecking for NVDA context format handling:")
        # Look for the NVDA pattern in the formatted output
        nvda_pattern = "i[a-z0-9]+_D"
        if nvda_pattern in formatted_output:
            print(f"✓ Found NVDA context format pattern '{nvda_pattern}' in output")
            
            # Check for date extraction
            if "@DATE_TYPE: Duration" in formatted_output and "@START_DATE:" in formatted_output:
                print("✓ Found duration date information extracted from NVDA contexts")
            else:
                print("✗ Duration date information missing for NVDA contexts")
                
            # Find and print a few examples
            print("\nSample NVDA context extractions:")
            lines = formatted_output.split("\n")
            context_found = False
            for i, line in enumerate(lines):
                if "@CONTEXT_REF: i" in line and "_D" in line:
                    context_found = True
                    # Print the context and a few lines after it to show date extraction
                    for j in range(i, min(i+5, len(lines))):
                        print(lines[j])
                    break
            
            if not context_found:
                print("No NVDA context examples found in output")
    
    # If present, print the first few lines of the reference guide
    if context_guide_present:
        guide_section = formatted_output.split("@CONTEXT_REFERENCE_GUIDE")[1].split("@")[0]
        print("\nContext reference guide sample:")
        first_few_lines = guide_section.strip().split("\n")[:10]
        for line in first_few_lines:
            print(line)
    else:
        print("\nWARNING: Context reference guide not found in output\!")
    
    print("\nDone testing formatter directly")

if __name__ == "__main__":
    test_formatter_directly()
