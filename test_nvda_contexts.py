#!/usr/bin/env python3
"""
Test script specifically for NVDA context extraction
"""

import os
import sys
import json
import re
from src2.formatter.llm_formatter import LLMFormatter

def test_nvda_contexts():
    print("Testing NVDA context pattern extraction")
    
    # Create a new formatter instance
    formatter = LLMFormatter()
    
    # Create a sample context ID in NVDA format
    nvda_context_id = "i2c5e111a942340e08ad1e8d2e3b0fb71_D20210201-20220130"
    
    # Test the pattern directly
    nvda_duration_match = re.search(r'i[a-z0-9]+_D(\d{8})-(\d{8})', nvda_context_id)
    
    if nvda_duration_match:
        start_date_str = nvda_duration_match.group(1)
        end_date_str = nvda_duration_match.group(2)
        
        formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
        formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
        
        print(f"✓ Pattern matched: {nvda_context_id}")
        print(f"  - Start date: {formatted_start}")
        print(f"  - End date: {formatted_end}")
    else:
        print(f"✗ Pattern failed to match: {nvda_context_id}")
    
    # Try with a real NVDA file
    nvda_file = "/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/NVDA/10-K/000104581023000017/_xbrl_raw.json"
    
    if os.path.exists(nvda_file):
        print(f"\nProcessing real NVDA file: {nvda_file}")
        
        with open(nvda_file, 'r') as f:
            data = json.load(f)
        
        # Create test data structure
        if isinstance(data, list):
            facts = data
            xbrl_data = {
                "contexts": {},
                "facts": facts,
                "units": {}
            }
        else:
            xbrl_data = data
        
        # Create metadata
        metadata = {
            "ticker": "NVDA",
            "filing_type": "10-K",
            "fiscal_year": "2023",
            "fiscal_period": "FY"
        }
        
        # Generate LLM format with our new formatter instance
        print("Generating LLM format with new formatter instance")
        formatted_output = formatter.generate_llm_format(xbrl_data, metadata)
        
        # Save output to a file
        output_file = "/Users/michael/NativeLLM/nvda_context_test_output.txt"
        with open(output_file, 'w') as f:
            f.write(formatted_output)
        
        print(f"Saved output to {output_file}")
        
        # Check for NVDA context patterns in output
        nvda_patterns_found = 0
        lines = formatted_output.split("\n")
        for i, line in enumerate(lines):
            if "@CONTEXT_REF: i" in line and "_D" in line:
                nvda_patterns_found += 1
                
                # Check the next few lines for date extraction
                date_type_found = False
                start_date_found = False
                end_date_found = False
                
                for j in range(i+1, min(i+5, len(lines))):
                    if "@DATE_TYPE: Duration" in lines[j]:
                        date_type_found = True
                    if "@START_DATE:" in lines[j]:
                        start_date_found = True
                    if "@END_DATE:" in lines[j]:
                        end_date_found = True
                
                if nvda_patterns_found <= 3:  # Only print the first 3 examples
                    print(f"\nFound NVDA context pattern #{nvda_patterns_found}:")
                    print(line)
                    if date_type_found and start_date_found and end_date_found:
                        print("✓ Date information correctly extracted")
                        # Print a few lines to show the extraction
                        for j in range(i+1, min(i+5, len(lines))):
                            print(lines[j])
                    else:
                        print("✗ Date information NOT correctly extracted")
        
        print(f"\nTotal NVDA context patterns found: {nvda_patterns_found}")
        
        if nvda_patterns_found > 0:
            print("✓ Test successful - NVDA context patterns found and processed")
        else:
            print("✗ Test failed - No NVDA context patterns found in output")
    else:
        print(f"NVDA file not found: {nvda_file}")

if __name__ == "__main__":
    test_nvda_contexts()