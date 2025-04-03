#!/usr/bin/env python3
"""
Test script to verify that our context format handler works with MSFT context formats
"""

import os
import sys
import json
import logging
from pathlib import Path
from src2.formatter.context_format_handler import extract_period_info

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# File paths
xbrl_path = Path("/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads/MSFT/10-K/000156459022026876/_xbrl_raw.json")

def test_msft_context_handling():
    """Test that our context format handler works with MSFT context formats"""
    
    print(f"Testing MSFT context handling with file: {xbrl_path}")
    
    # Check if file exists
    if not xbrl_path.exists():
        print(f"Error: XBRL file not found: {xbrl_path}")
        return
    
    # Load the XBRL data
    with open(xbrl_path, 'r', encoding='utf-8') as f:
        xbrl_data = json.load(f)
    
    # Extract unique context references
    context_refs = set()
    for fact in xbrl_data:
        context_ref = fact.get("contextRef")
        if context_ref:
            context_refs.add(context_ref)
    
    print(f"Found {len(context_refs)} unique context references")
    
    # Test our context format handler on each context reference
    successful_extractions = 0
    failed_extractions = 0
    
    for context_ref in sorted(context_refs)[:20]:  # Test the first 20 for brevity
        period_info = extract_period_info(context_ref)
        
        if period_info:
            print(f"✓ Successfully extracted period info from {context_ref}: {period_info}")
            successful_extractions += 1
        else:
            print(f"✗ Failed to extract period info from {context_ref}")
            failed_extractions += 1
    
    # Print summary
    print(f"\nTest summary: {successful_extractions} successful, {failed_extractions} failed")
    
    if successful_extractions > 0:
        success_rate = (successful_extractions / (successful_extractions + failed_extractions)) * 100
        print(f"Success rate: {success_rate:.2f}%")
    
    # If we have failures, let's add a new format handler for MSFT
    if failed_extractions > 0:
        print("\nAdding a new format handler for MSFT context references...")
        
        # Register a new format handler for MSFT
        from src2.formatter.context_format_handler import register_format_handler
        
        def handle_msft_context(context_ref):
            """Handle MSFT context references like C_0000789019_20210701_20220630"""
            match = None
            
            # Format: C_0000789019_20210701_20220630 (duration with CIK)
            duration_match = re.search(r'C_\d+_(\d{8})_(\d{8})', context_ref)
            if duration_match:
                start_date_str = duration_match.group(1)
                end_date_str = duration_match.group(2)
                
                formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                
                return {
                    "startDate": formatted_start,
                    "endDate": formatted_end
                }
            
            # Format: C_0000789019_20210701 (instant with CIK)
            instant_match = re.search(r'C_\d+_(\d{8})$', context_ref)
            if instant_match:
                date_str = instant_match.group(1)
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                return {
                    "instant": formatted_date
                }
            
            return None
        
        # This is just for demonstration - our handler already supports these formats
        register_format_handler("MSFT_Context", "MSFT context format (C_0000789019_20210701_20220630)", handle_msft_context)
        
        # Test again with the new handler
        print("\nTesting again with the new handler...")
        successful_extractions = 0
        failed_extractions = 0
        
        for context_ref in sorted(context_refs)[:20]:  # Test the first 20 for brevity
            period_info = extract_period_info(context_ref)
            
            if period_info:
                print(f"✓ Successfully extracted period info from {context_ref}: {period_info}")
                successful_extractions += 1
            else:
                print(f"✗ Failed to extract period info from {context_ref}")
                failed_extractions += 1
        
        # Print summary
        print(f"\nTest summary after adding new handler: {successful_extractions} successful, {failed_extractions} failed")
        
        if successful_extractions > 0:
            success_rate = (successful_extractions / (successful_extractions + failed_extractions)) * 100
            print(f"Success rate: {success_rate:.2f}%")

if __name__ == "__main__":
    import re  # Import re here for the new handler
    test_msft_context_handling()
