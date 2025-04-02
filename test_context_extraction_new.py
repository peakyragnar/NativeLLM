#!/usr/bin/env python3
"""
Test Context Extraction Module

This script tests the new context extraction module on SEC filing HTML content.
"""

import os
import sys
import logging
import json
from pprint import pprint

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Import our context extractor
from src2.formatter.context_extractor import (
    extract_contexts_from_html,
    map_contexts_to_periods,
    generate_context_dictionary
)

def test_context_extraction_on_file(html_file, output_file=None):
    """
    Test context extraction on a single HTML file
    
    Args:
        html_file (str): Path to HTML file
        output_file (str, optional): Path to save JSON output
    """
    logging.info(f"Testing context extraction on {html_file}")
    
    try:
        # Read HTML file
        with open(html_file, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
        
        # Get filing metadata from filename
        filename = os.path.basename(html_file)
        parts = filename.split('-')
        
        # Try to extract ticker and filing type
        ticker = parts[0] if len(parts) > 0 else "UNKNOWN"
        filing_type = parts[1] if len(parts) > 1 else "10-K"
        
        # Create basic metadata
        metadata = {
            "ticker": ticker,
            "filing_type": filing_type,
            "fiscal_year": "2023"  # Default
        }
        
        # Extract contexts
        logging.info("Extracting contexts from HTML...")
        contexts = extract_contexts_from_html(html_content, metadata)
        logging.info(f"Found {len(contexts)} contexts")
        
        # Map contexts to human-readable periods
        logging.info("Mapping contexts to periods...")
        context_map = map_contexts_to_periods(contexts, metadata)
        logging.info(f"Mapped {len(context_map)} contexts to periods")
        
        # Generate context dictionary section
        logging.info("Generating context dictionary section...")
        context_dict_lines = generate_context_dictionary(contexts)
        
        # Print some sample contexts
        logging.info("Sample contexts:")
        sample_size = min(5, len(contexts))
        for i, (context_id, context_data) in enumerate(list(contexts.items())[:sample_size]):
            print(f"\nContext {i+1}:")
            print(f"  ID: {context_id}")
            print(f"  Type: {context_data.get('type', 'unknown')}")
            
            if "period" in context_data:
                period = context_data["period"]
                if "instant" in period:
                    print(f"  Instant: {period['instant']}")
                elif "startDate" in period and "endDate" in period:
                    print(f"  Period: {period['startDate']} to {period['endDate']}")
            
            if "fiscal_period" in context_data and "fiscal_year" in context_data:
                print(f"  Fiscal: {context_data['fiscal_year']} {context_data['fiscal_period']}")
            
            if context_id in context_map:
                print(f"  Mapped Label: {context_map[context_id]}")
        
        # Print sample of context dictionary
        print("\nSample of context dictionary section:")
        for line in context_dict_lines[:15]:
            print(line)
        
        # Save results if output file specified
        if output_file:
            output = {
                "contexts": contexts,
                "context_map": context_map,
                "context_dictionary": context_dict_lines
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            logging.info(f"Saved results to {output_file}")
        
        return contexts, context_map, context_dict_lines
        
    except Exception as e:
        logging.error(f"Error testing context extraction: {str(e)}")
        raise

def find_html_files(base_dir="sec_processed", ticker=None):
    """
    Find HTML files to test context extraction on
    
    Args:
        base_dir (str): Base directory to search
        ticker (str, optional): Ticker to filter by
    
    Returns:
        list: List of HTML file paths
    """
    html_files = []
    
    # Look for HTML files in the base directory
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".htm") or file.endswith(".html"):
                # Filter by ticker if specified
                if ticker is None or ticker.upper() in file.upper():
                    html_files.append(os.path.join(root, file))
    
    return html_files

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test context extraction module")
    parser.add_argument("--file", help="Path to HTML file to test")
    parser.add_argument("--output", help="Path to save JSON output")
    parser.add_argument("--ticker", help="Ticker to filter by when finding files")
    parser.add_argument("--dir", default="sec_processed", help="Base directory to search for HTML files")
    
    args = parser.parse_args()
    
    if args.file:
        # Test on a specific file
        test_context_extraction_on_file(args.file, args.output)
    else:
        # Find HTML files and test on the first one
        html_files = find_html_files(args.dir, args.ticker)
        
        if not html_files:
            logging.error(f"No HTML files found in {args.dir}" + 
                         (f" for ticker {args.ticker}" if args.ticker else ""))
            return
        
        logging.info(f"Found {len(html_files)} HTML files")
        print("First 5 files found:")
        for i, file in enumerate(html_files[:5]):
            print(f"  {i+1}. {file}")
        
        # Test on the first file
        test_context_extraction_on_file(html_files[0], args.output)

if __name__ == "__main__":
    main() 