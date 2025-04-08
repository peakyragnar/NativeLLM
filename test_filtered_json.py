#!/usr/bin/env python3
"""
Test Filtered JSON Format

This script tests the filtered JSON output format for SEC filings.
"""

import os
import sys
import logging
import argparse
import json
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the filtered JSON formatter
from src2.formatter.filtered_json_formatter import filtered_json_formatter

def main():
    """
    Main function to test filtered JSON output format
    """
    parser = argparse.ArgumentParser(description="Test filtered JSON output format for SEC filings")
    parser.add_argument("--html-path", required=True, help="Path to HTML/XBRL document")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    parser.add_argument("--ticker", default="UNKNOWN", help="Ticker symbol")
    parser.add_argument("--filing-type", default="UNKNOWN", help="Filing type")
    parser.add_argument("--year", default="UNKNOWN", help="Fiscal year")

    args = parser.parse_args()

    # Create metadata
    metadata = {
        "ticker": args.ticker,
        "filing_type": args.filing_type,
        "fiscal_year": args.year
    }

    # Generate filtered JSON format
    json_data = filtered_json_formatter.generate_json_format(args.html_path, metadata)

    # Save filtered JSON format
    save_result = filtered_json_formatter.save_json_format(json_data, args.output)

    if save_result.get("success", False):
        logging.info(f"Successfully saved filtered JSON format to {args.output}")
        
        # Print JSON structure summary
        print(f"\nFiltered JSON file size: {os.path.getsize(args.output) / 1024:.2f} KB")
        print(f"JSON structure:")
        print(f"  - metadata: {list(json_data.get('metadata', {}).keys())}")
        
        # Print narrative sections
        sections = json_data.get('document', {}).get('sections', [])
        print(f"  - narrative sections: {len(sections)}")
        for i, section in enumerate(sections[:5]):
            print(f"    * {i+1}: {section.get('id', 'Unknown')} ({len(section.get('content', ''))} chars)")
        if len(sections) > 5:
            print(f"    * ... and {len(sections) - 5} more sections")
        
        # Print financial statements
        statements = json_data.get('financial_data', {}).get('statements', [])
        print(f"  - financial statements: {len(statements)}")
        for i, statement in enumerate(statements):
            print(f"    * {i+1}: {statement.get('type', 'Unknown')}")
            print(f"      - Contexts: {len(statement.get('contexts', []))}")
            print(f"      - Facts: {len(statement.get('facts', []))}")
            
            # Print sections
            sections = statement.get('sections', [])
            print(f"      - Sections: {len(sections)}")
            for j, section in enumerate(sections[:3]):
                print(f"        * {j+1}: {section.get('name', 'Unknown')} ({len(section.get('facts', []))} facts)")
            if len(sections) > 3:
                print(f"        * ... and {len(sections) - 3} more sections")
        
        # Print XBRL facts
        facts = json_data.get('xbrl_data', {}).get('facts', [])
        print(f"  - xbrl facts: {len(facts)}")
        for i, fact in enumerate(facts[:5]):
            print(f"    * {i+1}: {fact.get('name', 'Unknown')} = {fact.get('value', '')}")
        if len(facts) > 5:
            print(f"    * ... and {len(facts) - 5} more facts")
        
        # Print data integrity metrics
        print(f"  - data integrity: {json_data.get('data_integrity', {})}")
        
        # Compare with original JSON file if available
        original_path = f"sec_processed/{args.ticker}/{args.ticker}_{args.filing_type}_{args.year}_llm.json"
        if os.path.exists(original_path):
            original_size = os.path.getsize(original_path)
            new_size = os.path.getsize(args.output)
            size_diff = new_size - original_size
            size_diff_percent = (size_diff / original_size) * 100
            print(f"\nFile size comparison:")
            print(f"  - Original JSON: {original_size / 1024:.2f} KB")
            print(f"  - Filtered JSON: {new_size / 1024:.2f} KB")
            print(f"  - Difference: {size_diff / 1024:.2f} KB ({size_diff_percent:.2f}%)")
    else:
        logging.error(f"Error saving filtered JSON format: {save_result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()
