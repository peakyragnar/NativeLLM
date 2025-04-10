#!/usr/bin/env python3
"""
Test JSON Formatter

This script tests the JSON output formatter for SEC filings.
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

# Import the JSON output formatter
from src2.formatter.json_output_formatter import json_output_formatter

def main():
    """
    Main function to test JSON output formatter
    """
    parser = argparse.ArgumentParser(description="Test JSON output formatter for SEC filings")
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

    # Generate JSON format
    json_data = json_output_formatter.generate_json_format(args.html_path, metadata)

    # Save JSON format
    save_result = json_output_formatter.save_json_format(json_data, args.output)

    if save_result.get("success", False):
        logging.info(f"Successfully saved JSON format to {args.output}")
        
        # Print JSON structure summary
        print(f"\nJSON file size: {os.path.getsize(args.output) / 1024:.2f} KB")
        print(f"JSON structure:")
        print(f"  - metadata: {list(json_data.get('metadata', {}).keys())}")
        
        # Print narrative sections
        document_sections = json_data.get('content', {}).get('document_sections', {})
        print(f"  - narrative sections: {len(document_sections)}")
        for i, (section_id, section_data) in enumerate(list(document_sections.items())[:5]):
            print(f"    * {i+1}: {section_id} ({len(section_data.get('text', ''))} chars)")
        if len(document_sections) > 5:
            print(f"    * ... and {len(document_sections) - 5} more sections")
        
        # Print XBRL facts
        facts = json_data.get('xbrl_data', {}).get('facts', [])
        print(f"  - xbrl facts: {len(facts)}")
        for i, fact in enumerate(facts[:5]):
            print(f"    * {i+1}: {fact.get('name', 'Unknown')} = {fact.get('value', '')}")
        if len(facts) > 5:
            print(f"    * ... and {len(facts) - 5} more facts")
        
        # Print data integrity metrics
        print(f"  - data integrity: {json_data.get('data_integrity', {})}")
    else:
        logging.error(f"Error saving JSON format: {save_result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()
