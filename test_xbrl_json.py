#!/usr/bin/env python3
"""
Test XBRL JSON Output Format

This script tests the XBRL JSON output format for SEC filings.
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

# Import the SEC pipeline and XBRL JSON formatter
from src2.formatter.xbrl_json_formatter import xbrl_json_formatter

def main():
    """
    Main function to test XBRL JSON output format
    """
    parser = argparse.ArgumentParser(description="Test XBRL JSON output format for SEC filings")
    parser.add_argument("--input", required=True, help="Input JSON file path")
    parser.add_argument("--output", help="Output JSON file path (default: input_xbrl.json)")

    args = parser.parse_args()

    input_path = args.input
    if not args.output:
        # Create output path by adding _xbrl before the extension
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_xbrl{ext}"
    else:
        output_path = args.output

    logging.info(f"Processing {input_path} to {output_path}")

    # Load the input JSON file
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            input_data = json.load(f)
        logging.info(f"Loaded input file: {input_path}")
    except Exception as e:
        logging.error(f"Error loading input file: {e}")
        return

    # Extract XBRL data and metadata
    xbrl_data = None
    metadata = {}

    # Extract ticker, filing_type, and year from the filename
    filename = os.path.basename(input_path)
    parts = filename.split("_")

    if len(parts) >= 3:
        ticker = parts[0]
        filing_type = parts[1]
        year = parts[2].split(".")[0]
    else:
        # Default values if filename doesn't match expected pattern
        ticker = "UNKNOWN"
        filing_type = "UNKNOWN"
        year = "UNKNOWN"

    # Always try to find the raw XBRL file first
    raw_xbrl_path = f"sec_processed/tmp/sec_downloads/{ticker}/{filing_type}/*/xbrl_raw.json"
    raw_xbrl_files = list(Path(".").glob(raw_xbrl_path))

    if raw_xbrl_files:
        try:
            with open(raw_xbrl_files[0], "r", encoding="utf-8") as f:
                xbrl_data = json.load(f)
            logging.info(f"Loaded XBRL data from {raw_xbrl_files[0]} with {len(xbrl_data)} facts")
        except Exception as e:
            logging.error(f"Error loading raw XBRL file: {e}")
            xbrl_data = None
    else:
        logging.warning(f"No raw XBRL file found for {ticker} {filing_type} {year}")
        xbrl_data = None

    # If we couldn't find the raw XBRL file, try to extract it from the input file
    if not xbrl_data:
        if "xbrl_data" in input_data:
            xbrl_data = input_data["xbrl_data"]
            logging.info(f"Found XBRL data in input file with {len(xbrl_data.get('facts', []))} facts")
        elif isinstance(input_data, list):
            # This is likely a raw XBRL file with a list of facts
            xbrl_data = {"facts": input_data}
            logging.info(f"Found raw XBRL data with {len(input_data)} facts")

    # Extract metadata
    if "metadata" in input_data:
        metadata = input_data["metadata"]
    else:
        # Try to extract metadata from the filename
        filename = os.path.basename(input_path)
        parts = filename.split("_")

        if len(parts) >= 3:
            metadata["ticker"] = parts[0]
            metadata["filing_type"] = parts[1]
            metadata["fiscal_year"] = parts[2].split(".")[0]

    # Extract document sections
    if "document" in input_data and "sections" in input_data["document"]:
        metadata["html_content"] = {"document_sections": input_data["document"]["sections"]}
    elif "document_sections" in input_data:
        metadata["html_content"] = {"document_sections": input_data["document_sections"]}
    elif "sections" in input_data:
        metadata["html_content"] = {"document_sections": input_data["sections"]}
    elif "content" in input_data and isinstance(input_data["content"], dict):
        content = input_data["content"]
        if "document_sections" in content:
            metadata["html_content"] = {"document_sections": content["document_sections"]}
        elif "sections" in content:
            metadata["html_content"] = {"document_sections": content["sections"]}

    # Generate XBRL JSON format
    if xbrl_data:
        # Generate XBRL JSON format
        json_data = xbrl_json_formatter.generate_json_format(xbrl_data, metadata)

        # Save XBRL JSON format
        save_result = xbrl_json_formatter.save_json_format(json_data, metadata, output_path)

        if save_result.get("success", False):
            logging.info(f"Successfully saved XBRL JSON format to {output_path}")

            # Print JSON structure summary
            print(f"\nXBRL JSON file size: {os.path.getsize(output_path) / 1024:.2f} KB")
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
            print(f"  - xbrl contexts: {len(json_data.get('xbrl_data', {}).get('contexts', {}))}")
            print(f"  - xbrl facts: {len(facts)}")
            for i, fact in enumerate(facts[:5]):
                print(f"    * {i+1}: {fact.get('concept', 'Unknown')} = {fact.get('value', '')}")
            if len(facts) > 5:
                print(f"    * ... and {len(facts) - 5} more facts")

            # Print data integrity metrics
            print(f"  - data integrity: {json_data.get('data_integrity', {})}")

            # Compare file sizes
            original_size = os.path.getsize(input_path)
            new_size = os.path.getsize(output_path)
            size_diff = new_size - original_size
            size_diff_percent = (size_diff / original_size) * 100
            print(f"\nFile size comparison:")
            print(f"  - Original: {original_size / 1024:.2f} KB")
            print(f"  - XBRL JSON: {new_size / 1024:.2f} KB")
            print(f"  - Difference: {size_diff / 1024:.2f} KB ({size_diff_percent:.2f}%)")
        else:
            logging.error(f"Error saving XBRL JSON format: {save_result.get('error', 'Unknown error')}")
    else:
        logging.error("No XBRL data found")

if __name__ == "__main__":
    main()
