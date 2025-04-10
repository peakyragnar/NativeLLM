#!/usr/bin/env python3
"""
Run Improved JSON Formatter

This script runs the improved JSON formatter on an existing SEC filing.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the improved JSON formatter
from src2.formatter.improved_json_formatter import improved_json_formatter

def main():
    """
    Main function to run the improved JSON formatter
    """
    parser = argparse.ArgumentParser(description="Run improved JSON formatter on an existing SEC filing")
    parser.add_argument("--input", required=True, help="Input JSON file path")
    parser.add_argument("--output", help="Output JSON file path (default: input_improved.json)")

    args = parser.parse_args()

    input_path = args.input
    if not args.output:
        # Create output path by adding _improved before the extension
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_improved{ext}"
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

    if "xbrl_data" in input_data:
        xbrl_data = input_data["xbrl_data"]
        logging.info(f"Found XBRL data in input file with {len(xbrl_data.get('facts', []))} facts")
    else:
        # Try to find XBRL data in the raw file
        ticker = os.path.basename(input_path).split("_")[0]
        filing_type = os.path.basename(input_path).split("_")[1]
        year = os.path.basename(input_path).split("_")[2]

        # Look for raw XBRL file
        raw_xbrl_path = f"sec_processed/tmp/sec_downloads/{ticker}/{filing_type}/*/xbrl_raw.json"
        raw_xbrl_files = list(Path(".").glob(raw_xbrl_path))

        if raw_xbrl_files:
            try:
                with open(raw_xbrl_files[0], "r", encoding="utf-8") as f:
                    xbrl_data = json.load(f)
                logging.info(f"Loaded XBRL data from {raw_xbrl_files[0]} with {len(xbrl_data.get('facts', []))} facts")
            except Exception as e:
                logging.error(f"Error loading raw XBRL file: {e}")
        else:
            logging.warning(f"No raw XBRL file found for {ticker} {filing_type} {year}")

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
    if "document_sections" in input_data:
        metadata["html_content"] = {"document_sections": input_data["document_sections"]}
    elif "sections" in input_data:
        metadata["html_content"] = {"document_sections": input_data["sections"]}
    elif "document" in input_data and isinstance(input_data["document"], dict) and "sections" in input_data["document"]:
        metadata["html_content"] = {"document_sections": input_data["document"]["sections"]}

    # Check for sections in a different format
    if "html_content" not in metadata and "content" in input_data:
        content = input_data["content"]
        if isinstance(content, dict) and "document_sections" in content:
            metadata["html_content"] = {"document_sections": content["document_sections"]}
        elif isinstance(content, dict) and "sections" in content:
            metadata["html_content"] = {"document_sections": content["sections"]}

    # Generate improved JSON format
    if xbrl_data:
        # Generate improved JSON format
        json_data = improved_json_formatter.generate_json_format(xbrl_data, metadata)

        # Save improved JSON format
        save_result = improved_json_formatter.save_json_format(json_data, metadata, output_path)

        if save_result.get("success", False):
            logging.info(f"Successfully saved improved JSON format to {output_path}")

            # Print JSON structure summary
            print(f"\nImproved JSON file size: {os.path.getsize(output_path) / 1024:.2f} KB")
            print(f"JSON structure:")
            print(f"  - metadata: {list(json_data.get('metadata', {}).keys())}")

            # Print narrative sections
            sections = json_data.get('narrative_sections', [])
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
                print(f"      - Key metrics: {len(statement.get('key_metrics', {}))}")
                for metric, value in list(statement.get('key_metrics', {}).items())[:3]:
                    print(f"        {metric}: {value}")

                # Print structured data
                structured_data = statement.get('structured_data', {})
                periods = structured_data.get('periods', [])
                rows = structured_data.get('rows', [])
                print(f"      - Periods: {periods}")
                print(f"      - Rows: {len(rows)}")
                for row in rows[:3]:
                    print(f"        {row}")
                if len(rows) > 3:
                    print(f"        ... and {len(rows) - 3} more rows")

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
        else:
            logging.error(f"Error saving improved JSON format: {save_result.get('error', 'Unknown error')}")
    else:
        logging.error("No XBRL data found")

if __name__ == "__main__":
    main()
