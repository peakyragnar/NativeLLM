#!/usr/bin/env python3
"""
Test JSON Output Format

This script tests the JSON output format for SEC filings.
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

# Import the SEC pipeline
from src2.sec.pipeline import SECFilingPipeline
from src2.sec.batch_pipeline import BatchSECPipeline

def main():
    """
    Main function to test JSON output format
    """
    parser = argparse.ArgumentParser(description="Test JSON output format for SEC filings")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], default="10-K", help="Filing type")
    parser.add_argument("--year", required=True, help="Fiscal year")
    parser.add_argument("--period", choices=["Q1", "Q2", "Q3", "Q4", "annual"], help="Fiscal period (for 10-Q)")
    parser.add_argument("--output", default="./sec_processed", help="Output directory")
    parser.add_argument("--email", default="info@exascale.capital", help="Contact email for SEC")

    args = parser.parse_args()

    # Create batch pipeline with JSON output format
    batch = BatchSECPipeline(
        user_agent=f"NativeLLM_JSONTest/1.0",
        contact_email=args.email,
        output_dir=args.output,
        output_format="json"
    )

    # Process filings for the specified year
    logging.info(f"Processing {args.ticker} {args.filing_type} {args.year} {args.period or ''} with JSON output format")

    # Convert year to integer
    year = int(args.year)

    # For 10-K, process the annual filing
    if args.filing_type == "10-K":
        results = batch.process_filings_by_years(
            ticker=args.ticker,
            start_year=year,
            end_year=year,
            include_10k=True,
            include_10q=False
        )
    # For 10-Q, process the quarterly filing
    else:
        results = batch.process_filings_by_years(
            ticker=args.ticker,
            start_year=year,
            end_year=year,
            include_10k=False,
            include_10q=True
        )

    # Get the result for the specified filing
    result = None
    for filing_key, filing_result in results.items():
        # Skip non-dictionary results
        if not isinstance(filing_result, dict):
            continue

        if str(filing_result.get("fiscal_year", "")) == args.year:
            if args.filing_type == "10-K" and filing_result.get("filing_type") == "10-K":
                result = filing_result
                break
            elif args.filing_type == "10-Q" and filing_result.get("filing_type") == "10-Q":
                if args.period and filing_result.get("fiscal_period") == args.period:
                    result = filing_result
                    break

    if result:
        logging.info(f"Successfully processed filing: {result}")

        # Load and validate JSON
        json_path = result.get("llm_path") or result.get("reorganized_llm_path")

        # If path not found in result, try to construct it
        if not json_path or not os.path.exists(json_path):
            # Construct the path based on the ticker, filing type, and year
            constructed_path = f"sec_processed/{args.ticker}/{args.ticker}_{args.filing_type}_{args.year}_llm.json"
            if os.path.exists(constructed_path):
                json_path = constructed_path
                logging.info(f"Using constructed path: {json_path}")
            else:
                logging.warning(f"Constructed path not found: {constructed_path}")

        if json_path and os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    json_data = json.load(f)

                # Print JSON structure summary
                print(f"\nJSON file size: {os.path.getsize(json_path) / 1024:.2f} KB")
                print(f"JSON structure:")
                print(f"  - metadata: {list(json_data.get('metadata', {}).keys())}")

                # Print document sections
                sections = json_data.get('document', {}).get('sections', [])
                print(f"  - document sections: {len(sections)}")
                for i, section in enumerate(sections[:5]):
                    print(f"    * {i+1}: {section.get('id', 'Unknown')} ({len(section.get('content', ''))} chars)")
                if len(sections) > 5:
                    print(f"    * ... and {len(sections) - 5} more sections")

                # Print financial statements
                statements = json_data.get('financial_data', {}).get('statements', [])
                print(f"  - financial statements: {len(statements)}")
                for i, statement in enumerate(statements):
                    print(f"    * {i+1}: {statement.get('type', 'Unknown')} ({len(statement.get('facts', []))} facts)")

                # Print XBRL facts
                facts = json_data.get('xbrl_data', {}).get('facts', [])
                print(f"  - xbrl contexts: {len(json_data.get('xbrl_data', {}).get('contexts', {}))}")
                print(f"  - xbrl facts: {len(facts)}")
                for i, fact in enumerate(facts[:5]):
                    print(f"    * {i+1}: {fact.get('concept', 'Unknown')} = {fact.get('value', '')}")
                if len(facts) > 5:
                    print(f"    * ... and {len(facts) - 5} more facts")

                # Print path to JSON file
                logging.info(f"JSON file saved to: {json_path}")

            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON file: {e}")
            except Exception as e:
                logging.error(f"Error validating JSON: {e}")
        else:
            logging.error(f"JSON file not found: {json_path}")
    else:
        logging.warning(f"Failed to find matching filing in results, trying to load JSON directly")

        # Try to load the JSON file directly
        constructed_path = f"sec_processed/{args.ticker}/{args.ticker}_{args.filing_type}_{args.year}_llm.json"
        if os.path.exists(constructed_path):
            logging.info(f"Found JSON file at: {constructed_path}")
            try:
                with open(constructed_path, "r", encoding="utf-8") as f:
                    json_data = json.load(f)

                # Print JSON structure summary
                print(f"\nJSON file size: {os.path.getsize(constructed_path) / 1024:.2f} KB")
                print(f"JSON structure:")
                print(f"  - metadata: {list(json_data.get('metadata', {}).keys())}")

                # Print document sections
                sections = json_data.get('document', {}).get('sections', [])
                print(f"  - document sections: {len(sections)}")
                for i, section in enumerate(sections[:5]):
                    print(f"    * {i+1}: {section.get('id', 'Unknown')} ({len(section.get('content', ''))} chars)")
                if len(sections) > 5:
                    print(f"    * ... and {len(sections) - 5} more sections")

                # Print financial statements
                statements = json_data.get('financial_data', {}).get('statements', [])
                print(f"  - financial statements: {len(statements)}")
                for i, statement in enumerate(statements):
                    print(f"    * {i+1}: {statement.get('type', 'Unknown')} ({len(statement.get('facts', []))} facts)")

                # Print XBRL facts
                facts = json_data.get('xbrl_data', {}).get('facts', [])
                print(f"  - xbrl contexts: {len(json_data.get('xbrl_data', {}).get('contexts', {}))}")
                print(f"  - xbrl facts: {len(facts)}")
                for i, fact in enumerate(facts[:5]):
                    print(f"    * {i+1}: {fact.get('concept', 'Unknown')} = {fact.get('value', '')}")
                if len(facts) > 5:
                    print(f"    * ... and {len(facts) - 5} more facts")
            except Exception as e:
                logging.error(f"Error loading JSON file: {e}")
        else:
            logging.error(f"Failed to find matching filing and no JSON file found at {constructed_path}")

if __name__ == "__main__":
    main()
