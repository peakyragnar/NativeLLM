#!/usr/bin/env python3
"""
Test Complete JSON Output Format

This script tests the complete JSON output format for SEC filings.
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

# Import the SEC pipeline and complete JSON formatter
from src2.sec.pipeline import SECFilingPipeline
from src2.sec.batch_pipeline import BatchSECPipeline
from src2.formatter.complete_json_formatter import complete_json_formatter

def main():
    """
    Main function to test complete JSON output format
    """
    parser = argparse.ArgumentParser(description="Test complete JSON output format for SEC filings")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], default="10-K", help="Filing type")
    parser.add_argument("--year", required=True, help="Fiscal year")
    parser.add_argument("--period", choices=["Q1", "Q2", "Q3", "Q4", "annual"], help="Fiscal period (for 10-Q)")
    parser.add_argument("--output", default="./sec_processed", help="Output directory")
    parser.add_argument("--email", default="info@exascale.capital", help="Contact email for SEC")

    args = parser.parse_args()

    # Create batch pipeline with JSON output format
    batch = BatchSECPipeline(
        user_agent=f"NativeLLM_CompleteJSONTest/1.0",
        contact_email=args.email,
        output_dir=args.output,
        output_format="json"
    )

    # Process filings for the specified year
    logging.info(f"Processing {args.ticker} {args.filing_type} {args.year} {args.period or ''} with complete JSON output format")

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

        # Get the HTML path
        html_path = result.get("doc_path")
        if not html_path:
            logging.error("No HTML path found in result")
            return

        # Get the metadata
        metadata = {
            "ticker": args.ticker,
            "filing_type": args.filing_type,
            "fiscal_year": args.year,
            "fiscal_period": args.period or "annual",
            "period_end_date": result.get("period_end_date", ""),
            "filing_date": result.get("filing_date", ""),
            "company_name": result.get("company_name", ""),
            "source_url": result.get("source_url", ""),
            "cik": result.get("cik", "")
        }

        # Create output path for complete JSON
        output_path = f"sec_processed/{args.ticker}/{args.ticker}_{args.filing_type}_{args.year}_complete.json"

        # Generate complete JSON format
        json_data = complete_json_formatter.generate_json_format(html_path, metadata)

        # Save complete JSON format
        save_result = complete_json_formatter.save_json_format(json_data, metadata, output_path)

        if save_result.get("success", False):
            logging.info(f"Successfully saved complete JSON format to {output_path}")

            # Print JSON structure summary
            print(f"\nComplete JSON file size: {os.path.getsize(output_path) / 1024:.2f} KB")
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
            original_size = os.path.getsize(result.get("llm_path", "")) if result.get("llm_path") and os.path.exists(result.get("llm_path", "")) else 0
            new_size = os.path.getsize(output_path)
            if original_size > 0:
                size_diff = new_size - original_size
                size_diff_percent = (size_diff / original_size) * 100
                print(f"\nFile size comparison:")
                print(f"  - Original: {original_size / 1024:.2f} KB")
                print(f"  - Complete JSON: {new_size / 1024:.2f} KB")
                print(f"  - Difference: {size_diff / 1024:.2f} KB ({size_diff_percent:.2f}%)")
        else:
            logging.error(f"Error saving complete JSON format: {save_result.get('error', 'Unknown error')}")
    else:
        logging.warning(f"Failed to find matching filing in results")

if __name__ == "__main__":
    main()
