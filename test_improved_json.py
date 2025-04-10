#!/usr/bin/env python3
"""
Test Improved JSON Output Format

This script tests the improved JSON output format for SEC filings.
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

# Import the SEC pipeline and improved JSON formatter
from src2.sec.pipeline import SECFilingPipeline
from src2.sec.batch_pipeline import BatchSECPipeline
from src2.formatter.improved_json_formatter import improved_json_formatter

def main():
    """
    Main function to test improved JSON output format
    """
    parser = argparse.ArgumentParser(description="Test improved JSON output format for SEC filings")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], default="10-K", help="Filing type")
    parser.add_argument("--year", required=True, help="Fiscal year")
    parser.add_argument("--period", choices=["Q1", "Q2", "Q3", "Q4", "annual"], help="Fiscal period (for 10-Q)")
    parser.add_argument("--output", default="./sec_processed", help="Output directory")
    parser.add_argument("--email", default="info@exascale.capital", help="Contact email for SEC")

    args = parser.parse_args()

    # Create batch pipeline with JSON output format
    batch = BatchSECPipeline(
        user_agent=f"NativeLLM_ImprovedJSONTest/1.0",
        contact_email=args.email,
        output_dir=args.output,
        output_format="json"
    )

    # Process filings for the specified year
    logging.info(f"Processing {args.ticker} {args.filing_type} {args.year} {args.period or ''} with improved JSON output format")

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

        # Get the raw XBRL data
        xbrl_path = None
        if "stages" in result and "extract" in result["stages"]:
            extract_result = result["stages"]["extract"].get("result", {})
            if "xbrl_path" in extract_result:
                xbrl_path = extract_result["xbrl_path"]
        
        if not xbrl_path:
            # Try to find the XBRL data in the temp directory
            xbrl_path = f"sec_processed/tmp/sec_downloads/{args.ticker}/{args.filing_type}/{result.get('accession_number', '')}"
            if os.path.exists(xbrl_path):
                xbrl_path = os.path.join(xbrl_path, "_xbrl_raw.json")
                if not os.path.exists(xbrl_path):
                    xbrl_path = None
        
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
            "cik": result.get("cik", ""),
            "doc_path": result.get("doc_path", "")
        }
        
        # Get the HTML content
        html_content = {}
        if "stages" in result and "extract" in result["stages"]:
            extract_result = result["stages"]["extract"].get("result", {})
            if "document_sections" in extract_result:
                html_content["document_sections"] = extract_result["document_sections"]
        
        metadata["html_content"] = html_content
        
        # Load the XBRL data
        xbrl_data = None
        if xbrl_path and os.path.exists(xbrl_path):
            try:
                with open(xbrl_path, "r", encoding="utf-8") as f:
                    xbrl_data = json.load(f)
                logging.info(f"Loaded XBRL data from {xbrl_path}")
            except Exception as e:
                logging.error(f"Error loading XBRL data: {e}")
        
        # If we couldn't load the XBRL data, try to use the existing JSON file
        if not xbrl_data:
            json_path = result.get("llm_path") or result.get("reorganized_llm_path")
            if json_path and os.path.exists(json_path):
                try:
                    with open(json_path, "r", encoding="utf-8") as f:
                        json_data = json.load(f)
                    xbrl_data = json_data.get("xbrl_data", {})
                    logging.info(f"Loaded XBRL data from existing JSON file: {json_path}")
                except Exception as e:
                    logging.error(f"Error loading existing JSON file: {e}")
        
        # Generate improved JSON format
        if xbrl_data:
            # Create output path for improved JSON
            output_path = f"sec_processed/{args.ticker}/{args.ticker}_{args.filing_type}_{args.year}_improved.json"
            
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
    else:
        logging.warning(f"Failed to find matching filing in results")

if __name__ == "__main__":
    main()
