#!/usr/bin/env python3
"""
SEC EDGAR Pipeline using secedgar library

This module provides a better approach to downloading SEC XBRL filings
by leveraging the secedgar library for proper URL construction and
handling of SEC's filing structures.
"""

import os
import sys
import logging
import argparse
import datetime
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Import secedgar
from secedgar.filings import Filing, FilingType
from secedgar.client import NetworkClient
from secedgar.utils import get_cik_mapper

# Import core processing modules
from src.edgar.edgar_utils import get_company_name_from_cik
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('secedgar_pipeline.log'),
        logging.StreamHandler()
    ]
)

# User agent for SEC
USER_AGENT = "Exascale Capital info@exascale.capital"

def get_cik_from_ticker_secedgar(ticker: str) -> Optional[str]:
    """Get CIK from ticker using secedgar's mapper"""
    try:
        mapper = get_cik_mapper()
        if ticker in mapper:
            cik = mapper[ticker]
            # Format CIK with leading zeros to 10 digits
            return str(cik).zfill(10)
        return None
    except Exception as e:
        logging.error(f"Error getting CIK for {ticker}: {str(e)}")
        return None

def extract_xbrl_from_filing(base_dir: Path, ticker: str, filing_type: str) -> Optional[str]:
    """
    Extract XBRL instance document from downloaded filing directory
    
    Args:
        base_dir: Base directory where secedgar downloaded the filing
        ticker: Company ticker
        filing_type: Filing type (10-K, 10-Q)
        
    Returns:
        Path to the XBRL instance document or None if not found
    """
    try:
        # secedgar creates directories with CIK numbers - find the right one
        cik_dir = None
        for dir_path in base_dir.iterdir():
            if dir_path.is_dir() and dir_path.name.isdigit():
                cik_dir = dir_path
                break
                
        if not cik_dir:
            logging.error(f"Could not find CIK directory in {base_dir}")
            return None
            
        # Look for accession number directories within the CIK directory
        xbrl_files = []
        for accession_dir in cik_dir.iterdir():
            if not accession_dir.is_dir():
                continue
                
            # Search for XML files that might be XBRL instances
            for file_path in accession_dir.glob("**/*.xml"):
                # Only consider files that might be XBRL instances
                file_name = file_path.name.lower()
                if "_htm.xml" in file_name or file_name.endswith(".xbrl") or ticker.lower() in file_name:
                    xbrl_files.append(file_path)
                    
        if not xbrl_files:
            logging.error(f"No XBRL files found for {ticker} {filing_type}")
            return None
            
        # Sort by size (largest is usually the instance document)
        xbrl_files.sort(key=lambda x: x.stat().st_size, reverse=True)
        logging.info(f"Found {len(xbrl_files)} potential XBRL files, using {xbrl_files[0]}")
        
        return str(xbrl_files[0])
    except Exception as e:
        logging.error(f"Error extracting XBRL from filing: {str(e)}")
        return None

def extract_html_from_filing(base_dir: Path, ticker: str, filing_type: str) -> Optional[str]:
    """
    Extract HTML document from downloaded filing directory
    
    Args:
        base_dir: Base directory where secedgar downloaded the filing
        ticker: Company ticker
        filing_type: Filing type (10-K, 10-Q)
        
    Returns:
        Path to the HTML document or None if not found
    """
    try:
        # secedgar creates directories with CIK numbers - find the right one
        cik_dir = None
        for dir_path in base_dir.iterdir():
            if dir_path.is_dir() and dir_path.name.isdigit():
                cik_dir = dir_path
                break
                
        if not cik_dir:
            logging.error(f"Could not find CIK directory in {base_dir}")
            return None
            
        # Look for accession number directories within the CIK directory
        html_files = []
        for accession_dir in cik_dir.iterdir():
            if not accession_dir.is_dir():
                continue
                
            # Search for HTML files that might be the primary document
            for file_path in accession_dir.glob("**/*.htm*"):
                # Skip files that are likely XBRL-related
                file_name = file_path.name.lower()
                if "index" in file_name or "xbrl" in file_name or "_def" in file_name or "_lab" in file_name:
                    continue
                    
                html_files.append(file_path)
                    
        if not html_files:
            logging.error(f"No HTML files found for {ticker} {filing_type}")
            return None
            
        # Sort by size (largest is usually the primary document)
        html_files.sort(key=lambda x: x.stat().st_size, reverse=True)
        logging.info(f"Found {len(html_files)} potential HTML files, using {html_files[0]}")
        
        return str(html_files[0])
    except Exception as e:
        logging.error(f"Error extracting HTML from filing: {str(e)}")
        return None

def process_company_filing_secedgar(ticker: str, filing_type: FilingType, count: int = 1) -> Dict[str, Any]:
    """
    Process a single filing for a company using secedgar
    
    Args:
        ticker: Company ticker symbol
        filing_type: secedgar filing type (FilingType.FILING_10K, FilingType.FILING_10Q)
        count: Number of most recent filings to retrieve (default: 1)
        
    Returns:
        Dictionary with processing results
    """
    results = {
        "ticker": ticker,
        "filing_type": str(filing_type).split(".")[-1],  # Convert FilingType to string
        "success": False,
        "xbrl_processed": False,
        "html_processed": False
    }
    
    try:
        # Get CIK for the company
        cik = get_cik_from_ticker_secedgar(ticker)
        if not cik:
            logging.error(f"Could not find CIK for ticker {ticker}")
            results["error"] = f"Could not find CIK for ticker {ticker}"
            return results
            
        results["cik"] = cik
        
        # Get company name
        company_name = get_company_name_from_cik(cik)
        if not company_name:
            company_name = f"Company {ticker}"
        results["company_name"] = company_name
        
        # Configure the client with our user agent
        client = NetworkClient(user_agent=USER_AGENT)
        
        # Create a temporary directory for downloading the filing
        temp_dir = Path(f"data/temp_secedgar/{ticker}_{filing_type}")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Create the filing object
        filing = Filing(cik_lookup=ticker,
                      filing_type=filing_type,
                      count=count,
                      client=client)
        
        # Download the filing
        logging.info(f"Downloading {filing_type} filing for {ticker}")
        filing.save(temp_dir)
        
        # Extract XBRL from the downloaded filing
        xbrl_path = extract_xbrl_from_filing(temp_dir, ticker, str(filing_type))
        if xbrl_path:
            logging.info(f"Found XBRL file at {xbrl_path}")
            results["xbrl_path"] = xbrl_path
            
            # Copy to our raw data directory
            raw_dir = os.path.join(RAW_DATA_DIR, ticker, str(filing_type).split(".")[-1])
            os.makedirs(raw_dir, exist_ok=True)
            
            # Use the original file name for persistence
            file_name = os.path.basename(xbrl_path)
            raw_file_path = os.path.join(raw_dir, file_name)
            
            # Create a hard link or copy the file
            import shutil
            shutil.copy2(xbrl_path, raw_file_path)
            results["raw_xbrl_path"] = raw_file_path
            
            # Parse XBRL file
            parsed_result = parse_xbrl_file(raw_file_path, ticker=ticker)
            if "error" not in parsed_result:
                # Generate LLM format
                filing_metadata = {
                    "ticker": ticker,
                    "company_name": company_name,
                    "filing_type": str(filing_type).split(".")[-1],
                    "filing_date": parsed_result.get("filing_date", "Unknown"),
                    "period_end_date": parsed_result.get("period_end_date", "Unknown"),
                }
                
                llm_content = generate_llm_format(parsed_result, filing_metadata)
                if llm_content:
                    # Save LLM content
                    save_result = save_llm_format(llm_content, filing_metadata)
                    results["llm_path"] = save_result.get("file_path")
                    results["xbrl_processed"] = True
                    results["fact_count"] = parsed_result.get("fact_count", 0)
                    logging.info(f"Successfully generated LLM content with {results['fact_count']} facts")
            else:
                logging.error(f"Error parsing XBRL: {parsed_result.get('error')}")
                results["xbrl_error"] = parsed_result.get("error")
        else:
            logging.error(f"No XBRL file found for {ticker} {filing_type}")
            results["xbrl_error"] = "No XBRL file found"
            
        # Extract HTML from the downloaded filing
        html_path = extract_html_from_filing(temp_dir, ticker, str(filing_type))
        if html_path:
            logging.info(f"Found HTML file at {html_path}")
            results["html_path"] = html_path
            results["html_processed"] = True
            
            # Process HTML file (placeholder - implement actual HTML processing)
            # from src.xbrl.html_text_extractor import process_html_filing
            # html_result = process_html_filing(html_path, filing_metadata)
            # results["html_result"] = html_result
        else:
            logging.error(f"No HTML file found for {ticker} {filing_type}")
            results["html_error"] = "No HTML file found"
            
        # Set success if at least one of XBRL or HTML was processed
        results["success"] = results["xbrl_processed"] or results["html_processed"]
        
        return results
    except Exception as e:
        logging.error(f"Error processing {ticker} {filing_type}: {str(e)}")
        results["error"] = str(e)
        return results

def process_ticker(ticker: str, include_10k: bool = True, include_10q: bool = True) -> Dict[str, Any]:
    """
    Process both 10-K and 10-Q filings for a ticker
    
    Args:
        ticker: Company ticker symbol
        include_10k: Whether to include 10-K filings
        include_10q: Whether to include 10-Q filings
        
    Returns:
        Dictionary with processing results
    """
    results = {
        "ticker": ticker,
        "filings_processed": []
    }
    
    try:
        if include_10k:
            k_result = process_company_filing_secedgar(ticker, FilingType.FILING_10K)
            results["filings_processed"].append({
                "filing_type": "10-K",
                "success": k_result.get("success", False),
                "xbrl_processed": k_result.get("xbrl_processed", False),
                "html_processed": k_result.get("html_processed", False),
                "fact_count": k_result.get("fact_count", 0),
                "error": k_result.get("error", None)
            })
            
        if include_10q:
            q_result = process_company_filing_secedgar(ticker, FilingType.FILING_10Q)
            results["filings_processed"].append({
                "filing_type": "10-Q",
                "success": q_result.get("success", False),
                "xbrl_processed": q_result.get("xbrl_processed", False),
                "html_processed": q_result.get("html_processed", False),
                "fact_count": q_result.get("fact_count", 0),
                "error": q_result.get("error", None)
            })
            
        return results
    except Exception as e:
        logging.error(f"Error processing ticker {ticker}: {str(e)}")
        results["error"] = str(e)
        return results

def main():
    """Main entry point for the secedgar pipeline"""
    parser = argparse.ArgumentParser(description="SEC EDGAR Filing Processor using secedgar")
    
    # Company selection
    parser.add_argument("--ticker", help="Single company ticker to process")
    parser.add_argument("--tickers", nargs="+", help="List of company tickers to process")
    
    # Filing types
    parser.add_argument("--skip-10k", action="store_true", help="Skip 10-K filings")
    parser.add_argument("--skip-10q", action="store_true", help="Skip 10-Q filings")
    
    # Additional options
    parser.add_argument("--count", type=int, default=1, help="Number of most recent filings to process per type")
    parser.add_argument("--delay", type=float, default=0.15, help="Delay between SEC requests (seconds)")
    
    args = parser.parse_args()
    
    # Set SEC request delay
    NetworkClient.DEFAULT_RETRY_WAIT = args.delay
    
    # Process companies
    if args.ticker:
        # Process a single ticker
        result = process_ticker(
            args.ticker, 
            include_10k=not args.skip_10k, 
            include_10q=not args.skip_10q
        )
        logging.info(f"Completed processing for {args.ticker}")
        print(f"Results for {args.ticker}:")
        for filing in result.get("filings_processed", []):
            success = "✅ Success" if filing.get("success") else "❌ Failed"
            print(f"{filing.get('filing_type')}: {success}")
            if filing.get("fact_count"):
                print(f"  Facts extracted: {filing.get('fact_count')}")
            if filing.get("error"):
                print(f"  Error: {filing.get('error')}")
    
    elif args.tickers:
        # Process multiple tickers
        results = {}
        for ticker in args.tickers:
            logging.info(f"Processing {ticker}")
            result = process_ticker(
                ticker, 
                include_10k=not args.skip_10k, 
                include_10q=not args.skip_10q
            )
            results[ticker] = result
            logging.info(f"Completed processing for {ticker}")
            # Add delay between companies
            time.sleep(args.delay * 3)  # Triple the delay between companies
            
        # Print summary
        print("\nProcessing Summary:")
        for ticker, result in results.items():
            filings = result.get("filings_processed", [])
            successful = sum(1 for f in filings if f.get("success"))
            print(f"{ticker}: {successful}/{len(filings)} filings processed successfully")
            
    else:
        parser.print_help()

if __name__ == "__main__":
    main()