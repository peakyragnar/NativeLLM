#!/usr/bin/env python3
"""
Download Missing SEC Filings

This script checks for missing SEC filings and downloads them.
It focuses on the filings that were reported as missing in the batch processing.
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Import SEC pipeline
from src2.sec.pipeline import SECFilingPipeline
from src2.sec.downloader import SECDownloader

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def download_missing_filings(ticker, email, years=None):
    """
    Download missing filings for a ticker
    
    Args:
        ticker: Company ticker symbol
        email: Contact email for SEC
        years: List of years to check (default: None, which means all available years)
    """
    # Initialize downloader
    downloader = SECDownloader(
        user_agent=f"NativeLLM_MissingFilingsDownloader/1.0",
        contact_email=email,
        download_dir=f"/Users/michael/NativeLLM/sec_processed/tmp/sec_downloads"
    )
    
    # Initialize pipeline
    pipeline = SECFilingPipeline(
        user_agent=f"NativeLLM_MissingFilingsDownloader/1.0",
        contact_email=email,
        output_dir="/Users/michael/NativeLLM/sec_processed",
        temp_dir="/Users/michael/NativeLLM/sec_processed/tmp"
    )
    
    # Define missing filings based on the error messages
    missing_filings = []
    
    # For AAPL, the missing filings are:
    if ticker == "AAPL":
        # 10-Q (2023): No filing found for AAPL FY2023 Q2 with period end in 2023-3
        missing_filings.append({
            "ticker": "AAPL",
            "filing_type": "10-Q",
            "fiscal_year": "2023",
            "fiscal_period": "Q2",
            "period_end_month": 3,
            "period_end_year": 2023
        })
        
        # 10-Q (2023): No filing found for AAPL FY2023 Q3 with period end in 2023-6
        missing_filings.append({
            "ticker": "AAPL",
            "filing_type": "10-Q",
            "fiscal_year": "2023",
            "fiscal_period": "Q3",
            "period_end_month": 6,
            "period_end_year": 2023
        })
        
        # 10-K (2025): No filing found matching fiscal year 2025
        # This is a future filing, so we can't download it yet
        
        # 10-Q (2025): No filing found for AAPL FY2025 Q2 with period end in 2025-3
        # This is a future filing, so we can't download it yet
        
        # 10-Q (2025): No filing found for AAPL FY2025 Q3 with period end in 2025-6
        # This is a future filing, so we can't download it yet
    
    # Filter by years if specified
    if years:
        missing_filings = [f for f in missing_filings if f.get("fiscal_year") in years]
    
    # Download each missing filing
    for filing_info in missing_filings:
        logging.info(f"Attempting to download {filing_info['filing_type']} for {filing_info['ticker']} "
                    f"FY{filing_info['fiscal_year']} {filing_info['fiscal_period']}")
        
        # Get all filings of this type
        filing_type = filing_info["filing_type"]
        count = 10 if filing_type == "10-K" else 20  # More quarterly filings than annual
        
        try:
            # Get all filings
            all_filings = downloader.get_company_filings(
                ticker=filing_info["ticker"],
                filing_type=filing_type,
                count=count
            )
            
            logging.info(f"Found {len(all_filings)} {filing_type} filings for {filing_info['ticker']}")
            
            # Find the filing with the closest period end date
            target_filing = None
            target_period_end_month = filing_info.get("period_end_month")
            target_period_end_year = filing_info.get("period_end_year")
            
            for filing in all_filings:
                period_end_date = filing.get("period_end_date", "")
                if period_end_date:
                    try:
                        year = int(period_end_date.split("-")[0])
                        month = int(period_end_date.split("-")[1])
                        
                        # Check if this is the target filing
                        if year == target_period_end_year and month == target_period_end_month:
                            target_filing = filing
                            break
                        
                        # If no exact match, find the closest one
                        if not target_filing and year == target_period_end_year:
                            if not target_filing or abs(month - target_period_end_month) < abs(int(target_filing.get("period_end_date", "").split("-")[1]) - target_period_end_month):
                                target_filing = filing
                    except (ValueError, IndexError):
                        continue
            
            if target_filing:
                # Add fiscal metadata
                target_filing["fiscal_year"] = filing_info["fiscal_year"]
                target_filing["fiscal_period"] = filing_info["fiscal_period"]
                
                logging.info(f"Found filing with period end date {target_filing.get('period_end_date')} "
                            f"for {filing_info['ticker']} {filing_type} "
                            f"FY{filing_info['fiscal_year']} {filing_info['fiscal_period']}")
                
                # Process the filing
                result = pipeline.process_filing_with_info(target_filing)
                
                if "error" in result:
                    logging.error(f"Error processing filing: {result['error']}")
                else:
                    logging.info(f"Successfully processed filing: {result.get('output_path', 'unknown')}")
            else:
                logging.warning(f"No suitable filing found for {filing_info['ticker']} {filing_type} "
                               f"FY{filing_info['fiscal_year']} {filing_info['fiscal_period']}")
        except Exception as e:
            logging.error(f"Error downloading filing: {str(e)}")

def main():
    """Main function"""
    # Set up logging
    setup_logging()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Download missing SEC filings")
    parser.add_argument("ticker", help="Ticker symbol of the company")
    parser.add_argument("--email", default="info@exascale.capital", help="Contact email for SEC")
    parser.add_argument("--years", nargs="+", help="Specific fiscal years to download")
    
    args = parser.parse_args()
    
    # Download missing filings
    download_missing_filings(args.ticker, args.email, args.years)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
