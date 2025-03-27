#!/usr/bin/env python3
"""
Direct SEC Filing Downloader

This script provides a simple interface to directly download SEC filings
from the EDGAR system and save them locally, without using the more complex
modular system for rendering and text extraction.
"""

import os
import sys
import logging
import argparse
import json
from pathlib import Path

# Import our SEC downloader
from src2.sec.downloader import SECDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def download_filing(ticker, filing_type, email, output_dir, count=1):
    """Download SEC filings for a specific ticker"""
    # Create output directory
    output_path = Path(output_dir)
    os.makedirs(output_path, exist_ok=True)
    
    # Initialize downloader
    downloader = SECDownloader(
        user_agent=f"NativeLLM_SECDownloader/1.0",
        contact_email=email,
        download_dir=output_path
    )
    
    # Get filing information
    print(f"Looking up {filing_type} filings for {ticker}...")
    try:
        filings = downloader.get_company_filings(ticker=ticker, filing_type=filing_type, count=count)
        
        if not filings:
            print(f"No {filing_type} filings found for {ticker}")
            return False
        
        print(f"Found {len(filings)} {filing_type} filings for {ticker}")
        
        # Download each filing
        for i, filing in enumerate(filings):
            print(f"\nDownloading filing {i+1}/{len(filings)}:")
            print(f"  Accession Number: {filing['accession_number']}")
            print(f"  Filing Date: {filing['filing_date']}")
            
            # Download the filing
            result = downloader.download_filing(filing)
            
            if 'doc_path' in result:
                print(f"  Downloaded to: {result['doc_path']}")
                print(f"  SEC iXBRL URL: https://www.sec.gov{filing['ixbrl_url']}")
                
                # Save metadata
                meta_path = Path(result['filing_dir']) / "metadata.json"
                with open(meta_path, 'w') as f:
                    json.dump(filing, f, indent=2)
                print(f"  Metadata saved to: {meta_path}")
            else:
                print(f"  Failed to download: {result.get('error', 'Unknown error')}")
        
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(description="Download SEC filings")
    
    # Required arguments
    parser.add_argument("ticker", help="Ticker symbol of the company")
    parser.add_argument("--filing-type", default="10-K", help="Filing type (10-K, 10-Q, etc.)")
    parser.add_argument("--count", type=int, default=1, help="Number of recent filings to download")
    parser.add_argument("--email", required=True, help="Contact email for User-Agent (required by SEC)")
    parser.add_argument("--output", default="./sec_downloads", help="Output directory for downloaded files")
    
    args = parser.parse_args()
    
    print(f"\n{'=' * 80}")
    print(f"SEC Filing Downloader".center(80))
    print(f"{'=' * 80}")
    print(f"Ticker: {args.ticker}")
    print(f"Filing Type: {args.filing_type}")
    print(f"Count: {args.count}")
    print(f"Output Directory: {args.output}")
    print(f"{'=' * 80}\n")
    
    # Download the filings
    success = download_filing(
        args.ticker, 
        args.filing_type, 
        args.email, 
        args.output, 
        args.count
    )
    
    if success:
        print("\nDownload completed successfully!")
    else:
        print("\nDownload failed.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())