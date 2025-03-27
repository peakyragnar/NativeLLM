#!/usr/bin/env python3
"""
SEC Filing Downloader

A focused script to download SEC filings with secedgar library.
- Supports downloading by year range (2022-2025)
- Works with specific tickers
- Can process single filings
- Provides clear summary output
"""

import os
import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sec_downloader.log"),
        logging.StreamHandler()
    ]
)

# SEC EDGAR settings
USER_AGENT = "Exascale Capital info@exascale.capital"
OUTPUT_DIR = "data/filings"

# Import secedgar
try:
    from secedgar.client import NetworkClient
    from secedgar.core.filings import filings
    from secedgar.core.filing_types import FilingType
except ImportError:
    print("Error: The secedgar library is not installed.")
    print("Install it with: pip install secedgar")
    sys.exit(1)

def download_filing(ticker, filing_type, year=None):
    """
    Download a single filing for a ticker
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (10-K or 10-Q)
        year: Target year or None for most recent
        
    Returns:
        Dict with download results
    """
    print(f"\nDownloading {filing_type} for {ticker}" + (f" ({year})" if year else ""))
    
    try:
        # Create network client
        client = NetworkClient(user_agent=USER_AGENT)
        
        # Map filing type
        if filing_type == "10-K":
            sec_filing_type = FilingType.FILING_10K
        elif filing_type == "10-Q":
            sec_filing_type = FilingType.FILING_10Q
        else:
            return {"error": f"Unsupported filing type: {filing_type}"}
        
        # Create output directory
        output_path = Path(f"{OUTPUT_DIR}/{ticker}/{filing_type}")
        if year:
            output_path = output_path / str(year)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create filing object
        filing_obj = filings(cik_lookup=ticker,
                            filing_type=sec_filing_type,
                            count=1,
                            client=client)
        
        # Download filing
        download_start = time.time()
        filing_obj.save(output_path)
        download_time = time.time() - download_start
        
        # Find the downloaded files
        all_files = list(output_path.glob("**/*"))
        directories = [d for d in all_files if d.is_dir() and d != output_path]
        
        # If we have subdirectories, they probably contain the filing data
        if directories:
            # Use most recently modified directory
            latest_dir = max(directories, key=os.path.getmtime)
            xml_files = list(latest_dir.glob("**/*.xml"))
            html_files = list(latest_dir.glob("**/*.htm*"))
            
            # Find the main document (usually largest HTML file)
            main_document = None
            if html_files:
                main_document = max(html_files, key=lambda f: f.stat().st_size)
            
            # Find XBRL document
            xbrl_document = None
            if xml_files:
                # Try to find instance document
                for xml_file in xml_files:
                    if "_htm.xml" in xml_file.name or "-" in xml_file.name:
                        xbrl_document = xml_file
                        break
                
                # If no specific instance document found, use the first XML file
                if not xbrl_document and xml_files:
                    xbrl_document = xml_files[0]
            
            # Print summary
            print(f"\nDownload Summary for {ticker} {filing_type}:")
            print(f"  Directory: {latest_dir}")
            print(f"  Files found: {len(list(latest_dir.glob('**/*')))} total files")
            print(f"  HTML files: {len(html_files)}")
            print(f"  XML files: {len(xml_files)}")
            
            if main_document:
                print(f"\nMain document:")
                print(f"  Path: {main_document}")
                print(f"  Size: {main_document.stat().st_size / 1024:.1f} KB")
                
            if xbrl_document:
                print(f"\nXBRL document:")
                print(f"  Path: {xbrl_document}")
                print(f"  Size: {xbrl_document.stat().st_size / 1024:.1f} KB")
            
            print(f"\nPerformance:")
            print(f"  Download time: {download_time:.2f} seconds")
            
            return {
                "success": True,
                "ticker": ticker,
                "filing_type": filing_type,
                "year": year,
                "directory": str(latest_dir),
                "html_files": [str(f) for f in html_files],
                "xml_files": [str(f) for f in xml_files],
                "main_document": str(main_document) if main_document else None,
                "xbrl_document": str(xbrl_document) if xbrl_document else None,
                "file_count": len(list(latest_dir.glob('**/*'))),
                "download_time": download_time
            }
        else:
            print("\nNo files found after download.")
            return {
                "success": False,
                "error": "No files found after download"
            }
    
    except Exception as e:
        print(f"\nError downloading filing: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def download_filings_by_year_range(ticker, start_year, end_year, filing_types=None):
    """
    Download filings for a ticker by year range
    
    Args:
        ticker: Company ticker symbol
        start_year: Start year (inclusive)
        end_year: End year (inclusive)
        filing_types: List of filing types or None for all types
        
    Returns:
        Dict with download results
    """
    if filing_types is None:
        filing_types = ["10-K", "10-Q"]
    
    print(f"\nDownloading filings for {ticker} from {start_year} to {end_year}")
    print(f"Filing types: {', '.join(filing_types)}")
    
    results = {
        "ticker": ticker,
        "year_range": f"{start_year}-{end_year}",
        "filing_types": filing_types,
        "filings": []
    }
    
    for year in range(start_year, end_year + 1):
        for filing_type in filing_types:
            result = download_filing(ticker, filing_type, year)
            results["filings"].append(result)
    
    # Print summary
    success_count = sum(1 for f in results["filings"] if f.get("success", False))
    
    print("\nDownload Summary:")
    print(f"  Ticker: {ticker}")
    print(f"  Year range: {start_year}-{end_year}")
    print(f"  Filing types: {', '.join(filing_types)}")
    print(f"  Filings attempted: {len(results['filings'])}")
    print(f"  Filings successful: {success_count}")
    print(f"  Success rate: {success_count / len(results['filings']) * 100:.1f}%")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Download SEC filings with secedgar")
    parser.add_argument("--ticker", required=True, help="Company ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], help="Filing type for single filing")
    parser.add_argument("--year", type=int, help="Year for single filing")
    parser.add_argument("--start-year", type=int, help="Start year for range")
    parser.add_argument("--end-year", type=int, help="End year for range")
    parser.add_argument("--skip-10k", action="store_true", help="Skip 10-K filings")
    parser.add_argument("--skip-10q", action="store_true", help="Skip 10-Q filings")
    
    args = parser.parse_args()
    
    # Determine filing types
    filing_types = []
    if not args.skip_10k:
        filing_types.append("10-K")
    if not args.skip_10q:
        filing_types.append("10-Q")
    
    if not filing_types:
        print("Error: No filing types selected. Either --skip-10k or --skip-10q must be false.")
        sys.exit(1)
    
    # Single filing mode
    if args.filing_type:
        download_filing(args.ticker, args.filing_type, args.year)
    
    # Year range mode
    elif args.start_year and args.end_year:
        download_filings_by_year_range(args.ticker, args.start_year, args.end_year, filing_types)
    
    # Invalid mode
    else:
        print("Error: Either --filing-type or both --start-year and --end-year must be specified.")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()