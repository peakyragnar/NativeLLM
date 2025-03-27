#!/usr/bin/env python3
"""
Process SEC iXBRL filings using the modular SEC handling system.

This script demonstrates the full pipeline for processing SEC filings:
1. Download the filing using a SEC-compliant downloader
2. Render the iXBRL content using Arelle (SEC's own tool)
3. Extract and format text from the rendered content

Each step is implemented in a separate module for better debugging.
"""

import os
import sys
import json
import logging
import time
import argparse
from pathlib import Path

# Import our SEC handling modules
from src2.sec.downloader import SECDownloader
from src2.sec.renderer import ArelleRenderer
from src2.sec.extractor import SECExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("process_sec_ixbrl.log")
    ]
)

def setup_output_directories(base_dir):
    """Set up output directories for the pipeline"""
    dirs = {
        'base': Path(base_dir),
        'downloads': Path(base_dir) / 'downloads',
        'rendered': Path(base_dir) / 'rendered',
        'extracted': Path(base_dir) / 'extracted'
    }
    
    for name, path in dirs.items():
        os.makedirs(path, exist_ok=True)
        logging.info(f"Created directory: {path}")
    
    return dirs

def process_filing_by_ticker(ticker, filing_type, email, output_dir, count=1):
    """Process SEC filings for a specific ticker"""
    dirs = setup_output_directories(output_dir)
    
    # Step 1: Initialize components
    downloader = SECDownloader(
        user_agent=f"NativeLLM_SECProcessor/1.0",
        contact_email=email,
        download_dir=dirs['downloads']
    )
    
    renderer = ArelleRenderer(temp_dir=dirs['rendered'])
    extractor = SECExtractor(output_dir=dirs['extracted'])
    
    # Step 2: Get filing information
    logging.info(f"Looking up filings for {ticker} ({filing_type})")
    filings = downloader.get_company_filings(ticker=ticker, filing_type=filing_type, count=count)
    
    if not filings:
        logging.error(f"No {filing_type} filings found for {ticker}")
        return False
    
    results = []
    for i, filing in enumerate(filings):
        filing_id = f"{ticker}_{filing_type}_{i+1}"
        logging.info(f"\n{'=' * 80}\nProcessing filing {i+1}/{len(filings)}: {filing_id}\n{'=' * 80}")
        
        try:
            # Step 3: Download filing
            logging.info(f"Downloading filing: {filing['accession_number']}")
            download_result = downloader.download_filing(filing)
            
            if 'doc_path' not in download_result:
                logging.error(f"Failed to download filing: {download_result.get('error', 'Unknown error')}")
                continue
            
            html_path = download_result['doc_path']
            logging.info(f"Downloaded filing to: {html_path}")
            
            # Step 4: Render iXBRL
            logging.info(f"Rendering iXBRL content")
            rendered_path = dirs['rendered'] / f"{filing_id}_rendered.html"
            
            try:
                rendered_path = renderer.render_ixbrl(html_path, output_file=rendered_path)
                logging.info(f"Rendered iXBRL to: {rendered_path}")
            except Exception as e:
                logging.warning(f"Arelle rendering failed: {str(e)}")
                logging.info(f"Using original HTML file for extraction")
                rendered_path = html_path
            
            # Step 5: Extract text
            logging.info(f"Extracting text from rendered content")
            
            # Prepare metadata for the extractor
            metadata = {
                'filing_type': filing['filing_type'],
                'company_name': filing.get('company_name', ticker),
                'cik': filing['cik'],
                'filing_date': filing['filing_date'],
                'period_end_date': filing.get('period_end_date', 'unknown'),
                'source_url': f"https://www.sec.gov{filing['doc_url']}",
                'ixbrl_url': f"https://www.sec.gov{filing['ixbrl_url']}"
            }
            
            # Extract text from rendered content
            extracted_path = dirs['extracted'] / f"{filing_id}_extracted.txt"
            extract_result = extractor.process_filing(rendered_path, extracted_path, metadata)
            
            if extract_result['success']:
                logging.info(f"Extracted text to: {extract_result['output_path']}")
                logging.info(f"Extracted {extract_result['file_size_mb']:.2f} MB of text")
                
                # Add to results
                results.append({
                    'ticker': ticker,
                    'filing_type': filing_type,
                    'accession_number': filing['accession_number'],
                    'filing_date': filing['filing_date'],
                    'html_path': html_path,
                    'rendered_path': str(rendered_path),
                    'extracted_path': extract_result['output_path'],
                    'extracted_size_mb': extract_result['file_size_mb'],
                    'word_count': extract_result['word_count'],
                    'success': True
                })
            else:
                logging.error(f"Failed to extract text: {extract_result.get('error', 'Unknown error')}")
                results.append({
                    'ticker': ticker,
                    'filing_type': filing_type,
                    'accession_number': filing['accession_number'],
                    'filing_date': filing['filing_date'],
                    'html_path': html_path,
                    'success': False,
                    'error': extract_result.get('error', 'Unknown error')
                })
        
        except Exception as e:
            logging.error(f"Error processing filing {filing_id}: {str(e)}")
            results.append({
                'ticker': ticker,
                'filing_type': filing_type,
                'accession_number': filing.get('accession_number', 'unknown'),
                'success': False,
                'error': str(e)
            })
    
    # Save results summary
    summary_path = dirs['base'] / f"{ticker}_{filing_type}_results.json"
    with open(summary_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"\nProcessing complete!")
    logging.info(f"Processed {len(results)} filings")
    
    success_count = sum(1 for r in results if r.get('success', False))
    logging.info(f"Successfully processed {success_count}/{len(results)} filings")
    
    return success_count > 0

def process_filing_by_url(url, email, output_dir):
    """Process SEC filing directly from a URL"""
    dirs = setup_output_directories(output_dir)
    
    # Extract ticker and filing type from URL if possible
    ticker = "unknown"
    filing_type = "unknown"
    
    # Example URL: https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000095017024087843/msft-20240630.htm
    # Try to extract ticker from filename
    filename_match = re.search(r'/([a-zA-Z]+)-\d+\.htm$', url)
    if filename_match:
        ticker = filename_match.group(1).upper()
    
    # Step 1: Initialize components
    downloader = SECDownloader(
        user_agent=f"NativeLLM_SECProcessor/1.0",
        contact_email=email,
        download_dir=dirs['downloads']
    )
    
    renderer = ArelleRenderer(temp_dir=dirs['rendered'])
    extractor = SECExtractor(output_dir=dirs['extracted'])
    
    filing_id = f"{ticker}_{int(time.time())}"
    logging.info(f"\n{'=' * 80}\nProcessing filing from URL: {url}\n{'=' * 80}")
    
    try:
        # Step 2: Download filing
        logging.info(f"Downloading filing from URL: {url}")
        html_path = dirs['downloads'] / f"{filing_id}_raw.htm"
        
        # Extract accession number and CIK from URL
        acc_match = re.search(r'data/(\d+)/(\d+)/', url)
        cik = acc_match.group(1) if acc_match else "unknown"
        acc_number = acc_match.group(2) if acc_match else "unknown"
        
        # Download the file
        downloader.download_file(url, html_path)
        logging.info(f"Downloaded filing to: {html_path}")
        
        # Step 3: Render iXBRL
        logging.info(f"Rendering iXBRL content")
        rendered_path = dirs['rendered'] / f"{filing_id}_rendered.html"
        
        try:
            renderer.render_ixbrl(html_path, output_file=rendered_path)
            logging.info(f"Rendered iXBRL to: {rendered_path}")
        except Exception as e:
            logging.warning(f"Arelle rendering failed: {str(e)}")
            logging.info(f"Using original HTML file for extraction")
            rendered_path = html_path
        
        # Step 4: Extract text
        logging.info(f"Extracting text from rendered content")
        
        # Prepare metadata for the extractor
        metadata = {
            'filing_type': filing_type,
            'company_name': ticker,
            'cik': cik,
            'source_url': url
        }
        
        # Extract text from rendered content
        extracted_path = dirs['extracted'] / f"{filing_id}_extracted.txt"
        extract_result = extractor.process_filing(rendered_path, extracted_path, metadata)
        
        if extract_result['success']:
            logging.info(f"Extracted text to: {extract_result['output_path']}")
            logging.info(f"Extracted {extract_result['file_size_mb']:.2f} MB of text")
            
            print(f"\nProcessing complete!")
            print(f"Downloaded filing to: {html_path}")
            print(f"Rendered iXBRL to: {rendered_path}")
            print(f"Extracted text to: {extract_result['output_path']}")
            print(f"Extracted {extract_result['file_size_mb']:.2f} MB of text")
            return True
        else:
            logging.error(f"Failed to extract text: {extract_result.get('error', 'Unknown error')}")
            return False
    
    except Exception as e:
        logging.error(f"Error processing filing from URL: {str(e)}")
        return False

def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(description="Process SEC iXBRL filings")
    
    # Filing identification
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--ticker", help="Ticker symbol of the company")
    input_group.add_argument("--url", help="Direct URL to an SEC filing")
    
    # Filing type options
    parser.add_argument("--filing-type", default="10-K", help="Filing type (10-K, 10-Q, etc.)")
    parser.add_argument("--count", type=int, default=1, help="Number of recent filings to process")
    
    # Email is required for User-Agent
    parser.add_argument("--email", required=True, help="Contact email for User-Agent (required by SEC)")
    
    # Output options
    parser.add_argument("--output", default="./sec_output", help="Output directory for all files")
    
    args = parser.parse_args()
    
    print(f"\n{'=' * 80}")
    print(f"SEC iXBRL Filing Processor".center(80))
    print(f"{'=' * 80}")
    
    if args.ticker:
        print(f"Processing {args.filing_type} filings for {args.ticker}")
        print(f"Count: {args.count}")
        success = process_filing_by_ticker(args.ticker, args.filing_type, args.email, args.output, args.count)
    else:
        print(f"Processing filing from URL: {args.url}")
        success = process_filing_by_url(args.url, args.email, args.output)
    
    return 0 if success else 1

if __name__ == "__main__":
    import re  # Import outside for URL processing
    sys.exit(main())