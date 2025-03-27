#!/usr/bin/env python3
"""
Direct SEC Filing Download Script

This is a simplified script to download SEC filings directly using our 
existing edgar_utils functions instead of relying on secedgar. This
avoids the URL construction issues we were facing.
"""

import os
import sys
import logging
import requests
import re
import time
import argparse
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Import our existing utilities
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik, sec_request
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, SEC_BASE_URL, SEC_ARCHIVE_URL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('direct_sec_download.log'),
        logging.StreamHandler()
    ]
)

def get_filing_urls(cik, filing_type):
    """
    Get URLs for the latest filing of a specific type
    
    Args:
        cik: Company CIK
        filing_type: Filing type (10-K, 10-Q)
        
    Returns:
        Dictionary with filing URLs
    """
    # Construct URL for the filing index page
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}"
    logging.info(f"Getting filing index from: {url}")
    
    # Make request
    response = sec_request(url)
    if response.status_code != 200:
        return {'error': f'Failed to get filing index: {response.status_code}'}
    
    # Parse response
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the first document link
    document_link = soup.select_one('a[id="documentsbutton"]')
    if not document_link:
        return {'error': 'No documents found'}
    
    # Get documents page URL
    documents_url = urljoin(SEC_BASE_URL, document_link['href'])
    logging.info(f"Found documents URL: {documents_url}")
    
    # Get documents page
    documents_response = sec_request(documents_url)
    if documents_response.status_code != 200:
        return {'error': f'Failed to get documents page: {documents_response.status_code}'}
    
    # Parse documents page
    doc_soup = BeautifulSoup(documents_response.text, 'html.parser')
    
    # Extract accession number from URL
    accession_match = re.search(r'(\d{10}-\d{2}-\d{6})', documents_url)
    accession_number = accession_match.group(1) if accession_match else None
    
    # Find XBRL instance document
    xbrl_url = None
    primary_doc_url = None
    
    # First look for _htm.xml which is usually the XBRL instance
    xbrl_link = None
    for link in doc_soup.find_all('a'):
        href = link.get('href', '')
        if '_htm.xml' in href:
            xbrl_link = link
            break
    
    if xbrl_link:
        xbrl_url = urljoin(SEC_BASE_URL, xbrl_link['href'])
        logging.info(f"Found XBRL instance: {xbrl_url}")
    
    # Find primary HTML document
    # Look for .htm files that aren't index.htm or *_def.htm, etc.
    # Usually the largest HTML file is the main filing
    html_links = []
    
    for link in doc_soup.find_all('a'):
        href = link.get('href', '')
        if href.endswith('.htm') and 'index' not in href and '_def' not in href and '_lab' not in href:
            # Check if it's an iXBRL document
            if 'ix?doc=' in href:
                # Make sure it's a full URL
                if href.startswith('/'):
                    primary_doc_url = urljoin(SEC_BASE_URL, href)
                else:
                    primary_doc_url = href
                logging.info(f"Found iXBRL document: {primary_doc_url}")
                break
            else:
                # Save link for later sorting
                html_links.append(link)
    
    # If we didn't find an iXBRL document, use the first HTML link
    if not primary_doc_url and html_links:
        primary_doc_url = urljoin(SEC_BASE_URL, html_links[0]['href'])
        logging.info(f"Found HTML document: {primary_doc_url}")
    
    return {
        'documents_url': documents_url,
        'xbrl_url': xbrl_url,
        'primary_doc_url': primary_doc_url,
        'accession_number': accession_number
    }

def download_xbrl(xbrl_url, ticker, filing_type):
    """
    Download XBRL instance document
    
    Args:
        xbrl_url: URL to XBRL instance
        ticker: Company ticker
        filing_type: Filing type
        
    Returns:
        Path to downloaded file
    """
    if not xbrl_url:
        return {'error': 'No XBRL URL provided'}
    
    try:
        # Create directory
        dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
        os.makedirs(dir_path, exist_ok=True)
        
        # Generate filename from URL
        file_name = os.path.basename(xbrl_url)
        file_path = os.path.join(dir_path, file_name)
        
        # Download file
        logging.info(f"Downloading XBRL from {xbrl_url}")
        response = sec_request(xbrl_url)
        
        if response.status_code != 200:
            return {'error': f'Failed to download XBRL: {response.status_code}'}
        
        # Save file
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        logging.info(f"Saved XBRL to {file_path}")
        return {
            'file_path': file_path,
            'size': len(response.content)
        }
    except Exception as e:
        return {'error': f'Error downloading XBRL: {str(e)}'}

def download_html(html_url, ticker, filing_type):
    """
    Download HTML filing document
    
    Args:
        html_url: URL to HTML document
        ticker: Company ticker
        filing_type: Filing type
        
    Returns:
        Path to downloaded file
    """
    if not html_url:
        return {'error': 'No HTML URL provided'}
    
    try:
        # Create directory
        dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
        os.makedirs(dir_path, exist_ok=True)
        
        # Generate filename from URL
        file_name = os.path.basename(html_url)
        if not file_name.endswith('.htm') and not file_name.endswith('.html'):
            file_name = f"{ticker}_{filing_type}.htm"
        file_path = os.path.join(dir_path, file_name)
        
        # Download file
        logging.info(f"Downloading HTML from {html_url}")
        response = sec_request(html_url)
        
        if response.status_code != 200:
            return {'error': f'Failed to download HTML: {response.status_code}'}
        
        # Save file
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        logging.info(f"Saved HTML to {file_path}")
        return {
            'file_path': file_path,
            'size': len(response.content)
        }
    except Exception as e:
        return {'error': f'Error downloading HTML: {str(e)}'}

def process_company_filing(ticker, filing_type):
    """
    Process a filing for a specific company
    
    Args:
        ticker: Company ticker
        filing_type: Filing type (10-K, 10-Q)
        
    Returns:
        Dictionary with results
    """
    results = {
        'ticker': ticker,
        'filing_type': filing_type,
        'xbrl_processed': False,
        'html_processed': False,
        'success': False
    }
    
    # Handle special case for foreign companies using 20-F instead of 10-K
    foreign_filing_type = None
    if filing_type == '10-K':
        foreign_filing_type = '20-F'
    
    try:
        # Get CIK
        cik = get_cik_from_ticker(ticker)
        if not cik:
            results['error'] = f'Could not find CIK for ticker {ticker}'
            return results
        
        results['cik'] = cik
        
        # Get company name
        company_name = get_company_name_from_cik(cik)
        if not company_name:
            company_name = f'Company {ticker}'
        results['company_name'] = company_name
        
        # Get filing URLs
        filing_urls = get_filing_urls(cik, filing_type)
        
        # If no documents found and we have a foreign filing type, try that instead
        if 'error' in filing_urls and foreign_filing_type and "No documents found" in filing_urls['error']:
            logging.info(f"No {filing_type} found, trying {foreign_filing_type} for foreign company")
            filing_urls = get_filing_urls(cik, foreign_filing_type)
            if 'error' not in filing_urls:
                # Update filing type to show we're using the foreign equivalent
                results['original_filing_type'] = filing_type
                results['filing_type'] = foreign_filing_type
                logging.info(f"Successfully found {foreign_filing_type} filing instead of {filing_type}")
        
        if 'error' in filing_urls:
            results['error'] = filing_urls['error']
            return results
        
        results['urls'] = filing_urls
        
        # Download XBRL if available
        if filing_urls.get('xbrl_url'):
            xbrl_result = download_xbrl(filing_urls['xbrl_url'], ticker, filing_type)
            if 'error' not in xbrl_result:
                results['xbrl_file'] = xbrl_result['file_path']
                
                # Parse XBRL file
                filing_metadata = {
                    'ticker': ticker,
                    'company_name': company_name,
                    'filing_type': filing_type,
                    'cik': cik,
                    'accession_number': filing_urls.get('accession_number', ''),
                }
                
                parsed_result = parse_xbrl_file(xbrl_result['file_path'], ticker=ticker, filing_metadata=filing_metadata)
                
                if 'error' not in parsed_result:
                    # Generate LLM format
                    llm_content = generate_llm_format(parsed_result, filing_metadata)
                    
                    if llm_content:
                        # Save LLM content
                        save_result = save_llm_format(llm_content, filing_metadata)
                        results['llm_file'] = save_result.get('file_path')
                        results['xbrl_processed'] = True
                        results['fact_count'] = parsed_result.get('fact_count', 0)
                else:
                    results['xbrl_error'] = parsed_result.get('error')
            else:
                results['xbrl_error'] = xbrl_result['error']
        
        # Download HTML if available
        if filing_urls.get('primary_doc_url'):
            html_result = download_html(filing_urls['primary_doc_url'], ticker, filing_type)
            if 'error' not in html_result:
                results['html_file'] = html_result['file_path']
                results['html_processed'] = True
                
                # We'd process HTML here if needed
                # from src.xbrl.html_text_extractor import process_html_filing
                # html_text_result = process_html_filing(html_result['file_path'], filing_metadata)
                # results['html_text_file'] = html_text_result.get('text_file')
            else:
                results['html_error'] = html_result['error']
        
        # Mark as success if at least one file was processed
        results['success'] = results['xbrl_processed'] or results['html_processed']
        
        return results
    except Exception as e:
        logging.error(f"Error processing {ticker} {filing_type}: {str(e)}")
        results['error'] = str(e)
        return results

def process_ticker(ticker, include_10k=True, include_10q=True):
    """
    Process both 10-K and 10-Q filings for a ticker
    
    Args:
        ticker: Company ticker
        include_10k: Whether to process 10-K
        include_10q: Whether to process 10-Q
        
    Returns:
        Dictionary with results
    """
    results = {
        'ticker': ticker,
        'filings_processed': []
    }
    
    try:
        if include_10k:
            logging.info(f"Processing 10-K for {ticker}")
            k_result = process_company_filing(ticker, '10-K')
            results['filings_processed'].append({
                'filing_type': '10-K',
                'success': k_result.get('success', False),
                'xbrl_processed': k_result.get('xbrl_processed', False),
                'html_processed': k_result.get('html_processed', False),
                'fact_count': k_result.get('fact_count', 0),
                'error': k_result.get('error')
            })
        
        if include_10q:
            # Add delay to avoid hitting SEC rate limits
            time.sleep(0.1)
            logging.info(f"Processing 10-Q for {ticker}")
            q_result = process_company_filing(ticker, '10-Q')
            results['filings_processed'].append({
                'filing_type': '10-Q',
                'success': q_result.get('success', False),
                'xbrl_processed': q_result.get('xbrl_processed', False),
                'html_processed': q_result.get('html_processed', False),
                'fact_count': q_result.get('fact_count', 0),
                'error': q_result.get('error')
            })
        
        return results
    except Exception as e:
        logging.error(f"Error processing ticker {ticker}: {str(e)}")
        results['error'] = str(e)
        return results

def main():
    parser = argparse.ArgumentParser(description="Direct SEC Filing Downloader")
    
    # Company selection
    parser.add_argument("--ticker", help="Company ticker to process")
    parser.add_argument("--tickers", nargs="+", help="List of company tickers to process")
    
    # Filing types
    parser.add_argument("--filing-type", choices=['10-K', '10-Q'], help="Specific filing type to process")
    parser.add_argument("--skip-10k", action="store_true", help="Skip 10-K filings")
    parser.add_argument("--skip-10q", action="store_true", help="Skip 10-Q filings")
    
    args = parser.parse_args()
    
    if args.ticker and args.filing_type:
        # Process a specific filing
        result = process_company_filing(args.ticker, args.filing_type)
        
        # Print results
        print(f"\nResults for {args.ticker} {args.filing_type}:")
        print(f"Success: {'✅' if result.get('success') else '❌'}")
        print(f"XBRL processed: {'✅' if result.get('xbrl_processed') else '❌'}")
        print(f"HTML processed: {'✅' if result.get('html_processed') else '❌'}")
        
        if result.get('fact_count'):
            print(f"Facts extracted: {result.get('fact_count')}")
        
        if result.get('error'):
            print(f"Error: {result.get('error')}")
            
        if result.get('urls'):
            print("\nURLs:")
            for key, url in result.get('urls').items():
                if url and key != 'error':
                    print(f"  {key}: {url}")
    
    elif args.ticker:
        # Process both filing types for a ticker
        include_10k = not args.skip_10k
        include_10q = not args.skip_10q
        
        result = process_ticker(args.ticker, include_10k, include_10q)
        
        # Print results
        print(f"\nResults for {args.ticker}:")
        for filing in result.get('filings_processed', []):
            success = "✅ Success" if filing.get('success') else "❌ Failed"
            print(f"{filing.get('filing_type')}: {success}")
            
            if filing.get('fact_count'):
                print(f"  Facts extracted: {filing.get('fact_count')}")
            
            if filing.get('error'):
                print(f"  Error: {filing.get('error')}")
    
    elif args.tickers:
        # Process multiple tickers
        include_10k = not args.skip_10k
        include_10q = not args.skip_10q
        
        for ticker in args.tickers:
            result = process_ticker(ticker, include_10k, include_10q)
            
            # Print results for this ticker
            print(f"\nResults for {ticker}:")
            for filing in result.get('filings_processed', []):
                success = "✅ Success" if filing.get('success') else "❌ Failed"
                print(f"{filing.get('filing_type')}: {success}")
                
                if filing.get('fact_count'):
                    print(f"  Facts extracted: {filing.get('fact_count')}")
                
                if filing.get('error'):
                    print(f"  Error: {filing.get('error')}")
            
            # Add delay between tickers
            time.sleep(0.2)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()