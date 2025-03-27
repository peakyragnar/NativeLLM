#!/usr/bin/env python3
"""
Test script for the integrated SEC filing download solution.

This script tests the integrated solution with different companies,
including domestic and foreign companies, to verify that the enhanced
XBRL downloader works correctly.
"""

import os
import sys
import logging
import argparse
import json
from pathlib import Path
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_integration.log'),
        logging.StreamHandler()
    ]
)

# Import our modules
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.xbrl.xbrl_downloader import get_filing_urls, download_xbrl_instance, download_html_filing
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.xbrl.enhanced_processor import process_company_filing
from src.formatter.llm_formatter import generate_llm_format, save_llm_format

def test_filing_urls(ticker, filing_type):
    """Test the get_filing_urls function with a ticker and filing type"""
    logging.info(f"Testing get_filing_urls with {ticker} {filing_type}...")
    
    # Get CIK
    cik = get_cik_from_ticker(ticker)
    if not cik:
        logging.error(f"Could not find CIK for {ticker}")
        return False
    
    logging.info(f"Found CIK for {ticker}: {cik}")
    
    # Get company name
    company_name = get_company_name_from_cik(cik)
    logging.info(f"Company name: {company_name}")
    
    # Get filing URLs
    urls = get_filing_urls(cik, filing_type)
    
    if 'error' in urls:
        logging.error(f"Error getting filing URLs: {urls['error']}")
        return False
    
    # Log URLs
    logging.info(f"Filing URLs:")
    for key, url in urls.items():
        if url and key != 'error':
            logging.info(f"  {key}: {url}")
    
    # Check if we found at least one valid URL
    if not urls.get('xbrl_url') and not urls.get('primary_doc_url'):
        logging.error("No valid URLs found")
        return False
    
    return {
        'cik': cik,
        'company_name': company_name,
        'filing_type': filing_type,
        'urls': urls
    }

def test_download_xbrl(filing_data):
    """Test downloading XBRL using the provided filing data"""
    ticker = filing_data.get('ticker')
    filing_type = filing_data.get('filing_type')
    
    logging.info(f"Testing XBRL download for {ticker} {filing_type}...")
    
    # Create filing metadata
    filing_metadata = {
        'ticker': ticker,
        'filing_type': filing_type,
        'cik': filing_data.get('cik'),
        'company_name': filing_data.get('company_name')
    }
    
    # Add URLs if available
    if 'urls' in filing_data:
        if 'xbrl_url' in filing_data['urls']:
            filing_metadata['xbrl_url'] = filing_data['urls']['xbrl_url']
        if 'primary_doc_url' in filing_data['urls']:
            filing_metadata['primary_doc_url'] = filing_data['urls']['primary_doc_url']
        if 'accession_number' in filing_data['urls']:
            filing_metadata['accession_number'] = filing_data['urls']['accession_number']
        if 'filing_date' in filing_data['urls']:
            filing_metadata['filing_date'] = filing_data['urls']['filing_date']
        if 'period_end_date' in filing_data['urls']:
            filing_metadata['period_end_date'] = filing_data['urls']['period_end_date']
    
    # Download XBRL
    download_result = download_xbrl_instance(filing_metadata)
    
    if 'error' in download_result:
        logging.error(f"Error downloading XBRL: {download_result['error']}")
        return False
    
    logging.info(f"XBRL downloaded successfully: {download_result.get('file_path')}")
    
    # If downloaded from cache, note that
    if download_result.get('from_cache'):
        logging.info("File was loaded from cache (already downloaded)")
    
    return download_result

def test_download_html(filing_data):
    """Test downloading HTML using the provided filing data"""
    ticker = filing_data.get('ticker')
    filing_type = filing_data.get('filing_type')
    
    logging.info(f"Testing HTML download for {ticker} {filing_type}...")
    
    # Create filing metadata
    filing_metadata = {
        'ticker': ticker,
        'filing_type': filing_type,
        'cik': filing_data.get('cik'),
        'company_name': filing_data.get('company_name')
    }
    
    # Add URLs if available
    if 'urls' in filing_data:
        if 'xbrl_url' in filing_data['urls']:
            filing_metadata['xbrl_url'] = filing_data['urls']['xbrl_url']
        if 'primary_doc_url' in filing_data['urls']:
            filing_metadata['primary_doc_url'] = filing_data['urls']['primary_doc_url']
        if 'accession_number' in filing_data['urls']:
            filing_metadata['accession_number'] = filing_data['urls']['accession_number']
        if 'filing_date' in filing_data['urls']:
            filing_metadata['filing_date'] = filing_data['urls']['filing_date']
        if 'period_end_date' in filing_data['urls']:
            filing_metadata['period_end_date'] = filing_data['urls']['period_end_date']
    
    # Download HTML
    download_result = download_html_filing(filing_metadata)
    
    if 'error' in download_result:
        logging.error(f"Error downloading HTML: {download_result['error']}")
        return False
    
    logging.info(f"HTML downloaded successfully: {download_result.get('file_path')}")
    
    # If downloaded from cache, note that
    if download_result.get('from_cache'):
        logging.info("File was loaded from cache (already downloaded)")
    
    return download_result

def test_parse_xbrl(file_path, filing_data):
    """Test parsing XBRL using the provided file path"""
    ticker = filing_data.get('ticker')
    filing_type = filing_data.get('filing_type')
    
    logging.info(f"Testing XBRL parsing for {ticker} {filing_type}...")
    
    # Create filing metadata for parser
    filing_metadata = {
        'ticker': ticker,
        'filing_type': filing_type,
        'cik': filing_data.get('cik'),
        'company_name': filing_data.get('company_name')
    }
    
    # Add other metadata if available
    if 'urls' in filing_data and 'accession_number' in filing_data['urls']:
        filing_metadata['accession_number'] = filing_data['urls']['accession_number']
    
    # Parse XBRL
    try:
        parsed_result = parse_xbrl_file(file_path, ticker=ticker, filing_metadata=filing_metadata)
        
        if 'error' in parsed_result:
            logging.error(f"Error parsing XBRL: {parsed_result['error']}")
            return False
        
        fact_count = len(parsed_result.get('facts', []))
        logging.info(f"XBRL parsed successfully. Facts found: {fact_count}")
        
        return {
            'success': True,
            'fact_count': fact_count,
            'contexts': len(parsed_result.get('contexts', {})),
            'units': len(parsed_result.get('units', {})),
            'parsed_result': parsed_result
        }
    except Exception as e:
        logging.error(f"Exception parsing XBRL: {str(e)}")
        return False

def test_process_company_filing(ticker, filing_type):
    """Test the enhanced processor for a company filing"""
    logging.info(f"Testing enhanced processor for {ticker} {filing_type}...")
    
    # Get CIK
    cik = get_cik_from_ticker(ticker)
    if not cik:
        logging.error(f"Could not find CIK for {ticker}")
        return False
    
    logging.info(f"Found CIK for {ticker}: {cik}")
    
    # Get company name
    company_name = get_company_name_from_cik(cik)
    logging.info(f"Company name: {company_name}")
    
    # Create filing metadata
    filing_metadata = {
        'ticker': ticker,
        'filing_type': filing_type,
        'cik': cik,
        'company_name': company_name
    }
    
    # Process the filing
    try:
        result = process_company_filing(filing_metadata)
        
        if 'error' in result:
            logging.error(f"Error processing filing: {result['error']}")
            return False
        
        # Log results
        process_path = result.get('processing_path', 'unknown')
        fact_count = result.get('fact_count', 0)
        
        logging.info(f"Filing processed successfully using {process_path} approach")
        logging.info(f"Facts found: {fact_count}")
        
        # If we have facts, try generating LLM format
        if fact_count > 0:
            logging.info("Generating LLM format...")
            llm_content = generate_llm_format(result, filing_metadata)
            
            if llm_content:
                logging.info(f"LLM format generated successfully. Length: {len(llm_content)} chars")
                
                # Save LLM format
                save_result = save_llm_format(llm_content, filing_metadata)
                
                if 'error' in save_result:
                    logging.error(f"Error saving LLM format: {save_result['error']}")
                else:
                    logging.info(f"LLM format saved to: {save_result.get('file_path')}")
        
        return {
            'success': True,
            'process_path': process_path,
            'fact_count': fact_count,
            'result': result
        }
    
    except Exception as e:
        logging.error(f"Exception processing filing: {str(e)}")
        return False

def test_process_multi_company(tickers, filing_type='10-K'):
    """Test processing multiple companies"""
    logging.info(f"Testing multiple companies: {', '.join(tickers)}")
    logging.info(f"Filing type: {filing_type}")
    
    results = {}
    
    for ticker in tickers:
        logging.info(f"Processing {ticker}...")
        
        try:
            result = test_process_company_filing(ticker, filing_type)
            
            if not result:
                logging.error(f"Failed to process {ticker} {filing_type}")
                results[ticker] = {'success': False, 'error': 'Processing failed'}
            else:
                results[ticker] = {
                    'success': True,
                    'process_path': result.get('process_path', 'unknown'),
                    'fact_count': result.get('fact_count', 0)
                }
            
            logging.info(f"Completed processing {ticker}")
            
            # Add delay to avoid hitting SEC rate limits
            time.sleep(0.2)
        
        except Exception as e:
            logging.error(f"Exception processing {ticker}: {str(e)}")
            results[ticker] = {'success': False, 'error': str(e)}
    
    # Print summary
    logging.info("\nProcessing Summary:")
    successes = sum(1 for r in results.values() if r.get('success', False))
    
    for ticker, result in results.items():
        status = "✅ Success" if result.get('success', False) else "❌ Failed"
        info = f"({result.get('process_path', 'unknown')}, {result.get('fact_count', 0)} facts)" if result.get('success', False) else f"({result.get('error', 'unknown error')})"
        logging.info(f"{ticker}: {status} {info}")
    
    logging.info(f"\nOverall: {successes}/{len(results)} companies successfully processed")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Test the integrated SEC filing download solution")
    
    # Test options
    parser.add_argument("--test-urls", action="store_true", help="Test getting filing URLs")
    parser.add_argument("--test-download", action="store_true", help="Test downloading filings")
    parser.add_argument("--test-parse", action="store_true", help="Test parsing XBRL")
    parser.add_argument("--test-process", action="store_true", help="Test enhanced processing")
    parser.add_argument("--test-all", action="store_true", help="Run all tests")
    
    # Company selection
    parser.add_argument("--ticker", help="Company ticker to test (e.g., MSFT)")
    parser.add_argument("--tickers", nargs="+", help="List of tickers to test")
    
    # Filing type
    parser.add_argument("--filing-type", choices=['10-K', '10-Q', '20-F'], default='10-K', 
                        help="Filing type to test (default: 10-K)")
    
    args = parser.parse_args()
    
    # If no test specified, run all tests
    if not (args.test_urls or args.test_download or args.test_parse or args.test_process):
        args.test_all = True
    
    # If no ticker specified, use default test set
    if not args.ticker and not args.tickers:
        args.tickers = ['MSFT', 'AAPL', 'TM']  # Default test set with both US and foreign companies
    
    # If single ticker specified, convert to list for consistency
    if args.ticker:
        args.tickers = [args.ticker]
    
    # Run all combinations of tests for each ticker
    for ticker in args.tickers:
        logging.info(f"\n\n{'='*80}\nTesting {ticker} {args.filing_type}\n{'='*80}")
        filing_data = {'ticker': ticker, 'filing_type': args.filing_type}
        
        if args.test_urls or args.test_all:
            logging.info(f"\n--- Testing URL retrieval for {ticker} ---")
            url_result = test_filing_urls(ticker, args.filing_type)
            
            if url_result:
                # Add results to filing data
                filing_data.update(url_result)
                logging.info("URL test: SUCCESS")
            else:
                logging.error("URL test: FAILED")
                continue  # Skip further tests if we can't get URLs
        
        if args.test_download or args.test_all:
            if 'urls' in filing_data:
                logging.info(f"\n--- Testing XBRL download for {ticker} ---")
                xbrl_result = test_download_xbrl(filing_data)
                
                if xbrl_result:
                    filing_data['xbrl_file'] = xbrl_result.get('file_path')
                    logging.info("XBRL download test: SUCCESS")
                else:
                    logging.error("XBRL download test: FAILED")
                
                logging.info(f"\n--- Testing HTML download for {ticker} ---")
                html_result = test_download_html(filing_data)
                
                if html_result:
                    filing_data['html_file'] = html_result.get('file_path')
                    logging.info("HTML download test: SUCCESS")
                else:
                    logging.error("HTML download test: FAILED")
            else:
                logging.warning("Skipping download tests as URL test was not run or failed")
        
        if args.test_parse or args.test_all:
            if 'xbrl_file' in filing_data:
                logging.info(f"\n--- Testing XBRL parsing for {ticker} ---")
                parse_result = test_parse_xbrl(filing_data['xbrl_file'], filing_data)
                
                if parse_result:
                    filing_data['parsed_data'] = parse_result
                    logging.info(f"XBRL parse test: SUCCESS ({parse_result.get('fact_count', 0)} facts)")
                else:
                    logging.error("XBRL parse test: FAILED")
            else:
                logging.warning("Skipping parse test as download test was not run or failed")
        
        if args.test_process or args.test_all:
            logging.info(f"\n--- Testing enhanced processing for {ticker} ---")
            process_result = test_process_company_filing(ticker, args.filing_type)
            
            if process_result:
                logging.info(f"Enhanced processing test: SUCCESS ({process_result.get('process_path', 'unknown')} path, {process_result.get('fact_count', 0)} facts)")
            else:
                logging.error("Enhanced processing test: FAILED")
    
    # If multiple tickers specified, also test multi-company processing
    if len(args.tickers) > 1 and (args.test_process or args.test_all):
        logging.info(f"\n\n{'='*80}\nTesting multi-company processing\n{'='*80}")
        multi_result = test_process_multi_company(args.tickers, args.filing_type)

if __name__ == "__main__":
    main()