#!/usr/bin/env python3
"""
SEC Document Table Parser

This script analyzes SEC filing index pages to extract document tables
and understand document classification, format identification, and 
selection logic across different companies and filing types.

Usage:
  python sec_table_parser.py --company TICKER [--filing-type TYPE] [--year YEAR]

Examples:
  python sec_table_parser.py --company MSFT --filing-type 10-K --year 2023
  python sec_table_parser.py --company AAPL --filing-type 10-Q
"""

import os
import sys
import re
import json
import time
import logging
import argparse
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sec_table_parser.log'),
        logging.StreamHandler()
    ]
)

# SEC settings
SEC_BASE_URL = "https://www.sec.gov"
USER_AGENT = "Exascale Capital info@exascale.capital"

# Constants
OUTPUT_DIR = "sec_document_analysis"

def sec_request(url, max_retries=3):
    """Make a request to SEC with appropriate rate limiting and retry logic"""
    headers = {'User-Agent': USER_AGENT}
    logging.info(f"Making SEC request to: {url}")
    
    for retry in range(max_retries):
        try:
            time.sleep(0.5 * (retry + 1))  # Increased delay with each retry
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                logging.info(f"SEC request successful: {url}")
                return response
            elif response.status_code == 429:
                wait_time = 2 ** retry  # Exponential backoff
                logging.warning(f"SEC rate limit hit (429). Waiting {wait_time}s before retry {retry+1}/{max_retries}")
                time.sleep(wait_time)
            else:
                logging.warning(f"SEC request failed with status {response.status_code}. Retry {retry+1}/{max_retries}")
                if retry == max_retries - 1:
                    return response  # Return the failing response on last attempt
        except Exception as e:
            logging.error(f"Exception during SEC request: {str(e)}. Retry {retry+1}/{max_retries}")
            if retry == max_retries - 1:
                raise
            
    return response

def get_cik_from_ticker(ticker):
    """Convert ticker symbol to CIK number"""
    logging.info(f"Looking up CIK for ticker: {ticker}")
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany"
    
    response = sec_request(url)
    if response.status_code != 200:
        logging.error(f"Failed to get CIK for {ticker}. Status code: {response.status_code}")
        return None
    
    # Find CIK in the response
    cik_match = re.search(r'CIK=(\d{10})', response.text)
    if not cik_match:
        logging.error(f"CIK pattern not found in response for {ticker}")
        return None
    
    cik = cik_match.group(1)
    logging.info(f"Found CIK for {ticker}: {cik}")
    return cik

def get_filings_for_company(ticker, cik, filing_type=None, year=None):
    """Get recent filings for a company, optionally filtering by type and year"""
    logging.info(f"Getting filings for {ticker} (CIK: {cik})")
    
    # Construct URL with optional filters
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&owner=exclude"
    if filing_type:
        url += f"&type={filing_type}"
    url += "&count=100"  # Get a good number of filings
    
    response = sec_request(url)
    if response.status_code != 200:
        logging.error(f"Failed to get filings for {ticker}. Status code: {response.status_code}")
        return []
    
    # Parse the page to find filing links
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Save for debugging
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(f"{OUTPUT_DIR}/{ticker}_filings.html", 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    # Find the filing table
    filing_tables = []
    for table in soup.find_all('table'):
        if table.get('class') and any(c in ['tableFile2'] for c in table.get('class')):
            filing_tables.append(table)
    
    if not filing_tables:
        logging.error(f"No filing table found for {ticker}")
        return []
    
    filings = []
    
    # Process each table
    for table in filing_tables:
        rows = table.find_all('tr')
        
        # Skip header row
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) < 4:
                continue
            
            # Extract filing data
            filing_type_cell = cells[0].text.strip()
            filing_desc = ""
            if len(cells) > 1:
                filing_desc = cells[1].text.strip()
            
            filing_date = None
            date_cell = cells[3].text.strip()
            try:
                filing_date = datetime.strptime(date_cell, '%Y-%m-%d')
                filing_year = filing_date.year
            except:
                filing_year = None
                filing_date = None
                logging.warning(f"Could not parse date: {date_cell}")
            
            # Apply year filter if specified
            if year and filing_year != int(year):
                continue
            
            # Extract document link
            document_link = None
            document_button = row.find('a', id='documentsbutton')
            if document_button:
                document_link = document_button['href']
                if document_link.startswith('/'):
                    document_link = f"{SEC_BASE_URL}{document_link}"
            
            if document_link:
                filings.append({
                    'type': filing_type_cell,
                    'description': filing_desc,
                    'date': date_cell,
                    'year': filing_year,
                    'document_link': document_link
                })
    
    logging.info(f"Found {len(filings)} filings for {ticker}")
    return filings

def get_filing_documents(document_url):
    """Extract all documents from a filing's index page"""
    logging.info(f"Getting documents from {document_url}")
    
    response = sec_request(document_url)
    if response.status_code != 200:
        logging.error(f"Failed to get document page. Status code: {response.status_code}")
        return {"error": f"Failed to get document page: {response.status_code}"}
    
    # Save raw HTML for analysis
    url_parts = document_url.split('/')
    if len(url_parts) >= 2:
        accession = url_parts[-2]
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(f"{OUTPUT_DIR}/document_page_{accession}.html", 'w', encoding='utf-8') as f:
            f.write(response.text)
    
    # Parse the page
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the document tables - there might be multiple
    document_tables = []
    for table in soup.find_all('table'):
        # Check by class
        if table.get('class') and any(c in ['tableFile', 'tableFile2'] for c in table.get('class')):
            document_tables.append(table)
            continue
            
        # Check by caption
        caption = table.find('caption')
        if caption and any(text in caption.text for text in ['Document Format Files', 'Data Files']):
            document_tables.append(table)
            continue
    
    if not document_tables:
        logging.warning(f"No document tables found for {document_url}")
        # Try looking for ANY table with Document column
        for table in soup.find_all('table'):
            headers = [th.text.strip().lower() for th in table.find_all(['th', 'td'])]
            if 'document' in headers or 'description' in headers:
                document_tables.append(table)
    
    logging.info(f"Found {len(document_tables)} document tables")
    
    # Process all tables
    all_documents = []
    
    for i, table in enumerate(document_tables):
        table_documents = []
        rows = table.find_all('tr')
        
        if len(rows) <= 1:  # Skip empty tables
            continue
        
        # Get header row to identify columns
        header_row = rows[0]
        headers = [th.text.strip().lower() for th in header_row.find_all(['th', 'td'])]
        logging.info(f"Table {i+1} headers: {headers}")
        
        # Determine column indexes
        desc_idx = next((i for i, h in enumerate(headers) if 'description' in h), 0)
        doc_idx = next((i for i, h in enumerate(headers) if 'document' in h), 2)
        type_idx = next((i for i, h in enumerate(headers) if 'type' in h), 3)
        size_idx = next((i for i, h in enumerate(headers) if 'size' in h), 4)
        format_idx = next((i for i, h in enumerate(headers) if 'format' in h), None)
        
        # Process each row
        for row_idx, row in enumerate(rows[1:], 1):  # Skip header
            cells = row.find_all(['th', 'td'])
            if len(cells) <= doc_idx:
                continue  # Skip rows without enough cells
            
            # Extract cell data
            seq_num = row_idx
            description = cells[desc_idx].text.strip() if desc_idx < len(cells) else ""
            doc_type = cells[type_idx].text.strip() if type_idx < len(cells) else ""
            size = cells[size_idx].text.strip() if size_idx < len(cells) else ""
            format_info = cells[format_idx].text.strip() if format_idx and format_idx < len(cells) else ""
            
            # Extract document links
            doc_cell = cells[doc_idx]
            links = doc_cell.find_all('a')
            
            for link in links:
                href = link.get('href')
                link_text = link.text.strip()
                
                if not href:
                    continue
                
                # Construct full URL
                if href.startswith('/'):
                    doc_url = f"{SEC_BASE_URL}{href}"
                elif href.startswith('http'):
                    doc_url = href
                else:
                    # Relative URL, need to derive from document_url
                    base_url = '/'.join(document_url.split('/')[:-1])
                    doc_url = f"{base_url}/{href}"
                
                # Determine format from URL and description
                derived_format = ""
                if 'ix?doc=' in doc_url:
                    derived_format = 'inline XBRL'
                elif doc_url.endswith('.htm') or doc_url.endswith('.html'):
                    derived_format = 'HTML'
                elif doc_url.endswith('.xml'):
                    if 'htm.xml' in doc_url.lower() or 'instance' in description.lower():
                        derived_format = 'XBRL INSTANCE'
                    else:
                        derived_format = 'XML'
                
                # Determine document classification
                is_primary = False
                if doc_type and doc_type.lower() in ['10-k', '10-q', '8-k']:
                    is_primary = True
                elif 'complete submission' in description.lower():
                    is_primary = True
                
                document = {
                    'seq': seq_num,
                    'description': description,
                    'type': doc_type,
                    'link_text': link_text,
                    'url': doc_url,
                    'size': size,
                    'explicit_format': format_info,
                    'derived_format': derived_format,
                    'is_primary': is_primary,
                    'table_number': i + 1
                }
                
                table_documents.append(document)
        
        all_documents.extend(table_documents)
    
    # Extract filing info
    filing_info = {}
    
    # Try to extract from page title
    title = soup.find('title')
    if title:
        filing_info['page_title'] = title.text.strip()
    
    # Try to extract from header
    header_text = ""
    for header in soup.find_all(['h1', 'h2', 'h3']):
        if header.text and len(header.text.strip()) > 5:
            header_text = header.text.strip()
            break
    
    if header_text:
        filing_info['header'] = header_text
    
    # Look for company name
    company_elements = soup.select('span[class*="companyName"]')
    if company_elements:
        filing_info['company_name'] = company_elements[0].text.strip()
    
    return {
        'url': document_url,
        'filing_info': filing_info,
        'document_count': len(all_documents),
        'table_count': len(document_tables),
        'documents': all_documents
    }

def analyze_company_filings(ticker, filing_type=None, year=None, max_filings=3):
    """Analyze document tables for a company's filings"""
    logging.info(f"Analyzing {ticker} filings")
    
    # Step 1: Get CIK
    cik = get_cik_from_ticker(ticker)
    if not cik:
        return {"error": f"Could not find CIK for {ticker}"}
    
    # Step 2: Get filings
    filings = get_filings_for_company(ticker, cik, filing_type, year)
    if not filings:
        return {"error": f"No filings found for {ticker} with specified filters"}
    
    # Limit to max_filings
    filings = filings[:max_filings]
    
    # Step 3: Process each filing
    results = []
    
    for filing in filings:
        document_link = filing['document_link']
        
        # Get documents
        documents_data = get_filing_documents(document_link)
        
        # Add filing metadata
        result = {
            'ticker': ticker,
            'cik': cik,
            'filing_type': filing['type'],
            'filing_date': filing['date'],
            'document_link': document_link,
            'documents_data': documents_data
        }
        
        results.append(result)
        
        # Be nice to SEC servers
        time.sleep(1)
    
    # Save results
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    with open(f"{OUTPUT_DIR}/{ticker}_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    logging.info(f"Analysis completed for {ticker}. Results saved to {OUTPUT_DIR}/{ticker}_{timestamp}.json")
    
    return results

def generate_summary_report(all_results):
    """Generate a summary report across all analyzed companies"""
    summary = {
        'total_companies': len(all_results),
        'total_filings': sum(len(r) for r in all_results.values() if isinstance(r, list)),
        'format_patterns': {},
        'column_patterns': {},
        'document_types': {},
        'file_naming_patterns': {}
    }
    
    # Collect statistics
    for ticker, results in all_results.items():
        if not isinstance(results, list):
            continue
            
        for filing in results:
            documents_data = filing.get('documents_data', {})
            documents = documents_data.get('documents', [])
            
            # Collect format patterns
            for doc in documents:
                # Track format info
                explicit_format = doc.get('explicit_format', '')
                derived_format = doc.get('derived_format', '')
                
                if explicit_format:
                    if explicit_format not in summary['format_patterns']:
                        summary['format_patterns'][explicit_format] = 0
                    summary['format_patterns'][explicit_format] += 1
                
                if derived_format:
                    if f"derived: {derived_format}" not in summary['format_patterns']:
                        summary['format_patterns'][f"derived: {derived_format}"] = 0
                    summary['format_patterns'][f"derived: {derived_format}"] += 1
                
                # Track document types
                doc_type = doc.get('type', '')
                if doc_type:
                    if doc_type not in summary['document_types']:
                        summary['document_types'][doc_type] = 0
                    summary['document_types'][doc_type] += 1
                
                # Track file naming patterns
                url = doc.get('url', '')
                if url:
                    filename = url.split('/')[-1]
                    pattern = re.sub(r'\d+', 'N', filename)
                    pattern = re.sub(r'[a-zA-Z]+', 'X', pattern)
                    
                    if pattern not in summary['file_naming_patterns']:
                        summary['file_naming_patterns'][pattern] = {
                            'count': 0,
                            'examples': []
                        }
                    
                    summary['file_naming_patterns'][pattern]['count'] += 1
                    if len(summary['file_naming_patterns'][pattern]['examples']) < 3:
                        summary['file_naming_patterns'][pattern]['examples'].append(filename)
    
    # Sort results by frequency
    summary['format_patterns'] = dict(sorted(summary['format_patterns'].items(), 
                                           key=lambda x: x[1], reverse=True))
    summary['document_types'] = dict(sorted(summary['document_types'].items(), 
                                           key=lambda x: x[1], reverse=True))
    summary['file_naming_patterns'] = dict(sorted(summary['file_naming_patterns'].items(), 
                                                key=lambda x: x[1]['count'], reverse=True))
    
    # Save summary
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    with open(f"{OUTPUT_DIR}/summary_report_{timestamp}.json", 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    # Create Excel report for easier analysis
    df_documents = []
    
    for ticker, results in all_results.items():
        if not isinstance(results, list):
            continue
            
        for filing in results:
            filing_type = filing.get('filing_type', '')
            filing_date = filing.get('filing_date', '')
            
            documents_data = filing.get('documents_data', {})
            documents = documents_data.get('documents', [])
            
            for doc in documents:
                row = {
                    'ticker': ticker,
                    'filing_type': filing_type,
                    'filing_date': filing_date,
                    'seq': doc.get('seq', ''),
                    'description': doc.get('description', ''),
                    'type': doc.get('type', ''),
                    'explicit_format': doc.get('explicit_format', ''),
                    'derived_format': doc.get('derived_format', ''),
                    'is_primary': doc.get('is_primary', False),
                    'filename': doc.get('url', '').split('/')[-1] if doc.get('url', '') else '',
                    'size': doc.get('size', '')
                }
                df_documents.append(row)
    
    if df_documents:
        df = pd.DataFrame(df_documents)
        df.to_excel(f"{OUTPUT_DIR}/document_analysis_{timestamp}.xlsx", index=False)
    
    logging.info(f"Summary report generated and saved to {OUTPUT_DIR}/summary_report_{timestamp}.json")
    logging.info(f"Excel report saved to {OUTPUT_DIR}/document_analysis_{timestamp}.xlsx")
    
    return summary

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Analyze SEC filing document tables')
    parser.add_argument('--company', help='Company ticker symbol')
    parser.add_argument('--companies', help='Comma-separated list of company tickers')
    parser.add_argument('--filing-type', help='Filing type (10-K, 10-Q, etc.)')
    parser.add_argument('--year', help='Filter by filing year')
    parser.add_argument('--max-filings', type=int, default=3, help='Maximum filings to analyze per company')
    
    args = parser.parse_args()
    
    companies = []
    if args.company:
        companies.append(args.company)
    if args.companies:
        companies.extend([c.strip() for c in args.companies.split(',')])
    
    if not companies:
        # Default companies list if none provided
        companies = [
            "MSFT", "AAPL", "JPM", "WMT", "TM", "PYPL", "LOW", "CMI", 
            "BCS", "LNVGY", "INOD", "POWL", "HIBB", "NTB", "ERJ"
        ]
    
    # Analyze each company
    all_results = {}
    
    for ticker in companies:
        logging.info(f"Processing {ticker}")
        results = analyze_company_filings(ticker, args.filing_type, args.year, args.max_filings)
        all_results[ticker] = results
        
        # Be nice to SEC servers
        time.sleep(2)
    
    # Generate summary report
    summary = generate_summary_report(all_results)
    
    logging.info("Analysis complete!")
    
if __name__ == "__main__":
    main()