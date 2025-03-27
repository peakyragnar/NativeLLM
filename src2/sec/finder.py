#!/usr/bin/env python3
"""
SEC Filing Finder Module

Module for finding and locating SEC filings on the EDGAR database.
"""

import os
import sys
import logging
import time
import re
import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Import from our config
from .. import config
from ..edgar.edgar_utils import sec_request, get_cik_from_ticker, get_company_name_from_cik

# Constants from config
SEC_BASE_URL = config.SEC_BASE_URL

def get_filing_index_url(cik, filing_type):
    """Get the URL for the filing index page"""
    return f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}"

def get_latest_filing_url(cik, filing_type):
    """Get the URL for the latest filing of the specified type"""
    # Get the filing index page
    index_url = get_filing_index_url(cik, filing_type)
    logging.info(f"Getting filing index for {filing_type}: {index_url}")
    index_response = sec_request(index_url)
    
    if index_response.status_code != 200:
        logging.error(f"Failed to get filing index. Status code: {index_response.status_code}")
        return None
    
    # Save the page for debug purposes
    with open(f"debug_{cik}_{filing_type}_index.html", "w", encoding="utf-8") as f:
        f.write(index_response.text)
    logging.info(f"Saved index page to debug_{cik}_{filing_type}_index.html")
    
    # Parse the page to find the latest filing
    soup = BeautifulSoup(index_response.text, 'html.parser')
    
    # The SEC EDGAR page now uses tableFile2 class instead of filingsTable id
    filing_tables = soup.select('table.tableFile2')
    
    if not filing_tables:
        logging.error(f"No filing table found for {cik} {filing_type}")
        # Try additional selectors as fallback
        filing_tables = soup.select('.tableFile') 
        if not filing_tables:
            filing_tables = soup.select('table[summary="Results"]')
            if not filing_tables:
                return None
    
    logging.info(f"Found filing table: {filing_tables[0]['class'] if filing_tables[0].has_attr('class') else 'unknown class'}")
    
    # Find the document link for the first filing - still using documentsbutton id
    filing_links = soup.select('a[id="documentsbutton"]')
    
    if not filing_links:
        logging.warning(f"No documents button found with id. Trying alternative selectors")
        # Try alternative selectors
        filing_links = soup.select('a.documentsbutton')
        if not filing_links:
            filing_links = soup.select('a:contains("Documents")')
            if not filing_links:
                filing_links = soup.find_all('a', text=lambda t: t and 'Document' in t)
                if not filing_links:
                    logging.error(f"Could not find any document links")
                    return None
    
    logging.info(f"Found {len(filing_links)} document links")
    
    # Print the found link for debugging
    logging.info(f"Found document link: {filing_links[0]}")
    
    # Get the documents page URL
    documents_url = SEC_BASE_URL + filing_links[0]['href']
    
    # Get the documents page
    documents_response = sec_request(documents_url)
    if documents_response.status_code != 200:
        return None
    
    # Save the documents page for debugging
    with open(f"debug_{cik}_{filing_type}_documents.html", "w", encoding="utf-8") as f:
        f.write(documents_response.text)
    logging.info(f"Saved documents page to debug_{cik}_{filing_type}_documents.html")
    
    # Parse the documents page to find the XBRL/XML file
    doc_soup = BeautifulSoup(documents_response.text, 'html.parser')
    
    # Look for XBRL or XML instance document
    instance_link = None
    
    # First, look for the table that contains file listings
    tables = doc_soup.select('table.tableFile')
    if not tables:
        tables = doc_soup.select('table')
        logging.warning("Could not find tableFile class, trying all tables")
    
    if tables:
        logging.info(f"Found {len(tables)} tables in document page")
        
        # First, try to find a file with _htm.xml (which is usually the instance document)
        htm_xml_links = []
        for table in tables:
            links = [a for a in table.select('a') if re.search(r'_htm\.xml$', a.text)]
            htm_xml_links.extend(links)
        
        if htm_xml_links:
            instance_link = htm_xml_links[0]
            logging.info(f"Found htm.xml link: {instance_link.text}")
        
        # If not found, look for any XML or XBRL file
        if not instance_link:
            xml_links = []
            for table in tables:
                links = [a for a in table.select('a') if a.text.endswith('.xml') or a.text.endswith('.xbrl')]
                xml_links.extend(links)
            
            if xml_links:
                instance_link = xml_links[0]
                logging.info(f"Found XML/XBRL link: {instance_link.text}")
    else:
        # Fallback to all links in the page
        htm_xml_links = [a for a in doc_soup.select('a') if re.search(r'_htm\.xml$', a.text)]
        if htm_xml_links:
            instance_link = htm_xml_links[0]
            logging.info(f"Found htm.xml link via fallback: {instance_link.text}")
        
        # If not found, look for any XML or XBRL file
        if not instance_link:
            xml_links = [a for a in doc_soup.select('a') if a.text.endswith('.xml') or a.text.endswith('.xbrl')]
            if xml_links:
                instance_link = xml_links[0]
                logging.info(f"Found XML/XBRL link via fallback: {instance_link.text}")
    
    if not instance_link:
        logging.error("Could not find any instance documents (XML/XBRL)")
        return None
    
    # Get the full URL to the instance document
    instance_url = SEC_BASE_URL + instance_link['href']
    return instance_url

def get_filing_metadata(cik, filing_type, instance_url):
    """Extract metadata about the filing"""
    logging.info(f"Extracting metadata for {filing_type} from {instance_url}")
    
    # Extract accession number from URL
    accession_match = re.search(r'(\d{10}-\d{2}-\d{6})', instance_url)
    if not accession_match:
        logging.error(f"Could not extract accession number from URL: {instance_url}")
        # Try an alternative regex
        accession_match = re.search(r'/(\d+)/([^/]+)_htm\.xml', instance_url)
        if accession_match:
            logging.info(f"Found accession from alternative pattern")
            accession_number = f"{accession_match.group(1)}-{accession_match.group(2)}"
        else:
            # Create a fallback accession number based on file path
            file_parts = instance_url.split('/')
            if len(file_parts) > 1:
                accession_number = f"ACCN-{file_parts[-2]}-{file_parts[-1]}"
                logging.info(f"Using fallback accession number: {accession_number}")
            else:
                accession_number = f"ACCN-UNKNOWN-{int(time.time())}"
                logging.warning(f"Using timestamp-based accession number: {accession_number}")
    else:
        accession_number = accession_match.group(1)
        logging.info(f"Extracted accession number: {accession_number}")
    
    # Get the filing summary to extract more metadata
    summary_url = instance_url.replace('_htm.xml', '_FilingSummary.xml')
    logging.info(f"Getting filing summary from: {summary_url}")
    summary_response = sec_request(summary_url)
    
    filing_date = None
    period_end_date = None
    
    if summary_response.status_code == 200:
        try:
            # Save the summary for debugging
            with open(f"debug_{cik}_{filing_type}_summary.xml", "w", encoding="utf-8") as f:
                f.write(summary_response.text)
            logging.info(f"Saved summary to debug_{cik}_{filing_type}_summary.xml")
            
            soup = BeautifulSoup(summary_response.text, 'lxml-xml')
            filing_date_elem = soup.find('Accepted')
            if filing_date_elem:
                filing_date = filing_date_elem.text.split()[0]
                logging.info(f"Found filing date: {filing_date}")
            else:
                logging.warning("Could not find 'Accepted' element in summary")
                # Try alternative fields
                filing_date_elem = soup.find('AcceptanceDateTime')
                if filing_date_elem:
                    filing_date = filing_date_elem.text.split()[0]
                    logging.info(f"Found filing date from AcceptanceDateTime: {filing_date}")
            
            period_elem = soup.find('PeriodOfReport')
            if period_elem:
                period_end_date = period_elem.text
                logging.info(f"Found period end date: {period_end_date}")
            else:
                logging.warning("Could not find 'PeriodOfReport' element in summary")
                # Try alternative fields
                period_elem = soup.find('BalanceSheetDate')
                if period_elem:
                    period_end_date = period_elem.text
                    logging.info(f"Found period end date from BalanceSheetDate: {period_end_date}")
        except Exception as e:
            logging.error(f"Error parsing summary XML: {str(e)}")
            # Don't return None, provide some fallback values
            
    # If we still don't have dates, set defaults
    if not filing_date:
        filing_date = time.strftime("%Y-%m-%d")
        logging.warning(f"Using current date as filing date: {filing_date}")
        
    if not period_end_date:
        # Try to extract date from file name
        date_match = re.search(r'-(\d{8})_', instance_url)
        if date_match:
            date_str = date_match.group(1)
            period_end_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            logging.info(f"Extracted period end date from filename: {period_end_date}")
        else:
            # Use a default quarter-end date
            period_end_date = filing_date  # Use filing date as fallback
            logging.warning(f"Using filing date as period end date: {period_end_date}")
    
    return {
        "cik": cik,
        "accession_number": accession_number,
        "filing_type": filing_type,
        "filing_date": filing_date,
        "period_end_date": period_end_date,
        "instance_url": instance_url
    }

def find_company_filings(ticker, filing_types, specific_url=None):
    """Find filings for a company
    
    Args:
        ticker: Company ticker symbol
        filing_types: List of filing types to look for
        specific_url: Optional URL to a specific filing document page
        
    Returns:
        Dictionary with filing information
    """
    # Get CIK from ticker
    cik = get_cik_from_ticker(ticker)
    if not cik:
        return {"error": f"Could not find CIK for ticker {ticker}"}
    
    # Get company name
    company_name = get_company_name_from_cik(cik)
    
    results = {
        "ticker": ticker,
        "cik": cik,
        "company_name": company_name,
        "filings": {}
    }
    
    # If a specific URL is provided, process just that filing
    if specific_url:
        logging.info(f"Processing specific filing URL: {specific_url}")
        # Extract filing type from URL or use the first one in the list
        filing_type = filing_types[0] if filing_types else "10-K"
        
        # Use the specific URL directly
        try:
            response = sec_request(specific_url)
            if response.status_code == 200:
                # Save the document page for debugging
                debug_file = f"debug_{cik}_{filing_type}_specific_documents.html"
                with open(debug_file, "w") as f:
                    f.write(response.text)
                
                # Parse the HTML to find the XBRL instance document
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for XML links
                instance_url = None
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href and (href.endswith('_htm.xml') or href.endswith('.xml')):
                        # This is likely an XBRL instance document
                        instance_url = "https://www.sec.gov" + href
                        break
                
                if instance_url:
                    logging.info(f"Found XBRL instance at {instance_url}")
                    metadata = get_filing_metadata(cik, filing_type, instance_url)
                    
                    # Include the document URL for HTML extraction
                    metadata["document_url"] = specific_url
                    
                    results["filings"][filing_type] = metadata
                else:
                    logging.warning(f"Could not find instance document in {specific_url}")
            else:
                logging.warning(f"Failed to access {specific_url}: {response.status_code}")
        except Exception as e:
            logging.error(f"Error processing specific URL: {str(e)}")
    else:
        # Find filings for each type (standard behavior)
        for filing_type in filing_types:
            instance_url = get_latest_filing_url(cik, filing_type)
            if instance_url:
                metadata = get_filing_metadata(cik, filing_type, instance_url)
                results["filings"][filing_type] = metadata
    
    return results

def find_filings_by_cik(cik, filing_type, start_date=None, end_date=None, limit=10):
    """
    Find filings for a company by CIK within a date range
    
    Args:
        cik: Company CIK (10-digit string with leading zeros)
        filing_type: Filing type (10-K, 10-Q, etc.)
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        limit: Maximum number of filings to return
        
    Returns:
        List of filing metadata dictionaries
    """
    # Ensure CIK is properly formatted (10 digits with leading zeros)
    cik = cik.zfill(10) if cik.isdigit() else cik
    
    # Build the URL for the filing index page
    base_url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar"
    params = {
        "CIK": cik,
        "type": filing_type,
        "owner": "exclude",
        "action": "getcompany",
        "count": limit
    }
    
    url = f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    
    # Make the request
    response = sec_request(url)
    if response.status_code != 200:
        return []
    
    # Parse the page to find filings
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the filing table - try different selectors
    filing_tables = soup.select('.tableFile2') or soup.select('.tableFile') or soup.select('table[summary="Results"]')
    if not filing_tables:
        return []
    
    filings = []
    
    # Parse each filing row
    filing_rows = filing_tables[0].select('tr')
    for row in filing_rows[1:]:  # Skip header row
        cells = row.select('td')
        if len(cells) < 4:
            continue
        
        # Extract filing date
        filing_date_text = cells[3].get_text().strip()
        try:
            filing_date = datetime.datetime.strptime(filing_date_text, '%Y-%m-%d').date()
        except ValueError:
            # Skip if date format is unexpected
            continue
        
        # Apply date filtering if provided
        if start_date:
            try:
                start = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
                if filing_date < start:
                    continue
            except ValueError:
                pass
                
        if end_date:
            try:
                end = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
                if filing_date > end:
                    continue
            except ValueError:
                pass
        
        # Get filing type (to confirm it matches what we want)
        filing_type_text = cells[0].get_text().strip()
        if filing_type.lower() not in filing_type_text.lower():
            continue
        
        # Get filing links
        filing_link = None
        document_link = None
        
        # Find the document links - try multiple selectors
        links = cells[1].select('a')
        for link in links:
            if 'filings' in link.get('href', '') or 'Archives' in link.get('href', ''):
                filing_link = SEC_BASE_URL + link['href']
            elif 'document' in link.get('id', '') or 'documentsbutton' in link.get('id', ''):
                document_link = SEC_BASE_URL + link['href']
        
        if not document_link:
            # Try alternative methods to find the document link
            for link in links:
                if 'Documents' in link.get_text() or 'Document' in link.get_text():
                    document_link = SEC_BASE_URL + link['href']
                    break
        
        if not document_link:
            continue
        
        # Try to extract accession number
        accession_number = None
        
        # From document link
        accession_match = re.search(r'accession_number=(\d{10}-\d{2}-\d{6})', document_link)
        if accession_match:
            accession_number = accession_match.group(1)
        
        # From filing link if we have it
        if not accession_number and filing_link:
            accession_match = re.search(r'accession_number=(\d{10}-\d{2}-\d{6})', filing_link)
            if accession_match:
                accession_number = accession_match.group(1)
                
        # If still not found, try to extract from document link format
        if not accession_number:
            # Format likely: .../Archives/edgar/data/789019/000156459017022434/...
            parts_match = re.search(r'/data/\d+/(\d+)/', document_link)
            if parts_match:
                acc_number = parts_match.group(1)
                # Format into SEC accession number if possible
                if len(acc_number) == 12:  # Should be 000XXXXXXXXX format
                    accession_number = f"{acc_number[:10]}-{acc_number[10:12]}-{acc_number[12:]}"
                else:
                    # Use as is
                    accession_number = acc_number
        
        # Get instance URL by visiting the documents page
        instance_url = None
        period_end_date = None
        
        # Visit the document page to find instance document and more metadata
        doc_response = sec_request(document_link)
        if doc_response.status_code == 200:
            doc_soup = BeautifulSoup(doc_response.text, 'html.parser')
            
            # Try to find period of report if not already found
            if not period_end_date:
                period_labels = doc_soup.find_all(string=re.compile(r'Period of Report', re.IGNORECASE))
                if period_labels:
                    for label in period_labels:
                        parent = label.parent
                        if parent:
                            # Look for a sibling with the date
                            next_sibling = parent.next_sibling
                            if next_sibling:
                                period_text = next_sibling.get_text().strip()
                                # Try to parse as date
                                try:
                                    # Could be in various formats
                                    for fmt in ['%Y%m%d', '%Y-%m-%d', '%m/%d/%Y']:
                                        try:
                                            period_date = datetime.datetime.strptime(period_text, fmt)
                                            period_end_date = period_date.strftime('%Y-%m-%d')
                                            break
                                        except ValueError:
                                            continue
                                except:
                                    pass
            
            # Look for XBRL or XML instance document
            # First try to find a file with _htm.xml (which is usually the instance document)
            htm_xml_links = [a for a in doc_soup.select('a') if re.search(r'_htm\.xml$', a.text)]
            if htm_xml_links:
                instance_url = SEC_BASE_URL + htm_xml_links[0]['href']
            else:
                # If not found, look for any XML or XBRL file
                xml_links = [a for a in doc_soup.select('a') if a.text.endswith('.xml') or a.text.endswith('.xbrl')]
                if xml_links:
                    instance_url = SEC_BASE_URL + xml_links[0]['href']
        
        # Create filing metadata
        filing_metadata = {
            "cik": cik,
            "accession_number": accession_number,
            "filing_type": filing_type,
            "filing_date": filing_date_text,
            "period_end_date": period_end_date,
            "instance_url": instance_url,
            "filing_link": filing_link,
            "document_link": document_link
        }
        
        filings.append(filing_metadata)
    
    return filings