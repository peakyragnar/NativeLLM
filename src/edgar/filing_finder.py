# src/edgar/filing_finder.py
import os
import sys
import logging
import time
from bs4 import BeautifulSoup
import re

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.edgar.edgar_utils import sec_request
from src.config import SEC_BASE_URL

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

def find_company_filings(ticker, filing_types):
    """Find filings for a company"""
    from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
    
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
    
    # Find filings for each type
    for filing_type in filing_types:
        instance_url = get_latest_filing_url(cik, filing_type)
        if instance_url:
            metadata = get_filing_metadata(cik, filing_type, instance_url)
            results["filings"][filing_type] = metadata
    
    return results