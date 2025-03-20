# src/edgar/filing_finder.py
import os
import sys
import logging
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
    index_response = sec_request(index_url)
    
    if index_response.status_code != 200:
        return None
    
    # Parse the page to find the latest filing
    soup = BeautifulSoup(index_response.text, 'html.parser')
    filing_tables = soup.select('#filingsTable')
    
    if not filing_tables:
        return None
    
    # Find the document link for the first filing
    filing_links = soup.select('a[id="documentsbutton"]')
    if not filing_links:
        return None
    
    # Get the documents page URL
    documents_url = SEC_BASE_URL + filing_links[0]['href']
    
    # Get the documents page
    documents_response = sec_request(documents_url)
    if documents_response.status_code != 200:
        return None
    
    # Parse the documents page to find the XBRL/XML file
    doc_soup = BeautifulSoup(documents_response.text, 'html.parser')
    
    # Look for XBRL or XML instance document
    instance_link = None
    
    # First, try to find a file with _htm.xml (which is usually the instance document)
    htm_xml_links = [a for a in doc_soup.select('a') if re.search(r'_htm\.xml$', a.text)]
    if htm_xml_links:
        instance_link = htm_xml_links[0]
    
    # If not found, look for any XML or XBRL file
    if not instance_link:
        xml_links = [a for a in doc_soup.select('a') if a.text.endswith('.xml') or a.text.endswith('.xbrl')]
        if xml_links:
            instance_link = xml_links[0]
    
    if not instance_link:
        return None
    
    # Get the full URL to the instance document
    instance_url = SEC_BASE_URL + instance_link['href']
    return instance_url

def get_filing_metadata(cik, filing_type, instance_url):
    """Extract metadata about the filing"""
    # Extract accession number from URL
    accession_match = re.search(r'(\d{10}-\d{2}-\d{6})', instance_url)
    if not accession_match:
        return None
    
    accession_number = accession_match.group(1)
    
    # Get the filing summary to extract more metadata
    summary_url = instance_url.replace('_htm.xml', '_FilingSummary.xml')
    summary_response = sec_request(summary_url)
    
    filing_date = None
    period_end_date = None
    
    if summary_response.status_code == 200:
        try:
            soup = BeautifulSoup(summary_response.text, 'lxml-xml')
            filing_date_elem = soup.find('Accepted')
            if filing_date_elem:
                filing_date = filing_date_elem.text.split()[0]
            
            period_elem = soup.find('PeriodOfReport')
            if period_elem:
                period_end_date = period_elem.text
        except:
            pass
    
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