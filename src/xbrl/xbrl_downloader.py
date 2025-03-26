"""
XBRL downloader module that uses the direct SEC approach.

This module provides functions to download XBRL and HTML documents 
from SEC EDGAR using a direct approach that properly traverses the
SEC website structure rather than guessing URL patterns.
"""

import os
import sys
import logging
import requests
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.edgar.edgar_utils import sec_request
from src.config import RAW_DATA_DIR, SEC_BASE_URL, SEC_ARCHIVE_URL

def get_filing_urls(cik, filing_type):
    """
    Get URLs for the latest filing of a specific type
    
    Args:
        cik: Company CIK
        filing_type: Filing type (10-K, 10-Q, 20-F)
        
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
    
    # Extract filing date from the document's metadata
    filing_date = None
    try:
        # Try to find filing date in the header info
        filing_info = doc_soup.select_one('div.formGrouping')
        if filing_info:
            date_row = filing_info.find(string=re.compile('Filing Date'))
            if date_row:
                date_cell = date_row.find_parent('div').find_next_sibling('div')
                if date_cell:
                    filing_date = date_cell.text.strip()
    except Exception as e:
        logging.warning(f"Error extracting filing date: {str(e)}")
    
    # Extract period end date from the document's metadata
    period_end_date = None
    try:
        # Try to find period of report in the header info
        period_info = doc_soup.select_one('div.formGrouping')
        if period_info:
            period_row = period_info.find(string=re.compile('Period of Report'))
            if period_row:
                period_cell = period_row.find_parent('div').find_next_sibling('div')
                if period_cell:
                    period_end_date = period_cell.text.strip()
    except Exception as e:
        logging.warning(f"Error extracting period end date: {str(e)}")
    
    # Ensure document_url and primary_doc_url are always the same for consistency
    return {
        'documents_url': documents_url,
        'xbrl_url': xbrl_url,
        'primary_doc_url': primary_doc_url,
        'document_url': primary_doc_url,  # Always set document_url to same value as primary_doc_url
        'accession_number': accession_number,
        'filing_date': filing_date,
        'period_end_date': period_end_date
    }

def download_xbrl_instance(filing_metadata):
    """
    Download XBRL instance document for a filing
    
    Args:
        filing_metadata: Dictionary with filing metadata including URLs
        
    Returns:
        Dictionary with download results
    """
    # Get necessary info from metadata
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    
    # Check if we already have the XBRL URL
    if "xbrl_url" in filing_metadata:
        xbrl_url = filing_metadata["xbrl_url"]
    # If we don't have the XBRL URL but have other information, try to get it
    elif "cik" in filing_metadata:
        # Get URLs for this filing
        cik = filing_metadata["cik"]
        results = get_filing_urls(cik, filing_type)
        
        if "error" in results:
            return {"error": results["error"]}
        
        xbrl_url = results.get("xbrl_url")
        
        # Update metadata with new information
        if "filing_date" not in filing_metadata and "filing_date" in results:
            filing_metadata["filing_date"] = results["filing_date"]
        
        if "period_end_date" not in filing_metadata and "period_end_date" in results:
            filing_metadata["period_end_date"] = results["period_end_date"]
        
        if "accession_number" not in filing_metadata and "accession_number" in results:
            filing_metadata["accession_number"] = results["accession_number"]
        
        # Make sure both primary_doc_url and document_url are set and synchronized
        if "primary_doc_url" in results:
            primary_doc_url = results["primary_doc_url"]
            filing_metadata["primary_doc_url"] = primary_doc_url
            filing_metadata["document_url"] = primary_doc_url
            logging.info(f"Updated both primary_doc_url and document_url to: {primary_doc_url}")
    else:
        return {"error": "No XBRL URL or CIK provided in filing metadata"}
    
    # If we still don't have an XBRL URL, return an error
    if not xbrl_url:
        return {"error": "Could not determine XBRL URL"}
    
    # Update the metadata with the XBRL URL
    filing_metadata["xbrl_url"] = xbrl_url
    
    # Create directory path
    accession_number = filing_metadata.get("accession_number", "unknown")
    
    try:
        # Create directory structure
        dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
        os.makedirs(dir_path, exist_ok=True)
        
        # Generate filename from URL or accession number
        if xbrl_url:
            file_name = os.path.basename(xbrl_url)
            if not file_name.endswith('.xml'):
                file_name = f"{accession_number}_instance.xml"
        else:
            file_name = f"{accession_number}_instance.xml"
        
        file_path = os.path.join(dir_path, file_name)
        
        # Check if file already exists and is non-empty
        if os.path.exists(file_path) and os.path.getsize(file_path) > 100:
            logging.info(f"XBRL file already exists: {file_path}")
            return {
                "success": True,
                "file_path": file_path,
                "size": os.path.getsize(file_path),
                "from_cache": True
            }
        
        # Download the file
        logging.info(f"Downloading XBRL from {xbrl_url}")
        response = sec_request(xbrl_url)
        
        if response.status_code != 200:
            return {"error": f"Failed to download XBRL: {response.status_code}"}
        
        # Validate the response is XML
        content = response.content
        if len(content) < 100:
            return {"error": "Downloaded XBRL file is too small"}
        
        # Save the file
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # Also save a debug version for inspection
        debug_path = os.path.join(dir_path, f"debug_{file_name}")
        with open(debug_path, 'wb') as f:
            f.write(content)
        
        logging.info(f"Saved XBRL to {file_path}")
        return {
            "success": True,
            "file_path": file_path,
            "size": len(content)
        }
    except Exception as e:
        return {"error": f"Exception downloading XBRL: {str(e)}"}

def download_html_filing(filing_metadata):
    """
    Download HTML filing document
    
    Args:
        filing_metadata: Dictionary with filing metadata including URLs
        
    Returns:
        Dictionary with download results
    """
    # Get necessary info from metadata
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    
    # Check if we already have the HTML URL
    if "primary_doc_url" in filing_metadata:
        html_url = filing_metadata["primary_doc_url"]
        # Ensure document_url is also set for compatibility
        if "document_url" not in filing_metadata:
            filing_metadata["document_url"] = html_url
    # If we have document_url, use that (from previous workflow)
    elif "document_url" in filing_metadata:
        html_url = filing_metadata["document_url"]
        # Ensure primary_doc_url is also set for compatibility
        if "primary_doc_url" not in filing_metadata:
            filing_metadata["primary_doc_url"] = html_url
    # If we don't have the HTML URL but have other information, try to get it
    elif "cik" in filing_metadata:
        # Get URLs for this filing
        cik = filing_metadata["cik"]
        results = get_filing_urls(cik, filing_type)
        
        if "error" in results:
            return {"error": results["error"]}
        
        html_url = results.get("primary_doc_url")
        
        # Update metadata with new information
        if "filing_date" not in filing_metadata and "filing_date" in results:
            filing_metadata["filing_date"] = results["filing_date"]
        
        if "period_end_date" not in filing_metadata and "period_end_date" in results:
            filing_metadata["period_end_date"] = results["period_end_date"]
        
        if "accession_number" not in filing_metadata and "accession_number" in results:
            filing_metadata["accession_number"] = results["accession_number"]
        
        if "xbrl_url" not in filing_metadata and "xbrl_url" in results:
            filing_metadata["xbrl_url"] = results["xbrl_url"]
    else:
        return {"error": "No HTML URL or CIK provided in filing metadata"}
    
    # If we still don't have an HTML URL, return an error
    if not html_url:
        return {"error": "Could not determine HTML URL"}
    
    # Update the metadata with the HTML URL
    filing_metadata["primary_doc_url"] = html_url
    
    try:
        # Create directory structure
        dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
        os.makedirs(dir_path, exist_ok=True)
        
        # Generate filename from URL or ticker/filing_type
        accession_number = filing_metadata.get("accession_number", "unknown")
        if html_url:
            file_name = os.path.basename(html_url)
            if not file_name.endswith('.htm') and not file_name.endswith('.html'):
                file_name = f"{ticker}-{accession_number}.htm"
        else:
            file_name = f"{ticker}-{accession_number}.htm"
        
        file_path = os.path.join(dir_path, file_name)
        
        # Check if file already exists and is non-empty
        if os.path.exists(file_path) and os.path.getsize(file_path) > 1000:
            logging.info(f"HTML file already exists: {file_path}")
            return {
                "success": True,
                "file_path": file_path,
                "size": os.path.getsize(file_path),
                "from_cache": True
            }
        
        # Download the file
        logging.info(f"Downloading HTML from {html_url}")
        response = sec_request(html_url)
        
        if response.status_code != 200:
            return {"error": f"Failed to download HTML: {response.status_code}"}
        
        content = response.content
        if len(content) < 1000:
            return {"error": "Downloaded HTML file is too small"}
        
        # Save the file
        with open(file_path, 'wb') as f:
            f.write(content)
        
        logging.info(f"Saved HTML to {file_path}")
        return {
            "success": True,
            "file_path": file_path,
            "size": len(content)
        }
    except Exception as e:
        return {"error": f"Exception downloading HTML: {str(e)}"}