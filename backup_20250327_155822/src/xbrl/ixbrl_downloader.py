"""
Module for downloading HTML documents containing iXBRL data from SEC EDGAR.
"""

import os
import sys
import re
import json
import time
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.edgar.edgar_utils import sec_request
from src.config import RAW_DATA_DIR

def standardize_accession_number(accession_number):
    """Standardize accession number format to ensure consistent URL construction"""
    if not accession_number:
        return None
    
    # For URL construction, we typically want the format without hyphens
    if "-" in accession_number:
        return accession_number.replace("-", "")
    
    return accession_number

def find_html_document_url(filing_metadata):
    """Find the HTML document URL associated with a filing"""
    # Extract necessary data from metadata
    accession_number = filing_metadata.get("accession_number")
    cik = filing_metadata.get("cik")
    
    if not accession_number or not cik:
        return None
    
    # Remove dashes from accession number
    clean_accn = standardize_accession_number(accession_number)
    
    # Remove leading zeros from CIK for URL construction
    cik_no_zeros = cik.lstrip('0')
    
    # First try the direct filing URL (common format)
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/"
    
    # Check if this is a 10-K or 10-Q filing (common naming patterns)
    filing_type = filing_metadata.get("filing_type", "").lower()
    
    # Try potential HTML file names based on common patterns
    potential_file_names = []
    
    # First, try to extract the report date
    if filing_metadata.get("period_end_date"):
        # Try to use the period_end_date
        date_parts = filing_metadata.get("period_end_date").split("-")
        if len(date_parts) == 3:
            year, month, day = date_parts
            # Common pattern: ticker-YYYYMMDD.htm
            if "ticker" in filing_metadata:
                ticker = filing_metadata.get('ticker', '').lower()
                potential_file_names.append(f"{ticker}-{year}{month}{day}.htm")
    
    # Add common naming patterns
    potential_file_names.extend([
        f"{filing_type.lower()}.htm",                   # 10-k.htm
        f"{clean_accn}.htm",                            # 0001652044XXXXXXXX.htm
        f"form{filing_type.lower()}.htm",               # form10-k.htm
        f"{filing_type.lower().replace('-', '')}.htm",  # 10k.htm
        "report.htm",                                    # report.htm
        "filing.htm"                                     # filing.htm
    ])

    # Try each potential file directly
    for file_name in potential_file_names:
        url = f"{base_url}{file_name if file_name.startswith('/') else file_name}"
        response = sec_request(url)
        if response and response.status_code == 200:
            return url
    
    # If direct approach fails, try the index page to find the HTML document
    index_url = f"{base_url}index.json"
    response = sec_request(index_url)
    
    if response and response.status_code == 200:
        try:
            index_data = response.json()
            if "directory" in index_data and "item" in index_data["directory"]:
                for item in index_data["directory"]["item"]:
                    if "name" in item and item["name"].lower().endswith(".htm"):
                        return f"{base_url}{item['name'] if item['name'].startswith('/') else item['name']}"
        except Exception as e:
            print(f"Error parsing index JSON: {str(e)}")
    
    # If direct approach fails, try the index page to find the HTML document
    index_url = f"{base_url}index.json"
    response = sec_request(index_url)
    
    if response and response.status_code == 200:
        try:
            index_data = response.json()
            if "directory" in index_data and "item" in index_data["directory"]:
                # First try to find a file that matches common 10-K/10-Q naming patterns
                for item in index_data["directory"]["item"]:
                    if "name" in item and item["name"].lower().endswith(".htm"):
                        name = item["name"].lower()
                        # Prioritize files that match common filing patterns
                        if "10-k" in name or "10k" in name or "10-q" in name or "10q" in name:
                            return f"{base_url}{item['name']}"
                
                # If no priority match, return the first .htm file
                for item in index_data["directory"]["item"]:
                    if "name" in item and item["name"].lower().endswith(".htm"):
                        return f"{base_url}{item['name']}"
        except Exception as e:
            print(f"Error parsing index JSON: {str(e)}")
    
    # If all else fails, try the SEC's iXBRL viewer URL
    cik_no_zeros = cik.lstrip('0')
    period_end = filing_metadata.get('period_end_date', '').replace('-', '')
    ticker = filing_metadata.get('ticker', '').lower()
    
    # Try some common naming patterns
    possible_urls = []
    
    # 1. Company ticker pattern (most common)
    possible_urls.append(f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/{ticker}-{period_end}.htm")
    
    # 2. For Google, they often use "goog" regardless of ticker
    if ticker.upper() in ["GOOGL", "GOOG"]:
        possible_urls.append(f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/goog-{period_end}.htm")
    
    # 3. Generic patterns
    possible_urls.append(f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/10-k.htm")
    possible_urls.append(f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/10-q.htm")
    
    # 4. Try each URL
    for url in possible_urls:
        response = sec_request(url)
        if response and response.status_code == 200:
            return url
    
    # If we get here, return the default pattern as a last resort
    return f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{clean_accn}/{ticker}-{period_end}.htm"

def download_html_document(html_url, filing_metadata):
    """Download HTML document containing iXBRL data"""
    # Create directory path
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    accession_number = filing_metadata.get("accession_number", "unknown")
    
    # Create directory structure
    dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
    os.makedirs(dir_path, exist_ok=True)
    
    # Download the HTML document
    if not html_url:
        return {"error": "No HTML URL provided"}
    
    try:
        response = sec_request(html_url)
        if response.status_code != 200:
            return {"error": f"Failed to download HTML document: {response.status_code}"}
        
        # Determine if this is the SEC iXBRL viewer or a direct HTML file
        is_sec_viewer = "ix?doc=" in html_url.lower()
        
        # For SEC iXBRL viewer pages, we need to extract the actual document URL
        if is_sec_viewer:
            # Extract the document path from the URL
            parsed_url = urlparse(html_url)
            query_params = parse_qs(parsed_url.query)
            
            if 'doc' in query_params:
                actual_doc_path = query_params['doc'][0]
                
                # Construct the direct URL to the document
                if actual_doc_path.startswith('/'):
                    actual_doc_url = f"https://www.sec.gov{actual_doc_path}"
                else:
                    actual_doc_url = f"https://www.sec.gov/{actual_doc_path}"
                
                # Download the actual document
                actual_response = sec_request(actual_doc_url)
                if actual_response.status_code == 200:
                    response = actual_response
                    html_url = actual_doc_url
        
        # Extract filename from URL
        parsed_url = urlparse(html_url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            # Use a default name if we can't extract one
            filename = f"{accession_number}_document.htm"
        
        # Save the file
        file_path = os.path.join(dir_path, f"{accession_number}-{filename}")
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        # Save metadata about the file
        info_path = f"{file_path}.info.txt"
        with open(info_path, 'w') as f:
            f.write(f"URL: {html_url}\n")
            f.write(f"Type: iXBRL\n")
            f.write(f"Content-Type: {response.headers.get('Content-Type', 'unknown')}\n")
        
        # Determine if this is an iXBRL document
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Check for iXBRL namespace in the HTML
        is_ixbrl = False
        html_tag = soup.find('html')
        if html_tag:
            for attr_name, attr_value in html_tag.attrs.items():
                if 'xmlns:ix' in attr_name or (isinstance(attr_value, str) and 'inline' in attr_value.lower() and 'xbrl' in attr_value.lower()):
                    is_ixbrl = True
                    break
        
        # Also check for ix: elements
        if not is_ixbrl:
            ix_elements = soup.find_all(lambda tag: tag.name and ':' in tag.name and tag.name.split(':')[0] == 'ix')
            is_ixbrl = len(ix_elements) > 0
        
        # Also check if this is the SEC viewer
        if not is_ixbrl and "XBRL Viewer" in str(soup.title):
            is_sec_viewer = True
        
        return {
            "success": True,
            "file_path": file_path,
            "is_ixbrl": is_ixbrl,
            "is_sec_viewer": is_sec_viewer,
            "size": len(response.content)
        }
    except Exception as e:
        return {"error": f"Exception downloading HTML document: {str(e)}"}

def direct_download_from_sec_viewer(viewer_url, filing_metadata):
    """
    Extract and download the actual document from the SEC's iXBRL viewer
    
    The SEC viewer is a wrapper that loads the actual document in an iframe.
    This function extracts the document URL and downloads it directly.
    """
    try:
        # Extract the document path from the viewer URL
        parsed_url = urlparse(viewer_url)
        query_params = parse_qs(parsed_url.query)
        
        if 'doc' not in query_params:
            return {"error": "No document path found in viewer URL"}
        
        actual_doc_path = query_params['doc'][0]
        
        # Construct the direct URL to the document
        if actual_doc_path.startswith('/'):
            actual_doc_url = f"https://www.sec.gov{actual_doc_path}"
        else:
            actual_doc_url = f"https://www.sec.gov/{actual_doc_path}"
        
        # Download the document
        result = download_html_document(actual_doc_url, filing_metadata)
        
        if "error" in result:
            # If direct download fails, try to extract from the viewer's JavaScript
            response = sec_request(viewer_url)
            if response.status_code != 200:
                return {"error": f"Failed to download SEC viewer: {response.status_code}"}
            
            # Look for the document URL in the JavaScript
            doc_url_match = re.search(r'loadXbrl\("([^"]+)"', response.text)
            if doc_url_match:
                extracted_url = doc_url_match.group(1)
                if not extracted_url.startswith('http'):
                    extracted_url = urljoin('https://www.sec.gov/', extracted_url)
                
                # Try downloading with the extracted URL
                return download_html_document(extracted_url, filing_metadata)
        
        return result
        
    except Exception as e:
        return {"error": f"Exception extracting document from SEC viewer: {str(e)}"}

def download_sec_primary_document(filing_metadata):
    """
    Download the primary document from the SEC's EDGAR database.
    
    This function tries to find and download the primary document (usually an HTML file)
    that contains the filing data. For modern filings, this document typically contains
    inline XBRL (iXBRL) data.
    """
    # Ensure we have a standardized accession number
    if "accession_number" in filing_metadata and filing_metadata["accession_number"]:
        if "-" in filing_metadata["accession_number"]:
            accession_number = standardize_accession_number(filing_metadata["accession_number"])
            filing_metadata["original_accession_number"] = filing_metadata["accession_number"]
            filing_metadata["accession_number"] = accession_number
    
    # Try finding the document through index.json first (most reliable method)
    cik = filing_metadata.get("cik", "").lstrip('0')
    accession_number = filing_metadata.get("accession_number", "")
    
    if cik and accession_number:
        # First check index.json directly
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/index.json"
        response = sec_request(index_url)
        
        if response and response.status_code == 200:
            try:
                index_data = response.json()
                if "directory" in index_data and "item" in index_data["directory"]:
                    # Look for primary document
                    primary_doc = None
                    for item in index_data["directory"]["item"]:
                        if "name" in item and item["name"].lower().endswith(".htm"):
                            # Found an HTML file
                            doc_name = item["name"].lower()
                            
                            # Prioritize based on known patterns
                            if primary_doc is None:
                                primary_doc = item["name"]
                            
                            # Check for naming patterns that suggest main document
                            if "10-k" in doc_name or "10k" in doc_name or "10-q" in doc_name or "10q" in doc_name:
                                primary_doc = item["name"]
                                break
                    
                    if primary_doc:
                        # Found a potential primary document
                        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{primary_doc}"
                        print(f"Found primary document in index.json: {doc_url}")
                        
                        # Try to download it
                        result = download_html_document(doc_url, filing_metadata)
                        if "error" not in result:
                            return result
            except Exception as e:
                print(f"Error parsing index.json: {str(e)}")
    
    # If we couldn't get the document from index.json, try the standard URL finding logic
    html_url = find_html_document_url(filing_metadata)
    if not html_url:
        return {"error": "Could not find HTML document URL"}
    
    print(f"Successfully found HTML at: {html_url}")
    
    # Check if this is the SEC iXBRL viewer
    if "ix?doc=" in html_url.lower():
        # Download the document directly from the SEC viewer
        result = direct_download_from_sec_viewer(html_url, filing_metadata)
    else:
        # Download the HTML document
        result = download_html_document(html_url, filing_metadata)
    
    if "error" in result:
        return result
    
    # Check if the document contains iXBRL
    file_path = result.get("file_path")
    is_ixbrl = result.get("is_ixbrl", False)
    
    print(f"Saved HTML document to {file_path}")
    print(f"Document appears to be iXBRL: {is_ixbrl}")
    
    return result