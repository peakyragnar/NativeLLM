#!/usr/bin/env python3
"""
Enhanced Pipeline for NativeLLM

This module provides a unified system for processing SEC XBRL filings
with HTML optimization and cloud storage integration. It combines the
functionality of calendar_download.py with our enhanced processing
and optimization techniques.

Key features:
- Automatic format detection (XBRL vs iXBRL)
- HTML optimization with full data integrity
- GCP integration for cloud storage
- Unified command-line interface
"""

import os
import sys
import time
import logging
import argparse
import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage, firestore

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Import core processing modules
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.edgar.fiscal_manager import fiscal_manager

# Import enhanced processing modules
from src.xbrl.enhanced_processor import process_company_filing
from src.xbrl.html_text_extractor import process_html_filing

# Import formatter and cloud modules
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import INITIAL_COMPANIES, PROCESSED_DATA_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_pipeline.log'),
        logging.StreamHandler()
    ]
)

# GCP configuration
CREDENTIALS_PATH = "/Users/michael/NativeLLM/nativellmfilings-e149eb3298de.json"
GCS_BUCKET_NAME = "native-llm-filings"
FIRESTORE_DB = "nativellm"

def configure_gcp():
    """Set up GCP credentials if not already configured"""
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        if os.path.exists(CREDENTIALS_PATH):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
            logging.info(f"Set GCP credentials from {CREDENTIALS_PATH}")
            return True
        else:
            logging.error(f"Credentials file not found at {CREDENTIALS_PATH}")
            return False
    return True

def check_existing_filing(ticker, filing_type, fiscal_year, fiscal_period, db):
    """Check if a filing already exists in Firestore"""
    # Use fiscal_manager to standardize the period format for consistency
    firestore_period = fiscal_manager.standardize_period(fiscal_period, "internal")
    
    filing_id = f"{ticker}-{filing_type}-{fiscal_year}-{firestore_period}"
    filing_ref = db.collection('filings').document(filing_id).get()
    
    if filing_ref.exists:
        logging.info(f"Filing already exists in Firestore: {filing_id}")
        return True
    return False

def upload_to_gcs(local_file_path, ticker, filing_type, fiscal_year, fiscal_period, file_format, bucket_name=GCS_BUCKET_NAME):
    """Upload a filing to Google Cloud Storage"""
    # Check if we're running with --skip-gcp
    if os.environ.get("SKIP_GCP_UPLOAD") == "1":
        logging.info(f"Skipping GCS upload for {local_file_path} due to --skip-gcp flag")
        # Return a mock path and the actual file size
        mock_gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/{file_format}.txt"
        return mock_gcs_path, os.path.getsize(local_file_path)
        
    # Use fiscal_manager to standardize the period for folder naming
    quarter_folder = fiscal_manager.standardize_period(fiscal_period, "folder")
    
    # Construct GCS path
    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_folder}/{file_format}.txt"
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob and upload
        blob = bucket.blob(gcs_path)
        
        # Check if blob already exists
        if blob.exists():
            logging.info(f"File already exists in GCS: gs://{bucket_name}/{gcs_path}")
            # Return the path and size of existing file
            blob.reload()  # Refresh metadata
            return gcs_path, blob.size
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Successfully uploaded {local_file_path} to gs://{bucket_name}/{gcs_path}")
        return gcs_path, os.path.getsize(local_file_path)
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {str(e)}")
        return None, 0

def add_filing_metadata(company_ticker, company_name, filing_type, fiscal_year, fiscal_period, 
                       period_end_date, filing_date, text_path, llm_path,
                       text_size, llm_size):
    """Add metadata for a filing to Firestore"""
    try:
        db = firestore.Client(database=FIRESTORE_DB)
        
        # Ensure company exists in Firestore
        company_ref = db.collection('companies').document(company_ticker)
        if not company_ref.get().exists:
            company_ref.set({
                'ticker': company_ticker,
                'name': company_name,
                'last_updated': datetime.datetime.now()
            })
            logging.info(f"Added company to Firestore: {company_ticker} - {company_name}")
        
        # Create a unique filing ID with standardized period format
        firestore_period = fiscal_manager.standardize_period(fiscal_period, "internal")
            
        filing_id = f"{company_ticker}-{filing_type}-{fiscal_year}-{firestore_period}"
        
        # Check if filing already exists
        filing_ref = db.collection('filings').document(filing_id)
        if filing_ref.get().exists:
            logging.info(f"Filing metadata already exists: {filing_id}")
            return filing_id
        
        # Add to filings collection
        filing_data = {
            'filing_id': filing_id,
            'company_ticker': company_ticker,
            'company_name': company_name,
            'filing_type': filing_type,
            'fiscal_year': fiscal_year,
            'fiscal_period': firestore_period,
            'period_end_date': period_end_date,
            'filing_date': filing_date,
            'text_file_path': text_path,
            'llm_file_path': llm_path,
            'text_file_size': text_size,
            'llm_file_size': llm_size,
            'storage_class': 'STANDARD',
            'last_accessed': datetime.datetime.now(),
            'access_count': 0
        }
        
        filing_ref.set(filing_data)
        logging.info(f"Added filing metadata: {filing_id}")
        
        return filing_id
    except Exception as e:
        logging.error(f"Error adding metadata to Firestore: {str(e)}")
        return None

def is_filing_in_range(filing_date, start_date, end_date):
    """Check if a filing date is within the specified range"""
    try:
        filing_dt = datetime.datetime.strptime(filing_date, '%Y-%m-%d').date()
        start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        
        return start_dt <= filing_dt <= end_dt
    except Exception as e:
        logging.error(f"Error comparing dates: {str(e)}")
        return False

def parse_document_table_from_index_page(index_url, max_retries=3, backoff_factor=2.0):
    """
    Parse the document table from an SEC filing index page and extract all documents.
    
    This enhanced version includes:
    - Multiple retry attempts with exponential backoff
    - Better error recovery and diagnostics
    - Support for foreign company filings (20-F)
    - Support for both modern and legacy SEC document table formats

    This function handles both modern and legacy SEC document tables, extracting detailed
    information about each document including format, type, and description.
    
    Args:
        index_url: URL to the SEC filing index page
        
    Returns:
        Dictionary with document information and classification
    """
    from src.edgar.edgar_utils import sec_request
    from bs4 import BeautifulSoup
    
    logging.info(f"Parsing document table from index page: {index_url}")
    
    try:
        # Get the index page
        response = sec_request(index_url)
        if response.status_code != 200:
            logging.error(f"Failed to get document index page: {response.status_code}")
            return {"error": f"Failed to get document index page: {response.status_code}"}
        
        # Parse the page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract filing info from page
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
        
        # Find all document tables
        document_tables = []
        
        # Method 1: Find tables with specific captions
        for table in soup.find_all('table'):
            caption = table.find('caption')
            if caption and ('Document Format Files' in caption.text or 'Data Files' in caption.text):
                document_tables.append(table)
                logging.info(f"Found document table with caption: {caption.text.strip()}")
        
        # Method 2: Find tables with specific class names
        if not document_tables:
            for table in soup.find_all('table'):
                if table.get('class') and any(c in ['tableFile', 'tableFile2'] for c in table.get('class')):
                    document_tables.append(table)
                    logging.info(f"Found document table with class: {table.get('class')}")
        
        # Method 3: Check summary attribute (older SEC format)
        if not document_tables:
            for table in soup.find_all('table'):
                if table.get('summary') and ('Document Format Files' in table.get('summary') or 'Data Files' in table.get('summary')):
                    document_tables.append(table)
                    logging.info(f"Found document table with summary: {table.get('summary')}")
        
        # Method 4: Fall back to any table with links
        if not document_tables:
            logging.warning("No document tables found with standard markers, looking for any table with links")
            for table in soup.find_all('table'):
                if table.find('a'):
                    document_tables.append(table)
                    logging.info("Found table with links (fallback method)")
        
        if not document_tables:
            logging.error("No document tables found in index page")
            return {"error": "No document tables found in index page"}
        
        # Process all tables to extract documents
        all_documents = []
        
        for table_idx, table in enumerate(document_tables):
            logging.info(f"Processing document table {table_idx+1} of {len(document_tables)}")
            
            rows = table.find_all('tr')
            if len(rows) <= 1:  # Skip empty tables
                continue
            
            # Get header row to identify columns
            header_row = rows[0]
            headers = [th.text.strip().lower() for th in header_row.find_all(['th', 'td'])]
            logging.info(f"Table headers: {headers}")
            
            # Determine column indexes
            seq_idx = next((i for i, h in enumerate(headers) if 'seq' in h or '#' in h), 0)
            desc_idx = next((i for i, h in enumerate(headers) if 'description' in h), 1)
            doc_idx = next((i for i, h in enumerate(headers) if 'document' in h), 2)
            type_idx = next((i for i, h in enumerate(headers) if 'type' in h), 0)
            size_idx = next((i for i, h in enumerate(headers) if 'size' in h), None)
            format_idx = next((i for i, h in enumerate(headers) if 'format' in h), None)
            
            # Process each document row
            for row_idx, row in enumerate(rows[1:], 1):  # Skip header
                cells = row.find_all(['th', 'td'])
                if len(cells) <= 2:  # Need at least type, description, document columns
                    continue
                
                # Extract basic information
                seq_num = row_idx
                if seq_idx < len(cells):
                    try:
                        seq_text = cells[seq_idx].text.strip()
                        seq_num = int(seq_text) if seq_text.isdigit() else row_idx
                    except:
                        seq_num = row_idx
                
                # Get document type
                doc_type = ""
                if type_idx < len(cells):
                    doc_type = cells[type_idx].text.strip()
                
                # Get document description
                description = ""
                if desc_idx < len(cells):
                    description = cells[desc_idx].text.strip()
                
                # Get format information if available
                format_info = ""
                if format_idx and format_idx < len(cells):
                    format_info = cells[format_idx].text.strip()
                
                # Get document links
                if doc_idx >= len(cells):
                    continue
                
                link_cell = cells[doc_idx]
                links = link_cell.find_all('a')
                
                # Check for iXBRL format indicators in the cell
                is_ixbrl = False
                cell_html = str(link_cell).lower()
                if 'ixbrl' in cell_html or 'inline xbrl' in cell_html or 'ix' in cell_html:
                    is_ixbrl = True
                    # Look for green 'iXBRL' text which is a reliable indicator
                    ixbrl_texts = link_cell.select('span[style*="color:green"]')
                    if ixbrl_texts and any('ixbrl' in span.text.lower() for span in ixbrl_texts):
                        is_ixbrl = True
                        format_info = "iXBRL"
                
                # Process each link in the document cell
                for link in links:
                    href = link.get('href')
                    link_text = link.text.strip()
                    
                    if not href:
                        continue
                    
                    # Construct full URL if relative
                    if href.startswith('/'):
                        url = f"https://www.sec.gov{href}"
                    elif href.startswith('http'):
                        url = href
                    else:
                        # Handle relative URL
                        base_url = '/'.join(index_url.split('/')[:-1])
                        url = f"{base_url}/{href}"
                    
                    # Determine document format from URL and attributes
                    derived_format = ""
                    
                    # Check for iXBRL indicators
                    if 'ix?doc=' in url.lower():
                        derived_format = "iXBRL"
                    elif is_ixbrl and (url.endswith('.htm') or url.endswith('.html')):
                        derived_format = "iXBRL"
                    elif url.endswith('.htm') or url.endswith('.html'):
                        derived_format = "HTML"
                    elif url.endswith('.xml'):
                        if '_htm.xml' in url.lower() or 'instance' in description.lower():
                            derived_format = "XBRL INSTANCE"
                        else:
                            derived_format = "XML"
                    
                    # Determine if this document is the primary filing
                    is_primary = False
                    if doc_type and doc_type.upper() in ["10-K", "10-Q", "8-K"]:
                        is_primary = True
                    elif description and "complete submission" in description.lower():
                        is_primary = True
                    
                    # For main filings, sequence 1 is often the primary document
                    if seq_num == 1 and (doc_type == "" or doc_type.upper() in ["10-K", "10-Q", "8-K"]):
                        is_primary = True
                    
                    # Add document to the collection
                    document = {
                        'seq': seq_num,
                        'type': doc_type,
                        'description': description,
                        'url': url,
                        'link_text': link_text,
                        'explicit_format': format_info,
                        'derived_format': derived_format,
                        'is_primary': is_primary,
                        'table_index': table_idx + 1
                    }
                    
                    all_documents.append(document)
                    logging.info(f"Found document: {doc_type} - {description} - {url} [{derived_format}]")
        
        # Return the complete document collection with filing info
        return {
            "success": True,
            "index_url": index_url,
            "filing_info": filing_info,
            "document_count": len(all_documents),
            "table_count": len(document_tables),
            "documents": all_documents
        }
        
    except Exception as e:
        logging.error(f"Error parsing document table: {str(e)}")
        return {"error": f"Error parsing document table: {str(e)}"}

def select_filing_documents(documents, filing_type, metadata=None):
    """
    Select appropriate documents from the parsed document table based on priority rules.
    
    This implements a systematic document selection approach that works consistently
    across companies and filing years.
    
    Args:
        documents: List of documents from parse_document_table_from_index_page
        filing_type: Type of filing to select documents for (e.g., "10-K", "10-Q")
        metadata: Optional metadata dictionary that may contain selection hints
        
    Returns:
        Dictionary with selected documents for different purposes
    """
    if not documents:
        return {}
        
    # Extract hints from metadata if provided
    try_alternative = False
    find_xbrl_only = False
    if metadata:
        try_alternative = metadata.get("try_alternative", False)
        find_xbrl_only = metadata.get("find_xbrl_only", False)
        
    # Special mode for XBRL-only search
    if find_xbrl_only:
        logging.info("Running in XBRL-only search mode, prioritizing XBRL instances")
    
    # Initialize selected documents
    selected = {
        "primary_document": None,  # Main HTML or iXBRL document for the filing
        "xbrl_instance": None,     # XBRL instance document, if available
        "complete_submission": None # Complete submission text
    }
    
    # Clean filing type for matching
    clean_filing_type = filing_type.lower().replace('-', '')
    
    # For XBRL-only search, prioritize finding XBRL instance documents
    if find_xbrl_only:
        # Search for XBRL instance documents with specific indicators
        for doc in documents:
            description = doc.get("description", "").lower()
            url = doc.get("url", "").lower()
            derived_format = doc.get("derived_format", "")
            
            # Look for XBRL instance document indicators
            is_xbrl_instance = (
                derived_format == "XBRL INSTANCE" or
                "xbrl instance" in description or
                "extracted xbrl" in description or
                "_htm.xml" in url or
                url.endswith(".xbrl") or
                (url.endswith(".xml") and not url.endswith("_def.xml") and not url.endswith("_cal.xml"))
            )
            
            if is_xbrl_instance:
                selected["xbrl_instance"] = doc
                logging.info(f"Selected XBRL instance document for XBRL-only search: {doc.get('url')}")
                break
        
        # Still select a primary document for reference
        for doc in documents:
            if ".htm" in doc.get("url", "").lower():
                selected["primary_document"] = doc
                break
        
        # Return early since we're only interested in XBRL documents
        return selected
    
    # Regular document selection with priorities
    # Different selection logic based on whether we're trying alternatives
    if not try_alternative:
        # STANDARD SELECTION LOGIC (PRIMARY FLOW)
        
        # PRIORITY 1: Find primary document with exact filing type match in iXBRL format
        for doc in documents:
            doc_type = doc.get("type", "").lower().replace('-', '')
            description = doc.get("description", "").lower()
            derived_format = doc.get("derived_format", "")
            
            if (clean_filing_type == doc_type or clean_filing_type in description) and derived_format == "iXBRL":
                selected["primary_document"] = doc
                logging.info(f"Selected primary document (priority 1 - iXBRL with exact filing type match): {doc.get('url')}")
                break
        
        # PRIORITY 2: If no iXBRL with exact match, look for any iXBRL document
        if not selected["primary_document"]:
            for doc in documents:
                derived_format = doc.get("derived_format", "")
                if derived_format == "iXBRL":
                    selected["primary_document"] = doc
                    logging.info(f"Selected primary document (priority 2 - any iXBRL): {doc.get('url')}")
                    break
        
        # PRIORITY 3: If no iXBRL, look for HTML document with exact filing type match
        if not selected["primary_document"]:
            for doc in documents:
                doc_type = doc.get("type", "").lower().replace('-', '')
                description = doc.get("description", "").lower()
                derived_format = doc.get("derived_format", "")
                
                if (clean_filing_type == doc_type or clean_filing_type in description) and derived_format == "HTML":
                    selected["primary_document"] = doc
                    logging.info(f"Selected primary document (priority 3 - HTML with exact filing type match): {doc.get('url')}")
                    break
    
        # PRIORITY 4: Use a document marked as primary or main document
        if not selected["primary_document"]:
            for doc in documents:
                is_primary = doc.get("is_primary", False)
                description = doc.get("description", "").lower()
                derived_format = doc.get("derived_format", "")
                
                if is_primary and (derived_format == "HTML" or derived_format == "iXBRL"):
                    selected["primary_document"] = doc
                    logging.info(f"Selected primary document (priority 4 - marked as primary): {doc.get('url')}")
                    break
            if any(indicator in description for indicator in primary_indicators) and (derived_format == "HTML" or derived_format == "iXBRL"):
                selected["primary_document"] = doc
                logging.info(f"Selected primary document (priority 4 - description indicates primary): {doc.get('url')}")
                break
    
        # PRIORITY 5: Sequence 1 HTML document (often the main filing)
        if not selected["primary_document"]:
            for doc in documents:
                seq = doc.get("seq", 0)
                derived_format = doc.get("derived_format", "")
                
                if seq == 1 and (derived_format == "HTML" or derived_format == "iXBRL"):
                    selected["primary_document"] = doc
                    logging.info(f"Selected primary document (priority 5 - sequence 1): {doc.get('url')}")
                    break
        
        # PRIORITY 6: Any HTML document (last resort)
        if not selected["primary_document"]:
            for doc in documents:
                derived_format = doc.get("derived_format", "")
                if derived_format == "HTML" or derived_format == "iXBRL":
                    selected["primary_document"] = doc
                    logging.info(f"Selected primary document (priority 6 - any HTML/iXBRL): {doc.get('url')}")
                    break
    else:
        # ALTERNATIVE SELECTION LOGIC FOR RETRIES
        # This is the alternative path for try_alternative=True
        logging.info("Using alternative document selection logic")
        
        # Try documents we typically wouldn't have selected first
        for doc in documents:
            url = doc.get("url", "").lower()
            seq = doc.get("seq", 0)
            # Look for HTML files with higher sequence numbers
            if ".htm" in url and seq > 2:  # Skip seq 1 and 2 which would have been selected first
                selected["primary_document"] = doc
                logging.info(f"Selected alternative document (high sequence): {doc.get('url')}")
                break
    
    # Find XBRL instance document (for traditional XBRL processing) regardless of path
    for doc in documents:
        derived_format = doc.get("derived_format", "")
        description = doc.get("description", "").lower()
        url = doc.get("url", "").lower()
        
        # Look for multiple indicators of XBRL instance documents
        is_xbrl_instance = (
            derived_format == "XBRL INSTANCE" or 
            "xbrl instance" in description or
            "extracted xbrl" in description or
            "_htm.xml" in url or
            url.endswith(".xbrl") or
            (url.endswith(".xml") and not url.endswith("_def.xml") and not url.endswith("_cal.xml"))
        )
        
        if is_xbrl_instance:
            selected["xbrl_instance"] = doc
            logging.info(f"Selected XBRL instance document: {doc.get('url')}")
            break
    
    # Find complete submission text
    for doc in documents:
        description = doc.get("description", "").lower()
        if "complete submission" in description:
            selected["complete_submission"] = doc
            logging.info(f"Selected complete submission: {doc.get('url')}")
            break
    
    return selected

def get_filing_index_url(filing_metadata):
    """
    Determine the SEC index page URL from filing metadata.
    
    This function constructs the most likely index page URL based on CIK and accession number.
    It handles various formats and edge cases for maximum reliability.
    
    Args:
        filing_metadata: Dictionary containing filing metadata
        
    Returns:
        Index page URL or None if insufficient information
    """
    # Extract necessary metadata
    cik = filing_metadata.get("cik")
    accession_number = filing_metadata.get("accession_number")
    
    # If missing basic information, try to derive from instance_url if available
    if (not cik or not accession_number) and "instance_url" in filing_metadata:
        instance_url = filing_metadata["instance_url"]
        
        # Standard pattern in instance URLs
        accession_match = re.search(r'data/(\d+)/(\d{10}-\d{2}-\d{6})', instance_url)
        if accession_match:
            cik = accession_match.group(1)
            accession_number = accession_match.group(2)
            logging.info(f"Derived CIK ({cik}) and accession number ({accession_number}) from instance URL")
        else:
            # Alternative pattern for older or non-standard URLs
            alt_match = re.search(r'data/(\d+)/([^/]+)/', instance_url)
            if alt_match:
                cik = alt_match.group(1)
                formatted_acc = alt_match.group(2)
                # Use this as the accession number for URL construction
                accession_number = formatted_acc
                logging.info(f"Derived CIK ({cik}) and formatted accession ({formatted_acc}) from instance URL")
            else:
                logging.error(f"Could not extract accession number from instance URL: {instance_url}")
                return None
    
    # If still missing basic information, cannot continue
    if not cik or not accession_number:
        logging.error("Cannot construct index URL: missing CIK or accession number")
        return None
    
    # Format CIK and accession number for URL construction
    cik_no_zeros = cik.lstrip('0')
    formatted_acc = accession_number.replace('-', '')
    
    # Construct the standard index page URL
    # Modern SEC filings use this format consistently
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{accession_number}-index.htm"
    
    return index_url

def extract_document_url_from_filing_metadata(filing_metadata):
    """
    Extract document URLs from filing metadata using a document table parser approach.
    
    This function uses a systematic approach to find and parse SEC document tables,
    then selects appropriate documents based on priority rules.
    
    Args:
        filing_metadata: Dictionary containing filing metadata
        
    Returns:
        URL of the primary document or None if not found
    """
    # Import needed functions
    from src.edgar.edgar_utils import sec_request
    
    # Step 1: Check if document_url is already provided in metadata
    if "document_url" in filing_metadata:
        document_url = filing_metadata["document_url"]
        logging.info(f"Using document URL from metadata: {document_url}")
        return document_url
    
    if "html_url" in filing_metadata:
        document_url = filing_metadata["html_url"]
        logging.info(f"Using HTML URL from metadata: {document_url}")
        return document_url
    
    # Step 2: Get the filing's index page URL
    index_url = get_filing_index_url(filing_metadata)
    if not index_url:
        logging.warning("Could not construct index page URL, trying fallback methods")
        return fallback_url_patterns(filing_metadata)
    
    # Step 3: Parse the document table from the index page
    document_result = parse_document_table_from_index_page(index_url)
    
    if "error" in document_result:
        logging.warning(f"Error parsing document table: {document_result['error']}")
        # Try alternative index page formats
        ticker = filing_metadata.get("ticker", "")
        cik = filing_metadata.get("cik")
        accession_number = filing_metadata.get("accession_number")
        formatted_acc = accession_number.replace('-', '') if accession_number else ""
        cik_no_zeros = cik.lstrip('0') if cik else ""
        
        alternative_formats = [
            f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/index.htm",
            f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{formatted_acc}-index.htm"
        ]
        
        # Try special format for hyphenated-style accession numbers in URL
        if formatted_acc and len(formatted_acc) >= 12:
            special_acc = f"{formatted_acc[0:10]}-{formatted_acc[10:12]}-{formatted_acc[12:]}"
            alternative_formats.append(f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{special_acc}-index.htm")
        
        # Try each alternative format
        for alt_url in alternative_formats:
            logging.info(f"Trying alternative index page format: {alt_url}")
            document_result = parse_document_table_from_index_page(alt_url)
            if "error" not in document_result:
                logging.info(f"Successfully parsed document table from alternative index page: {alt_url}")
                break
        
        # If still failing, fall back to pattern matching
        if "error" in document_result:
            logging.warning("Failed to parse document table from all index page formats, using fallback pattern matching")
            return fallback_url_patterns(filing_metadata)
    
    # Step 4: Select appropriate documents based on filing type and metadata
    filing_type = filing_metadata.get("filing_type", "").upper()
    documents = document_result.get("documents", [])
    
    # Pass filing_metadata to allow selection based on hints
    selected_docs = select_filing_documents(documents, filing_type, filing_metadata)
    
    # Step 5: Return primary document URL if found
    if selected_docs.get("primary_document"):
        primary_doc = selected_docs["primary_document"]
        document_url = primary_doc.get("url")
        logging.info(f"Selected primary document: {document_url}")
        
        # Also update filing_metadata with other useful documents
        if selected_docs.get("xbrl_instance"):
            xbrl_url = selected_docs["xbrl_instance"].get("url")
            filing_metadata["instance_url"] = xbrl_url
            logging.info(f"Added XBRL instance URL to metadata: {xbrl_url}")
        
        return document_url
    
    # No primary document found, try fallback
    logging.warning("No primary document found in document table, using fallback pattern matching")
    return fallback_url_patterns(filing_metadata)

def fallback_url_patterns(filing_metadata):
    """
    Fallback method that tries common URL patterns when document table parsing fails.
    Uses a comprehensive approach to try all possible patterns across different SEC formats.
    """
    from src.edgar.edgar_utils import sec_request
    
    logging.info("Using fallback URL pattern matching")
    
    # Extract essential metadata
    ticker = filing_metadata.get("ticker", "").lower()
    period_end_date = filing_metadata.get("period_end_date", "").replace("-", "")
    filing_type = filing_metadata.get("filing_type", "").lower()
    
    # Keep track of attempted URLs to avoid duplicates
    attempted_urls = set()
    
    # Check if we have instance_url
    if "instance_url" in filing_metadata:
        instance_url = filing_metadata["instance_url"]
        logging.info(f"Working with instance URL: {instance_url}")
        
        # Try to extract base directory
        if '/' in instance_url:
            parts = instance_url.split('/')
            if len(parts) >= 7:  # Should be like https://www.sec.gov/Archives/edgar/data/CIK/ACCESSION/file.xml
                base_dir = '/'.join(parts[:-1])  # Get everything except the filename
                xml_filename = parts[-1]
                logging.info(f"Base directory: {base_dir}")
                logging.info(f"XML filename: {xml_filename}")
                
                # Extract CIK and accession from URL using multiple patterns
                cik = None
                accession = None
                formatted_acc = None
                
                # Try standard pattern first
                accession_match = re.search(r'data/(\d+)/(\d{10}-\d{2}-\d{6})', instance_url)
                if accession_match:
                    cik = accession_match.group(1)
                    accession = accession_match.group(2)
                    formatted_acc = accession.replace('-', '')
                    logging.info(f"Extracted CIK: {cik}, Accession: {accession}")
                else:
                    # Try alternative pattern for older format URLs
                    alt_match = re.search(r'data/(\d+)/([^/]+)', instance_url)
                    if alt_match:
                        cik = alt_match.group(1)
                        formatted_acc = alt_match.group(2)
                        logging.info(f"Extracted CIK: {cik}, Formatted accession: {formatted_acc}")
                
                # Create more comprehensive list of potential filenames
                html_candidates = []
                
                # TIER 1 - Most specific and reliable patterns
                
                # XML-derived filenames (most reliable when available)
                if xml_filename:
                    for pattern in ['_htm.xml', '_cal.xml', '_def.xml', '_lab.xml', '_pre.xml']:
                        if pattern in xml_filename:
                            # Try variations on the XML filename, both with and without 'htm'
                            base_name = xml_filename.split('.')[0].replace(pattern.replace('.xml', ''), '')
                            html_candidates.append(xml_filename.replace(pattern, '.htm'))
                            html_candidates.append(xml_filename.replace(pattern, '_htm.htm'))
                            html_candidates.append(xml_filename.replace(pattern, '.html'))
                            html_candidates.append(f"{base_name}.htm")
                            html_candidates.append(f"{base_name}.html")
                
                # Ticker and period date combinations (reliable for newer filings)
                if ticker and period_end_date:
                    html_candidates.extend([
                        f"{ticker}-{period_end_date}.htm",
                        f"{ticker}_{period_end_date}.htm",
                        f"{ticker}{period_end_date}.htm",
                        f"{ticker.upper()}-{period_end_date}.htm",
                        f"{ticker.upper()}_{period_end_date}.htm",
                        f"{ticker.upper()}{period_end_date}.htm"
                    ])
                
                # TIER 2 - Filing type based patterns
                
                # Filing type specific patterns (good for standardized docs)
                if filing_type:
                    clean_filing_type = filing_type.replace('-', '')
                    if ticker:
                        html_candidates.extend([
                            f"{ticker}{clean_filing_type}.htm",
                            f"{ticker}-{clean_filing_type}.htm",
                            f"{ticker}_{clean_filing_type}.htm",
                            f"{ticker.upper()}{clean_filing_type}.htm",
                            f"{ticker.upper()}-{clean_filing_type}.htm"
                        ])
                    
                    html_candidates.extend([
                        f"{clean_filing_type}.htm",
                        f"Form{clean_filing_type}.htm",
                        f"form{clean_filing_type}.htm",
                        f"{clean_filing_type}_doc.htm",
                        f"{clean_filing_type}_document.htm"
                    ])
                
                # TIER 3 - Generic document patterns (last resort)
                
                # Generic filenames that are commonly used
                generic_filenames = [
                    "primary.htm", "document.htm", "filing.htm", "report.htm", "form.htm",
                    "primary_document.htm", "complete_submission.htm", "main.htm",
                    "FilingSummary.htm", "index.htm", "Financial_Report.htm"
                ]
                
                html_candidates.extend(generic_filenames)
                
                # Combine base directory with each candidate and try them one by one
                logging.info(f"Trying {len(html_candidates)} filename patterns...")
                
                for filename in html_candidates:
                    url = f"{base_dir}/{filename}"
                    
                    # Skip if we've already tried this URL
                    if url in attempted_urls:
                        continue
                    
                    attempted_urls.add(url)
                    logging.info(f"Trying URL pattern: {url}")
                    
                    try:
                        response = sec_request(url)
                        if response.status_code == 200:
                            # Verify it's actually HTML content
                            if '<html' in response.text.lower() or '<body' in response.text.lower():
                                logging.info(f"Found valid HTML URL using pattern matching: {url}")
                                return url
                            else:
                                logging.warning(f"URL returned 200 but not HTML content: {url}")
                        else:
                            logging.debug(f"URL returned {response.status_code}: {url}")
                    except Exception as e:
                        logging.debug(f"Error trying {url}: {str(e)}")
                
                # Try iXBRL viewer URLs for recent filings
                if cik and formatted_acc:
                    # Try iXBRL format with different filename patterns
                    ixbrl_base = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{formatted_acc}/"
                    
                    ixbrl_candidates = []
                    
                    # Most likely filenames for iXBRL
                    if ticker and period_end_date:
                        ixbrl_candidates.extend([
                            f"{ixbrl_base}{ticker}-{period_end_date}.htm",
                            f"{ixbrl_base}{ticker.upper()}-{period_end_date}.htm"
                        ])
                    
                    # Try XML filename converted to HTM with iXBRL format
                    if xml_filename and '_htm.xml' in xml_filename:
                        ixbrl_candidates.append(f"{ixbrl_base}{xml_filename.replace('_htm.xml', '.htm')}")
                    
                    # Try with filing type patterns
                    if filing_type and ticker:
                        clean_filing_type = filing_type.replace('-', '')
                        ixbrl_candidates.extend([
                            f"{ixbrl_base}{ticker}{clean_filing_type}.htm",
                            f"{ixbrl_base}{ticker.upper()}{clean_filing_type}.htm"
                        ])
                    
                    # Try iXBRL candidates
                    for url in ixbrl_candidates:
                        if url in attempted_urls:
                            continue
                            
                        attempted_urls.add(url)
                        logging.info(f"Trying iXBRL URL: {url}")
                        
                        try:
                            response = sec_request(url)
                            if response.status_code == 200:
                                logging.info(f"Found valid iXBRL URL: {url}")
                                return url
                        except Exception as e:
                            logging.debug(f"Error trying iXBRL URL {url}: {str(e)}")
    
    # LAST RESORT: Try constructing URLs from company and filing information
    # This is for cases where we don't even have an instance URL
    
    cik = filing_metadata.get("cik")
    if cik and ticker and filing_type and period_end_date:
        logging.info("Trying to construct URLs from company and filing information")
        
        # Get known accession numbers if available
        accession_number = filing_metadata.get("accession_number")
        if accession_number:
            formatted_acc = accession_number.replace('-', '')
            cik_no_zeros = cik.lstrip('0')
            
            # Construct URL patterns
            base_urls = [
                f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}"
            ]
            
            filenames = [
                f"{ticker}-{period_end_date}.htm",
                f"{filing_type.replace('-', '')}.htm",
                f"{ticker}_{filing_type.replace('-', '')}.htm",
                "primary.htm", 
                "document.htm"
            ]
            
            for base_url in base_urls:
                for filename in filenames:
                    url = f"{base_url}/{filename}"
                    
                    if url in attempted_urls:
                        continue
                        
                    attempted_urls.add(url)
                    logging.info(f"Trying last resort URL: {url}")
                    
                    try:
                        response = sec_request(url)
                        if response.status_code == 200:
                            logging.info(f"Found valid URL using last resort pattern: {url}")
                            return url
                    except Exception:
                        pass
    
    # If nothing worked
    logging.error(f"All URL pattern matching attempts failed after trying {len(attempted_urls)} patterns")
    return None

def process_filing(filing_metadata, include_html=True, include_xbrl=True, use_enhanced_processor=True):
    """Process a single filing and upload to cloud storage with enhanced processing"""
    try:
        ticker = filing_metadata.get("ticker")
        company_name = filing_metadata.get("company_name")
        filing_type = filing_metadata.get("filing_type")
        filing_date = filing_metadata.get("filing_date")
        period_end_date = filing_metadata.get("period_end_date")
        fiscal_year = filing_metadata.get("fiscal_year")
        fiscal_period = filing_metadata.get("fiscal_period")
        cik = filing_metadata.get("cik")
        
        logging.info(f"Processing filing: {ticker} {filing_type} for period {period_end_date}")
        
        # Normalize filing type for foreign companies - convert 20-F to 10-K equivalent for processing
        original_filing_type = filing_type
        if filing_type == "20-F":
            logging.info(f"Detected foreign company filing type 20-F for {ticker}, treating as 10-K equivalent")
            filing_type = "10-K"  # Treat 20-F (foreign annual report) as 10-K equivalent
            filing_metadata["filing_type"] = filing_type
            filing_metadata["original_filing_type"] = original_filing_type
        
        # Fix incorrect company name - enhanced with multiple fallbacks
        if not company_name or company_name == "EDGAR | Company Search Results" or "EDGAR" in company_name:
            logging.info(f"Detected invalid company name '{company_name}', applying fallback methods")
            
            # Method 1: Try to get from CIK first (most reliable)
            if cik:
                try:
                    from src.edgar.edgar_utils import get_company_name_from_cik
                    fetched_name = get_company_name_from_cik(cik)
                    if fetched_name and "EDGAR" not in fetched_name:
                        company_name = fetched_name
                        # Update metadata for future processing
                        filing_metadata["company_name"] = company_name
                        logging.info(f"Fixed company name using CIK lookup: {company_name}")
                except Exception as e:
                    logging.warning(f"Error retrieving company name from CIK: {str(e)}")
            
            # Method 2: Try to find in document URLs if available
            if (not company_name or "EDGAR" in company_name) and "document_url" in filing_metadata:
                doc_url = filing_metadata["document_url"]
                try:
                    # Extract potential company info from URL
                    if "?" in doc_url:
                        doc_url = doc_url.split("?")[0]
                    file_part = doc_url.split("/")[-1]
                    if "-" in file_part:
                        # Format might be like "aapl-20240630.htm"
                        potential_ticker = file_part.split("-")[0].upper()
                        if potential_ticker.isalpha() and len(potential_ticker) <= 5:
                            # This might be a valid ticker - try to map to a company name
                            hardcoded_names = {
                                "AAPL": "Apple Inc.", 
                                "MSFT": "Microsoft Corporation",
                                "GOOGL": "Alphabet Inc.",
                                "AMZN": "Amazon.com, Inc.",
                                "META": "Meta Platforms, Inc.",
                                "TM": "Toyota Motor Corporation",
                                "PYPL": "PayPal Holdings, Inc.",
                                "LOW": "Lowe's Companies, Inc."
                            }
                            if potential_ticker in hardcoded_names:
                                company_name = hardcoded_names[potential_ticker]
                                filing_metadata["company_name"] = company_name
                                logging.info(f"Fixed company name using URL pattern: {company_name}")
                except Exception as e:
                    logging.warning(f"Error extracting company name from URL: {str(e)}")
            
            # Method 3: Try our hardcoded CIK mapping if available
            if (not company_name or "EDGAR" in company_name) and cik:
                try:
                    hardcoded_names = {
                        "0000789019": "Microsoft Corporation", 
                        "0000320193": "Apple Inc.",
                        "0001652044": "Alphabet Inc.",
                        "0001094517": "Toyota Motor Corporation",
                        "0001633917": "PayPal Holdings, Inc.",
                        "0000060667": "Lowe's Companies, Inc.",
                        "0000312069": "Barclays PLC",
                        "0000932477": "Lenovo Group Limited",
                        "0000828146": "Innodata Inc.",
                        "0000080420": "Powell Industries, Inc.",
                        "0001074543": "Hibbett, Inc.",
                        "0001707885": "Bank of N.T. Butterfield & Son Limited",
                        "0001355440": "Embraer S.A."
                    }
                    if cik in hardcoded_names:
                        company_name = hardcoded_names[cik]
                        filing_metadata["company_name"] = company_name
                        logging.info(f"Fixed company name using hardcoded CIK mapping: {company_name}")
                except Exception as e:
                    logging.warning(f"Error using hardcoded CIK mapping: {str(e)}")
            
            # Method 4: If we still don't have a good name, use ticker as last resort
            if not company_name or "EDGAR" in company_name:
                company_name = f"{ticker} Inc."
                filing_metadata["company_name"] = company_name
                logging.info(f"Using ticker-based fallback company name: {company_name}")
        
        # Ensure fiscal information is set using our evidence-based fiscal manager
        if (not fiscal_year or not fiscal_period) and period_end_date:
            try:
                # Use filing content as additional evidence if available
                if "html_content" in filing_metadata:
                    # Add the HTML content as an additional signal source
                    filing_metadata["has_html_content"] = True
                
                # Let fiscal_manager determine the fiscal period
                fiscal_info = fiscal_manager.update_model(ticker, filing_metadata)
                
                # Get standardized values 
                if not fiscal_year:
                    fiscal_year = fiscal_info.get("fiscal_year")
                if not fiscal_period:
                    fiscal_period = fiscal_info.get("fiscal_period")
                    
                # Always set 10-K filings to annual
                if filing_type == "10-K" and fiscal_period != "annual":
                    fiscal_period = "annual"
                
                # Update the metadata
                filing_metadata["fiscal_year"] = fiscal_year
                filing_metadata["fiscal_period"] = fiscal_period
                
                logging.info(f"Set fiscal info using evidence-based system: {fiscal_year}-{fiscal_period} for {ticker}")
                
            except Exception as e:
                logging.warning(f"Error using fiscal manager: {str(e)}, using fallback logic")
                
                # Extract year from period end date (fallback)
                if not fiscal_year:
                    try:
                        if '-' in period_end_date:
                            fiscal_year = period_end_date.split('-')[0]
                        elif len(period_end_date) >= 4:
                            fiscal_year = period_end_date[:4]
                        else:
                            fiscal_year = str(datetime.datetime.now().year)
                        
                        filing_metadata["fiscal_year"] = fiscal_year
                        logging.info(f"Set fiscal_year from period_end_date (fallback): {fiscal_year}")
                    except Exception as e:
                        fiscal_year = str(datetime.datetime.now().year)
                        filing_metadata["fiscal_year"] = fiscal_year
                        logging.warning(f"Using current year ({fiscal_year}) for fiscal_year due to error: {str(e)}")
                
                # If still no fiscal_period, use basic fallback
                if not fiscal_period:
                    if filing_type == "10-K":
                        fiscal_period = "annual"
                    else:
                        fiscal_period = "Q"  # Generic quarter if we can't determine which one
                    
                    filing_metadata["fiscal_period"] = fiscal_period
                    logging.info(f"Set fiscal_period using fallback logic: {fiscal_period}")
        
        # Check if this filing already exists in Firestore
        db = firestore.Client(database=FIRESTORE_DB)
        if check_existing_filing(ticker, filing_type, fiscal_year, fiscal_period, db):
            logging.info(f"Skipping existing filing: {ticker} {filing_type} {fiscal_year} {fiscal_period}")
            return {
                "ticker": ticker,
                "filing_type": filing_type,
                "fiscal_year": fiscal_year,
                "fiscal_period": fiscal_period,
                "status": "skipped",
                "reason": "already exists"
            }
        
        results = {
            "ticker": ticker,
            "filing_type": filing_type,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "filing_date": filing_date
        }
        
        # FIRST STEP: Find document URLs using enhanced document table approach
        # This is crucial for both HTML and XBRL/iXBRL processing
        attempted_urls = []
        if "document_url" not in filing_metadata:
            logging.info(f"No document URL in metadata, using document table approach for {ticker} {filing_type}")
            
            # Get document URLs from document table
            # This is the enhanced approach that parses SEC document tables
            document_url = extract_document_url_from_filing_metadata(filing_metadata)
            
            if document_url:
                filing_metadata["document_url"] = document_url
                attempted_urls.append(document_url)
                logging.info(f"Found document URL using document table approach: {document_url}")
                
                # If it's an iXBRL URL (contains ix?doc=), also set html_url
                if 'ix?doc=' in document_url:
                    html_url = document_url.split('ix?doc=')[-1]
                    filing_metadata["html_url"] = html_url
                    logging.info(f"Setting HTML URL from iXBRL URL: {html_url}")
                
                # Get index page URL from document table if available
                if "index_url" in filing_metadata:
                    logging.info(f"Using index URL from document table: {filing_metadata['index_url']}")
                
                # Also check if we found XBRL instance URL in document table
                if "instance_url" in filing_metadata:
                    logging.info(f"Using XBRL instance URL from document table: {filing_metadata['instance_url']}")
                elif "xbrl_url" in filing_metadata:
                    logging.info(f"Using XBRL URL from document table: {filing_metadata['xbrl_url']}")
            else:
                logging.warning(f"Document table approach failed to find document URL for {ticker} {filing_type}")
        else:
            document_url = filing_metadata["document_url"]
            attempted_urls.append(document_url)
            logging.info(f"Using existing document URL from metadata: {document_url}")
            
            # If it's an iXBRL URL but no html_url is set, derive it
            if 'ix?doc=' in document_url and "html_url" not in filing_metadata:
                html_url = document_url.split('ix?doc=')[-1]
                filing_metadata["html_url"] = html_url
                logging.info(f"Setting HTML URL from existing iXBRL URL: {html_url}")
        
        # Define file size validation thresholds
        # These are used to ensure we're getting complete data
        SIZE_THRESHOLDS = {
            "text": {
                "min": 10 * 1024,  # 10 KB minimum for text files
                "warn": 50 * 1024,  # 50 KB warning threshold
                "units": "KB"
            },
            "llm": {
                "min": 500 * 1024,  # 500 KB minimum for LLM files
                "warn": 1 * 1024 * 1024,  # 1 MB warning threshold
                "units": "MB"
            }
        }
        
        # Track document URLs we've tried for debugging
        filing_metadata["attempted_urls"] = attempted_urls
        
        # Process XBRL first if enhanced processing is enabled
        # This might discover additional document URLs
        llm_content = None
        parsed_result = {}  # Initialize to avoid reference errors
        
        if include_xbrl and use_enhanced_processor:
            logging.info(f"Using enhanced processor for {ticker} {filing_type}")
            
            # Process filing using enhanced processor (handles both XBRL and iXBRL)
            # Make a deep copy of the metadata to prevent reference issues
            import copy
            xbrl_metadata = copy.deepcopy(filing_metadata)
            
            parsed_result = process_company_filing(xbrl_metadata)
            
            if "error" not in parsed_result:
                # Generate LLM format from the parsed data
                llm_content = generate_llm_format(parsed_result, filing_metadata)
                
                # If we have a processed result with a document URL, add it to metadata for HTML processing
                if "processing_path" in parsed_result:
                    filing_metadata["processing_path"] = parsed_result["processing_path"]
                    
                if "file_path" in parsed_result:
                    filing_metadata["xbrl_file_path"] = parsed_result["file_path"]
                    
                # If we found an iXBRL document URL, update our metadata
                if "document_url" in parsed_result:
                    filing_metadata["document_url"] = parsed_result["document_url"]
                    logging.info(f"Using document URL from enhanced processor: {parsed_result['document_url']}")
                
                # Update with any other discovered URLs from XBRL processing
                for key in ["html_url", "instance_url", "xbrl_url"]:
                    if key in xbrl_metadata and key not in filing_metadata:
                        filing_metadata[key] = xbrl_metadata[key]
                        logging.info(f"Updating {key} from XBRL processing: {xbrl_metadata[key]}")
            else:
                logging.warning(f"Error in enhanced processor: {parsed_result.get('error', 'Unknown error')}")
                
                # Even if XBRL processing failed, let's retry our document URL detection
                if "document_url" not in filing_metadata:
                    document_url = extract_document_url_from_filing_metadata(filing_metadata)
                    if document_url:
                        filing_metadata["document_url"] = document_url
                        logging.info(f"Found document URL after XBRL failure: {document_url}")
        
        # Process HTML if requested
        html_processed = False
        if include_html:
            # Try up to 3 attempts for HTML processing (increased from 2)
            max_html_attempts = 3
            html_attempts = 0
            html_processing_result = {"error": "HTML processing not attempted"}
            
            # Keep track of previously tried document URLs to avoid duplicates
            attempted_document_urls = set(attempted_urls)
            
            while html_attempts < max_html_attempts and not html_processed:
                html_attempts += 1
                
                # Make sure we have a document URL (it might have been found by any of the previous steps)
                if "document_url" in filing_metadata:
                    current_url = filing_metadata["document_url"]
                    
                    # If we've already tried this URL, try to find a different one
                    if current_url in attempted_document_urls and html_attempts < max_html_attempts:
                        logging.warning(f"Already tried URL {current_url}, attempting to find a different one")
                        
                        # Force a fresh document table parse with different criteria
                        if "document_url" in filing_metadata:
                            del filing_metadata["document_url"]
                            
                        # Try finding a document with different criteria based on attempt number
                        if html_attempts == 2:
                            # On second attempt, try fallback URL patterns
                            document_url = fallback_url_patterns(filing_metadata)
                        else:
                            # On third attempt, use the document table again but with different selection criteria
                            # Add a hint to try lower priority documents
                            filing_metadata["try_alternative"] = True
                            document_url = extract_document_url_from_filing_metadata(filing_metadata)
                            
                        if document_url and document_url not in attempted_document_urls:
                            filing_metadata["document_url"] = document_url
                            attempted_document_urls.add(document_url)
                            attempted_urls.append(document_url)  # for tracking
                            logging.info(f"HTML attempt {html_attempts}: Found alternative document URL: {document_url}")
                            continue  # Try again with the new URL
                    
                    # Process HTML filing with document URL in metadata
                    logging.info(f"HTML attempt {html_attempts}: Using document URL: {filing_metadata['document_url']}")
                    
                    # Make a deep copy of filing_metadata to avoid any reference issues
                    import copy
                    html_metadata = copy.deepcopy(filing_metadata)
                    
                    # Process HTML filing
                    logging.info(f"Processing HTML for {ticker} {filing_type} {fiscal_year} {fiscal_period}")
                    html_processing_result = process_html_filing(html_metadata)
                    
                    # If HTML processing was successful, check if it found a better document URL
                    if "error" not in html_processing_result:
                        if "document_url" in html_metadata and html_metadata["document_url"] != filing_metadata.get("document_url"):
                            new_url = html_metadata["document_url"]
                            if new_url not in attempted_document_urls:
                                logging.info(f"HTML processing found better document URL: {new_url}")
                                filing_metadata["document_url"] = new_url
                                attempted_document_urls.add(new_url)
                                attempted_urls.append(new_url)  # for tracking
                        
                        local_text_path = html_processing_result.get("file_path")
                        
                        if local_text_path and os.path.exists(local_text_path):
                            logging.info(f"Found local text file: {local_text_path}")
                            
                            # Check file size against minimum threshold
                            text_file_size_bytes = os.path.getsize(local_text_path)
                            min_text_size = SIZE_THRESHOLDS["text"]["min"]
                            warn_text_size = SIZE_THRESHOLDS["text"]["warn"]
                            
                            # Size validation logic
                            if text_file_size_bytes < min_text_size:
                                error_msg = (f"Text file size ({text_file_size_bytes/1024:.1f} KB) is below "
                                           f"minimum threshold of {min_text_size/1024:.1f} KB. "
                                           f"Likely incomplete data.")
                                logging.error(error_msg)
                                results["text_error"] = error_msg
                                
                                # Try again with a different URL if we have more attempts
                                if html_attempts < max_html_attempts:
                                    logging.info(f"Text file too small, trying a different document selection method")
                                    
                                    # Record current URL as attempted
                                    if "document_url" in filing_metadata:
                                        attempted_document_urls.add(filing_metadata["document_url"])
                                        del filing_metadata["document_url"]  # Force a fresh parse
                                    
                                    # Try a different method based on attempt number
                                    if html_attempts == 1:
                                        # On first retry, try document table with alternative selection criteria
                                        filing_metadata["try_alternative"] = True
                                        document_url = extract_document_url_from_filing_metadata(filing_metadata)
                                    else:
                                        # On second retry, try fallback URL patterns
                                        document_url = fallback_url_patterns(filing_metadata)
                                    
                                    if document_url and document_url not in attempted_document_urls:
                                        filing_metadata["document_url"] = document_url
                                        attempted_document_urls.add(document_url)
                                        attempted_urls.append(document_url)  # for tracking
                                        logging.info(f"Found new document URL for next HTML attempt: {document_url}")
                                    else:
                                        logging.warning("Could not find an alternative document URL that hasn't been tried")
                            elif text_file_size_bytes < warn_text_size:
                                # File is valid but suspiciously small
                                warning_msg = (f"Text file size ({text_file_size_bytes/1024:.1f} KB) is below "
                                            f"warning threshold of {warn_text_size/1024:.1f} KB. "
                                            f"Data may be incomplete.")
                                logging.warning(warning_msg)
                                results["text_warning"] = warning_msg
                                html_processed = True
                                
                                # Upload to GCS (small but valid file)
                                text_gcs_path, text_size = upload_to_gcs(
                                    local_text_path, 
                                    ticker, 
                                    filing_type, 
                                    fiscal_year, 
                                    fiscal_period, 
                                    "text"
                                )
                                results["text_file"] = {"local_path": local_text_path, "gcs_path": text_gcs_path, "size": text_size}
                                break  # Exit the retry loop
                            else:
                                # File size is good
                                logging.info(f"Text file size check passed: {text_file_size_bytes/1024:.1f} KB")
                                html_processed = True
                                
                                # Upload to GCS
                                text_gcs_path, text_size = upload_to_gcs(
                                    local_text_path, 
                                    ticker, 
                                    filing_type, 
                                    fiscal_year, 
                                    fiscal_period, 
                                    "text"
                                )
                                results["text_file"] = {"local_path": local_text_path, "gcs_path": text_gcs_path, "size": text_size}
                                break  # Exit the retry loop
                    else:
                        # HTML processing failed, try again with a different URL selection method
                        error = html_processing_result.get("error", "Unknown error")
                        logging.warning(f"HTML attempt {html_attempts} failed: {error}")
                        
                        if html_attempts < max_html_attempts:
                            # Record current URL as attempted
                            if "document_url" in filing_metadata:
                                attempted_document_urls.add(filing_metadata["document_url"])
                                del filing_metadata["document_url"]  # Force a fresh parse
                            
                            # Try a different method based on attempt number
                            if html_attempts == 1:
                                # On first retry, try document table with alternative selection criteria
                                filing_metadata["try_alternative"] = True
                                document_url = extract_document_url_from_filing_metadata(filing_metadata)
                            else:
                                # On second retry, try fallback URL patterns
                                document_url = fallback_url_patterns(filing_metadata)
                            
                            if document_url and document_url not in attempted_document_urls:
                                filing_metadata["document_url"] = document_url
                                attempted_document_urls.add(document_url)
                                attempted_urls.append(document_url)  # for tracking
                                logging.info(f"Retrying HTML with new document URL: {document_url}")
                            else:
                                logging.warning("Could not find an alternative document URL that hasn't been tried")
                else:
                    # No document URL available, try to find one
                    logging.warning(f"No document URL found for HTML processing: {ticker} {filing_type}")
                    
                    # Try different methods based on attempt number
                    if html_attempts == 1:
                        # First attempt: use document table approach
                        document_url = extract_document_url_from_filing_metadata(filing_metadata)
                    elif html_attempts == 2:
                        # Second attempt: try document table with alternative selection criteria
                        filing_metadata["try_alternative"] = True
                        document_url = extract_document_url_from_filing_metadata(filing_metadata)
                    else:
                        # Third attempt: use fallback URL patterns
                        document_url = fallback_url_patterns(filing_metadata)
                    
                    if document_url and document_url not in attempted_document_urls:
                        filing_metadata["document_url"] = document_url
                        attempted_document_urls.add(document_url)
                        attempted_urls.append(document_url)  # for tracking
                        logging.info(f"Found document URL for HTML processing: {document_url}")
                    else:
                        # No document URL found that we haven't tried, cannot process HTML
                        logging.error(f"Could not find any additional document URL for HTML processing")
                        break
            
            # Update metadata with all attempted URLs for diagnostics
            filing_metadata["attempted_urls"] = attempted_urls
        
        # Process XBRL if not already processed and requested
        if include_xbrl and not llm_content:
            # Restore original filing type from before normalization if applicable
            if "original_filing_type" in filing_metadata:
                original_type = filing_metadata["original_filing_type"]
                logging.info(f"Restoring original filing type {original_type} for XBRL processing")
                filing_type = original_type
                filing_metadata["filing_type"] = original_type
            
            # Use document table results to find XBRL instance URLs
            if not ("instance_url" in filing_metadata or "xbrl_url" in filing_metadata):
                # Try another document table parse specifically looking for XBRL instances
                logging.info(f"No XBRL URL found yet, running document table parser to find XBRL instance document")
                
                # Create a copy for XBRL-specific processing
                import copy
                xbrl_metadata = copy.deepcopy(filing_metadata)
                xbrl_metadata["find_xbrl_only"] = True  # Set a flag to prioritize XBRL instances
                
                # Parse document table focusing on XBRL
                document_result = parse_document_table_from_index_page(get_filing_index_url(xbrl_metadata))
                
                if "error" not in document_result:
                    documents = document_result.get("documents", [])
                    selected_docs = select_filing_documents(documents, filing_type, xbrl_metadata)
                    
                    if selected_docs.get("xbrl_instance"):
                        xbrl_doc = selected_docs["xbrl_instance"]
                        xbrl_url = xbrl_doc.get("url")
                        filing_metadata["instance_url"] = xbrl_url
                        logging.info(f"Found XBRL instance URL from document table: {xbrl_url}")
            
            # Use traditional XBRL processing with enhanced validation
            if ("instance_url" in filing_metadata or "xbrl_url" in filing_metadata):
                logging.info(f"Using traditional XBRL processor for {ticker} {filing_type}")
                # Use traditional XBRL processing path from calendar_download.py
                # Import only when needed to avoid circular imports
                from src.xbrl.xbrl_downloader import download_xbrl_instance
                from src.xbrl.xbrl_parser import parse_xbrl_file
                
                # Use instance_url or fall back to xbrl_url
                xbrl_url = filing_metadata.get("instance_url") or filing_metadata.get("xbrl_url")
                # Set xbrl_url for backward compatibility
                filing_metadata["xbrl_url"] = xbrl_url
                
                # Try up to 2 XBRL processing attempts with different URLs
                max_xbrl_attempts = 2
                xbrl_attempts = 0
                xbrl_processing_success = False
                
                # Keep track of tried XBRL URLs
                tried_xbrl_urls = set()
                
                while xbrl_attempts < max_xbrl_attempts and not xbrl_processing_success:
                    xbrl_attempts += 1
                    current_xbrl_url = filing_metadata.get("xbrl_url")
                    
                    # Skip if we've already tried this URL
                    if current_xbrl_url in tried_xbrl_urls:
                        logging.warning(f"Already tried XBRL URL: {current_xbrl_url}")
                        continue
                    
                    tried_xbrl_urls.add(current_xbrl_url)
                    logging.info(f"XBRL attempt {xbrl_attempts}: Processing {current_xbrl_url}")
                    
                    # Download XBRL instance
                    download_result = download_xbrl_instance(filing_metadata)
                    
                    if "error" not in download_result:
                        xbrl_file_path = download_result.get("file_path")
                        
                        # Size validation for XBRL file
                        if os.path.exists(xbrl_file_path):
                            xbrl_size = os.path.getsize(xbrl_file_path)
                            if xbrl_size < 10 * 1024:  # Less than 10KB
                                logging.warning(f"XBRL file is suspiciously small: {xbrl_size/1024:.1f} KB")
                                results["xbrl_warning"] = f"XBRL file is small ({xbrl_size/1024:.1f} KB), might be incomplete"
                            
                            # Parse XBRL with company information
                            parsed_result = parse_xbrl_file(xbrl_file_path, ticker=ticker, filing_metadata=filing_metadata)
                            
                            if "error" not in parsed_result:
                                # Check fact count for validation
                                fact_count = parsed_result.get("fact_count", 0)
                                if fact_count < 50:  # Arbitrary threshold for a reasonable filing
                                    logging.warning(f"XBRL file has very few facts: {fact_count}, data may be incomplete")
                                    results["xbrl_warning"] = f"Only {fact_count} facts found, data may be incomplete"
                                
                                # Generate LLM format
                                llm_content = generate_llm_format(parsed_result, filing_metadata)
                                
                                if llm_content:
                                    xbrl_processing_success = True
                                    logging.info(f"Successfully generated LLM content with {fact_count} facts")
                                    break
                            else:
                                logging.warning(f"Error parsing XBRL: {parsed_result.get('error')}")
                    else:
                        logging.warning(f"Error downloading XBRL: {download_result.get('error', 'Unknown error')}")
                    
                    # If first attempt failed, try an alternative XBRL URL if available
                    if xbrl_attempts == 1 and not xbrl_processing_success:
                        # Try to find another XBRL instance through document table
                        alt_document_result = parse_document_table_from_index_page(get_filing_index_url(filing_metadata))
                        
                        if "error" not in alt_document_result:
                            documents = alt_document_result.get("documents", [])
                            # Set a flag to prioritize XBRL documents
                            filing_metadata["find_xbrl_only"] = True
                            selected_docs = select_filing_documents(documents, filing_type, filing_metadata)
                            
                            # Check if we found an XBRL instance
                            if selected_docs.get("xbrl_instance"):
                                xbrl_doc = selected_docs["xbrl_instance"]
                                xbrl_url = xbrl_doc.get("url")
                                if xbrl_url and xbrl_url not in tried_xbrl_urls:
                                    logging.info(f"Found alternative XBRL URL through document table: {xbrl_url}")
                                    filing_metadata["xbrl_url"] = xbrl_url
                                    break
                            
                            # Fallback to manual search through all documents
                            for doc in documents:
                                # Look for alternative XBRL instances
                                desc = doc.get("description", "").lower()
                                url = doc.get("url", "")
                                if ("xbrl" in desc or ".xml" in url.lower()) and url not in tried_xbrl_urls:
                                    logging.info(f"Found alternative XBRL URL: {url}")
                                    filing_metadata["xbrl_url"] = url
                                    break
            else:
                logging.warning(f"No XBRL URL available for {ticker} {filing_type}")
        
        # If we have LLM content, save it and upload to GCS
        llm_processed = False
        if llm_content:
            # Save LLM content to local file first (keeping existing format)
            local_dir = os.path.join("data", "processed", ticker)
            os.makedirs(local_dir, exist_ok=True)
            
            # Use common naming format
            file_name = f"{company_name.replace(' ', '_')}_{fiscal_year}_{fiscal_period}_{ticker}_{filing_type}_{period_end_date.replace('-', '')}_llm.txt"
            local_llm_path = os.path.join(local_dir, file_name)
            
            with open(local_llm_path, 'w', encoding='utf-8') as f:
                f.write(llm_content)
            
            # Check file size against thresholds
            file_size_bytes = os.path.getsize(local_llm_path)
            min_llm_size = SIZE_THRESHOLDS["llm"]["min"]
            warn_llm_size = SIZE_THRESHOLDS["llm"]["warn"]
            
            # Record file size in results
            results["llm_file_size_bytes"] = file_size_bytes
            results["llm_file_size_mb"] = file_size_bytes / 1024 / 1024
            
            # Add fact count for assessment
            fact_count = parsed_result.get("fact_count", 0) if "fact_count" in parsed_result else 0
            results["fact_count"] = fact_count
            
            # Size validation logic
            if file_size_bytes < min_llm_size:
                error_msg = (f"LLM file size ({file_size_bytes/1024/1024:.2f} MB) is below "
                           f"minimum threshold of {min_llm_size/1024/1024:.2f} MB. "
                           f"Contains only {fact_count} facts, which indicates incomplete data.")
                
                logging.error(error_msg)
                results["llm_error"] = error_msg
                llm_processed = False  # Mark as failed
            elif file_size_bytes < warn_llm_size:
                # File is valid but suspiciously small
                warning_msg = (f"LLM file size ({file_size_bytes/1024/1024:.2f} MB) is below "
                             f"warning threshold of {warn_llm_size/1024/1024:.2f} MB. "
                             f"Contains {fact_count} facts, which may indicate partial data.")
                
                logging.warning(warning_msg)
                results["llm_warning"] = warning_msg
                llm_processed = True  # Still usable
            else:
                # File size is good
                logging.info(f"LLM file size check passed: {file_size_bytes/1024/1024:.2f} MB with {fact_count} facts")
                llm_processed = True
            
            # Upload to GCS
            llm_gcs_path, llm_size = upload_to_gcs(
                local_llm_path, 
                ticker, 
                filing_type, 
                fiscal_year, 
                fiscal_period, 
                "llm"
            )
            results["llm_file"] = {"local_path": local_llm_path, "gcs_path": llm_gcs_path, "size": llm_size}
        
        # Track processing status for both HTML and LLM
        results["html_processed"] = html_processed
        results["llm_processed"] = llm_processed
        
        # Add metadata to Firestore if both files were processed
        if html_processed and llm_processed and "text_file" in results and "llm_file" in results:
            metadata_id = add_filing_metadata(
                company_ticker=ticker,
                company_name=company_name,
                filing_type=filing_type,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                period_end_date=period_end_date,
                filing_date=filing_date,
                text_path=results["text_file"]["gcs_path"],
                llm_path=results["llm_file"]["gcs_path"],
                text_size=results["text_file"]["size"],
                llm_size=results["llm_file"]["size"]
            )
            results["metadata_id"] = metadata_id
            results["status"] = "success"
        else:
            results["status"] = "partial"
            missing_items = []
            if "text_file" not in results or not html_processed:
                missing_items.append("text_file")
            if "llm_file" not in results or not llm_processed:
                missing_items.append("llm_file")
            
            if missing_items:
                results["missing"] = missing_items
        
        return results
    
    except Exception as e:
        logging.error(f"Error processing filing: {str(e)}")
        return {
            "ticker": filing_metadata.get("ticker"),
            "filing_type": filing_metadata.get("filing_type"),
            "status": "error",
            "error": str(e)
        }

def process_company_calendar_range(ticker, start_date, end_date, filing_types, use_enhanced=True):
    """Process all filings for a company within a calendar date range"""
    try:
        logging.info(f"Processing {ticker} filings from {start_date} to {end_date}")
        
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
            "date_range": f"{start_date} to {end_date}",
            "filings_processed": []
        }
        
        # Process 10-K filings first to establish fiscal calendar pattern
        if "10-K" in filing_types:
            # Find all 10-K filings to establish fiscal pattern
            logging.info(f"Finding 10-K filings first to establish fiscal pattern for {ticker}")
            k_filings_result = find_company_filings(ticker, ["10-K"])
            
            if "filings" in k_filings_result and "10-K" in k_filings_result["filings"]:
                # Use 10-K filings to update fiscal model
                filing_metadata = k_filings_result["filings"]["10-K"]
                filing_metadata["ticker"] = ticker
                filing_metadata["company_name"] = company_name
                
                # Update fiscal model with this 10-K filing
                fiscal_info = fiscal_manager.update_model(ticker, filing_metadata)
                logging.info(f"Updated fiscal model for {ticker} based on 10-K filing: {fiscal_info}")
        
        # Find filings for each type
        for filing_type in filing_types:
            filings_result = find_company_filings(ticker, [filing_type])
            
            if "error" in filings_result:
                logging.error(f"Error finding {filing_type} filings for {ticker}: {filings_result['error']}")
                continue
            
            # For calendar year filtering, we need to look at all available filings
            # not just the first one returned
            
            # First, let's check if there are any filings of this type
            if "filings" in filings_result and filing_type in filings_result["filings"]:
                # Let's extract the filing date from the index page
                # The index page contains multiple filings with their dates in a table
                
                # We'll need to parse the index page to get all filings with their dates
                index_page_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}&count=100"
                
                logging.info(f"Getting all {filing_type} filings to filter by date: {index_page_url}")
                try:
                    from src.edgar.edgar_utils import sec_request
                    response = sec_request(index_page_url)
                    
                    if response.status_code == 200:
                        from bs4 import BeautifulSoup
                        
                        soup = BeautifulSoup(response.text, 'html.parser')
                        filing_tables = soup.find_all('table', {'class': 'tableFile2'})
                        
                        if filing_tables:
                            table = filing_tables[0]
                            rows = table.find_all('tr')
                            
                            # Process each row (filing) in the table
                            for row in rows:
                                cells = row.find_all('td')
                                if len(cells) >= 4:  # Ensure we have enough cells
                                    # Extract filing info from this row
                                    row_filing_type = cells[0].text.strip()
                                    filing_date = cells[3].text.strip()
                                    
                                    # Only process if it's the right filing type and within date range
                                    if row_filing_type == filing_type and is_filing_in_range(filing_date, start_date, end_date):
                                        logging.info(f"Found {filing_type} filing from {filing_date} within date range")
                                        
                                        # Get the documents link for this filing
                                        doc_link = None
                                        for link in cells[1].find_all('a'):
                                            if 'documentsbutton' in link.get('id', ''):
                                                doc_link = link.get('href')
                                                break
                                        
                                        if doc_link:
                                            # Process this specific filing
                                            doc_url = f"https://www.sec.gov{doc_link}"
                                            logging.info(f"Processing filing from document link: {doc_url}")
                                            
                                            # We need to fetch this specific filing's metadata
                                            # Rather than processing the latest one from filings_result
                                            specific_filing_result = find_company_filings(ticker, [filing_type], specific_url=doc_url)
                                            
                                            if "filings" in specific_filing_result and filing_type in specific_filing_result["filings"]:
                                                filing_metadata = specific_filing_result["filings"][filing_type]
                                                filing_metadata["filing_date"] = filing_date  # Ensure date is set
                                                
                                                # Add company info to metadata
                                                filing_metadata["ticker"] = ticker
                                                filing_metadata["company_name"] = company_name
                                                
                                                # Process the filing using enhanced processor if requested
                                                filing_result = process_filing(filing_metadata, use_enhanced_processor=use_enhanced)
                                                results["filings_processed"].append(filing_result)
                                        else:
                                            logging.warning(f"Could not find documents link for {filing_type} filing from {filing_date}")
                    else:
                        logging.warning(f"Failed to get index page: {response.status_code}")
                
                except Exception as e:
                    logging.error(f"Error processing index page: {str(e)}")
                    
                    # Fallback to using the latest filing if we can't parse the index
                    logging.info("Falling back to latest filing method")
                    filing_metadata = filings_result["filings"][filing_type]
                    
                    # Check if filing is within date range
                    filing_date = filing_metadata.get("filing_date")
                    if filing_date and is_filing_in_range(filing_date, start_date, end_date):
                        # Add company info to metadata
                        filing_metadata["ticker"] = ticker
                        filing_metadata["company_name"] = company_name
                        
                        # Process the filing using enhanced processor if requested
                        filing_result = process_filing(filing_metadata, use_enhanced_processor=use_enhanced)
                        results["filings_processed"].append(filing_result)
        
        return results
    
    except Exception as e:
        logging.error(f"Error processing {ticker}: {str(e)}")
        return {"ticker": ticker, "error": str(e)}

def download_filings_by_calendar_years(start_year, end_year, companies=None, 
                                     include_10k=True, include_10q=True, 
                                     max_workers=3, use_enhanced=True):
    """
    Download all SEC filings within the specified calendar year range
    
    Args:
        start_year: Starting calendar year (e.g., 2022)
        end_year: Ending calendar year (e.g., 2025)
        companies: List of company tickers (default: use configured list)
        include_10k: Whether to include 10-K filings
        include_10q: Whether to include 10-Q filings
        max_workers: Maximum parallel workers
        use_enhanced: Whether to use enhanced processor (vs. traditional)
        
    Returns:
        Summary of downloaded filings
    """
    # First ensure GCP is configured
    if not configure_gcp():
        logging.error("Failed to configure GCP. Exiting.")
        return {"error": "GCP configuration failed"}
    
    # Generate date range
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    # Define filing types to include
    filing_types = []
    if include_10k:
        filing_types.append("10-K")
    if include_10q:
        filing_types.append("10-Q")
    
    # Use the provided company list, or fallback to default (only for test mode)
    if companies:
        company_list = companies
        logging.info(f"Processing specified companies: {company_list}")
    else:
        # This should only happen in test mode, as we've added validation in main()
        company_list = [company["ticker"] for company in INITIAL_COMPANIES]
        logging.info(f"TEST MODE: Using default companies from config: {company_list}")
    
    logging.info(f"Starting download for {len(company_list)} companies from {start_year} to {end_year}")
    logging.info(f"Companies: {', '.join(company_list)}")
    logging.info(f"Filing types: {filing_types}")
    logging.info(f"Max workers: {max_workers}")
    logging.info(f"Using enhanced processor: {use_enhanced}")
    
    # Process companies in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_company = {
            executor.submit(
                process_company_calendar_range, 
                ticker, 
                start_date, 
                end_date, 
                filing_types,
                use_enhanced
            ): ticker for ticker in company_list
        }
        
        for future in as_completed(future_to_company):
            ticker = future_to_company[future]
            try:
                result = future.result()
                results[ticker] = result
                logging.info(f"Completed processing for {ticker}")
            except Exception as e:
                results[ticker] = {"error": str(e)}
                logging.error(f"Failed processing for {ticker}: {str(e)}")
    
    # Summarize results
    successful_companies = sum(1 for r in results.values() if "error" not in r)
    total_filings = sum(
        len(r.get("filings_processed", [])) 
        for r in results.values() 
        if "error" not in r
    )
    successful_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") == "success")
        for r in results.values() 
        if "error" not in r
    )
    skipped_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") == "skipped")
        for r in results.values() 
        if "error" not in r
    )
    
    summary = {
        "calendar_range": f"{start_year}-{end_year}",
        "companies_processed": len(results),
        "successful_companies": successful_companies,
        "failed_companies": len(results) - successful_companies,
        "total_filings_found": total_filings,
        "successful_filings": successful_filings,
        "skipped_filings": skipped_filings,
        "details": results
    }
    
    logging.info(f"Download summary: {summary['successful_filings']}/{summary['total_filings_found']} filings processed successfully")
    logging.info(f"Skipped filings (already exists): {summary['skipped_filings']}")
    
    return summary

def process_single_filing(ticker, filing_type, gcp_upload=True, use_enhanced=True):
    """Process a single filing for a specific ticker and filing type"""
    
    if gcp_upload:
        if not configure_gcp():
            logging.error("Failed to configure GCP. Exiting.")
            return {"error": "GCP configuration failed"}
    else:
        logging.info("Skipping GCP upload as requested.")
    
    logging.info(f"Processing latest {filing_type} filing for {ticker}")
    
    # Get CIK and company name
    cik = get_cik_from_ticker(ticker)
    if not cik:
        return {"error": f"Could not find CIK for ticker {ticker}"}
    
    company_name = get_company_name_from_cik(cik)
    if not company_name:
        company_name = f"Company {ticker}"
    
    # Find the filing
    filing_result = find_company_filings(ticker, [filing_type])
    
    if "error" in filing_result:
        return {"error": filing_result["error"]}
    
    if "filings" not in filing_result or filing_type not in filing_result["filings"]:
        return {"error": f"No {filing_type} filing found for {ticker}"}
    
    # Get filing metadata
    filing_metadata = filing_result["filings"][filing_type]
    filing_metadata["ticker"] = ticker
    filing_metadata["company_name"] = company_name
    
    # Process the filing
    result = process_filing(
        filing_metadata, 
        include_html=True, 
        include_xbrl=True, 
        use_enhanced_processor=use_enhanced
    )
    
    return {
        "ticker": ticker,
        "filing_type": filing_type,
        "result": result
    }

def main():
    """Main entry point for the enhanced pipeline"""
    parser = argparse.ArgumentParser(description="Enhanced SEC Filing Processing Pipeline")
    
    # Primary operation modes
    parser.add_argument("--calendar-range", action="store_true", help="Process filings by calendar year range")
    parser.add_argument("--single-filing", action="store_true", help="Process a single filing for a specific ticker")
    
    # Calendar range parameters
    parser.add_argument("--start-year", type=int, help="Starting calendar year (e.g., 2022)")
    parser.add_argument("--end-year", type=int, help="Ending calendar year (e.g., 2025)")
    
    # Single filing parameters
    parser.add_argument("--ticker", help="Company ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], help="Type of filing to process")
    
    # Common parameters
    parser.add_argument("--tickers", nargs="+", help="Specific company tickers to process")
    parser.add_argument("--skip-10k", action="store_true", help="Skip 10-K filings")
    parser.add_argument("--skip-10q", action="store_true", help="Skip 10-Q filings")
    parser.add_argument("--workers", type=int, default=3, help="Maximum number of parallel workers")
    parser.add_argument("--traditional", action="store_true", help="Use traditional processing instead of enhanced")
    parser.add_argument("--skip-gcp", action="store_true", help="Skip uploading to GCP")
    
    # Test mode
    parser.add_argument("--test", action="store_true", help="Run in test mode with limited scope")
    
    args = parser.parse_args()
    
    # Set environment variables based on arguments
    if args.skip_gcp:
        os.environ["SKIP_GCP_UPLOAD"] = "1"
        logging.info("Setting SKIP_GCP_UPLOAD=1 environment variable")
    
    # Set up test mode if requested
    if args.test:
        logging.info("Running in TEST MODE with limited scope")
        args.tickers = ["MSFT"]  # Just process Microsoft
        args.start_year = 2022
        args.end_year = 2022
        args.workers = 1
    
    # Determine processing mode
    if args.calendar_range or (args.start_year and args.end_year):
        # Validate years
        if not args.start_year or not args.end_year:
            parser.error("Both --start-year and --end-year are required for calendar range mode")
            
        if args.start_year > args.end_year:
            parser.error("start_year must be less than or equal to end_year")
            
        # Require tickers for calendar range mode
        if not args.tickers and not args.test:
            parser.error("At least one ticker must be specified with --tickers for calendar range mode")
            
        # Get company list
        companies = args.tickers
        
        # Process filings by calendar range
        download_filings_by_calendar_years(
            start_year=args.start_year,
            end_year=args.end_year,
            companies=companies,
            include_10k=not args.skip_10k,
            include_10q=not args.skip_10q,
            max_workers=args.workers,
            use_enhanced=not args.traditional
        )
        
    elif args.single_filing or (args.ticker and args.filing_type):
        # Validate parameters
        if not args.ticker or not args.filing_type:
            parser.error("Both --ticker and --filing-type are required for single filing mode")
            
        # Process a single filing
        result = process_single_filing(
            ticker=args.ticker,
            filing_type=args.filing_type,
            gcp_upload=not args.skip_gcp,
            use_enhanced=not args.traditional
        )
        
        # Report result
        if "error" in result:
            logging.error(f"Error processing filing: {result['error']}")
        else:
            filing_result = result.get("result", {})
            status = filing_result.get("status", "unknown")
            
            if status == "success":
                logging.info(f"Successfully processed {args.ticker} {args.filing_type}")
                logging.info(f"Text file: {filing_result.get('text_file', {}).get('local_path')}")
                logging.info(f"LLM file: {filing_result.get('llm_file', {}).get('local_path')}")
            elif status == "skipped":
                logging.info(f"Filing already exists: {args.ticker} {args.filing_type}")
            else:
                logging.warning(f"Partial processing: {filing_result}")
    
    else:
        # No valid mode specified, show help
        parser.print_help()

if __name__ == "__main__":
    main()