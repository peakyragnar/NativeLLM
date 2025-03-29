#!/usr/bin/env python3
"""
SEC Compliant Downloader

This module implements SEC-compliant file downloading with proper headers,
rate limiting, and error handling.
"""

import os
import time
import logging
import requests
import json
import re
from pathlib import Path
from urllib.parse import urljoin
import random
from bs4 import BeautifulSoup

# Import from config
from src2.config import SEC_BASE_URL, RAW_DATA_DIR

# Constants
SEC_RATE_LIMIT = 10  # Maximum requests per second allowed
DEFAULT_TIMEOUT = 30  # Default timeout in seconds

class SECDownloader:
    """
    SEC-compliant file downloader with rate limiting and proper headers.
    
    This class handles downloading files from SEC EDGAR in compliance with
    their usage policies, including rate limiting and proper identification.
    """
    
    # Known fiscal patterns for popular companies
    COMPANY_FISCAL_YEARS = {
        "MSFT": {"month": 6, "day": 30, "confidence": 1.0},  # Microsoft: Fiscal year ends June 30
        "AAPL": {"month": 9, "day": 30, "confidence": 1.0},  # Apple: Fiscal year ends September 30
        "GOOGL": {"month": 12, "day": 31, "confidence": 1.0},  # Google: Calendar year
        "AMZN": {"month": 12, "day": 31, "confidence": 1.0},  # Amazon: Calendar year
        "WMT": {"month": 1, "day": 31, "confidence": 1.0},  # Walmart: Fiscal year ends January 31
        "TGT": {"month": 1, "day": 31, "confidence": 1.0},  # Target: Fiscal year ends January 31
    }
    
    def __init__(self, user_agent=None, contact_email=None, rate_limit=5, 
                 download_dir=None, enforce_rate_limit=True):
        """
        Initialize the downloader with SEC-compliant settings.
        
        Args:
            user_agent: User agent identification (org/tool name)
            contact_email: Contact email for identification
            rate_limit: Maximum requests per second (should be <= 10)
            download_dir: Directory to save downloaded files (defaults to RAW_DATA_DIR from config)
            enforce_rate_limit: Whether to enforce rate limiting
        """
        # Use RAW_DATA_DIR from config if download_dir is not provided
        if download_dir is None:
            download_dir = f"./{RAW_DATA_DIR}"
        # Set up user agent - required by SEC
        if not user_agent:
            user_agent = "NativeLLM_SECDownloader"
        
        # Ensure we have a contact email
        if not contact_email:
            contact_email = "example@example.com"
            logging.warning("No contact email provided. Using example@example.com")
        
        # Format the user agent string with contact info as requested by SEC
        self.user_agent = f"{user_agent} ({contact_email})"
        
        # Rate limiting settings
        self.rate_limit = min(rate_limit, SEC_RATE_LIMIT)  # Ensure we don't exceed SEC's limit
        self.enforce_rate_limit = enforce_rate_limit
        self._last_request_time = 0
        
        # Set up download directory
        self.download_dir = Path(download_dir)
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Cache for CIK numbers
        self.cik_cache = {}
        
        logging.info(f"Initialized SEC downloader with user agent: {self.user_agent}")
        logging.info(f"Rate limit set to {self.rate_limit} requests per second")
    
    def _get_request_headers(self):
        """
        Get SEC-compliant request headers.
        
        Returns:
            Dictionary of HTTP headers for SEC requests
        """
        return {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov" if "sec.gov" in SEC_BASE_URL else None, 
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Connection": "keep-alive"
        }
    
    def _enforce_rate_limit(self):
        """
        Enforce rate limiting by sleeping if necessary.
        """
        if not self.enforce_rate_limit:
            return
            
        # Calculate time since last request
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        
        # Calculate minimum interval between requests to maintain rate limit
        min_interval = 1.0 / self.rate_limit
        
        # Add a small random delay to avoid request bunching
        jitter = random.uniform(0, 0.1)
        min_interval += jitter
        
        # Sleep if we need to enforce rate limit
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logging.debug(f"Rate limiting: sleeping for {sleep_time:.4f}s")
            time.sleep(sleep_time)
        
        # Update last request time
        self._last_request_time = time.time()
    
    def _handle_sec_response(self, response, url):
        """
        Handle SEC response with proper error handling.
        
        Args:
            response: Response object from requests
            url: URL that was requested
            
        Returns:
            Response object if successful
            
        Raises:
            Exception on error with appropriate message
        """
        if response.status_code == 200:
            return response
        
        if response.status_code == 403:
            # Check for known SEC rate limit or bot detection messages
            if "Request Rate Threshold Exceeded" in response.text:
                logging.error(f"SEC rate limit exceeded. Implement appropriate backoff.")
                raise Exception("SEC rate limit exceeded. Wait 10 minutes before retrying.")
            
            if "Unauthorized" in response.text or "403 Forbidden" in response.text:
                logging.error(f"SEC rejected request as unauthorized. Check User-Agent.")
                raise Exception("SEC rejected request. Verify User-Agent has contact info.")
        
        if response.status_code == 404:
            logging.warning(f"File not found: {url}")
            raise Exception(f"File not found: {url}")
        
        # Generic error handler
        logging.error(f"SEC request failed with code {response.status_code}: {url}")
        raise Exception(f"SEC request failed: HTTP {response.status_code}")
    
    def download_file(self, url, save_path=None, timeout=DEFAULT_TIMEOUT):
        """
        Download a file from SEC with proper rate limiting and headers.
        
        Args:
            url: URL to download (will be joined with SEC_BASE_URL if not absolute)
            save_path: Path to save the file (if None, returns content instead)
            timeout: Request timeout in seconds
            
        Returns:
            Path to saved file if save_path provided, otherwise content
        """
        # Enforce rate limiting
        self._enforce_rate_limit()
        
        # Ensure URL is properly formatted
        if not url.startswith(("http://", "https://")):
            full_url = urljoin(SEC_BASE_URL, url)
        else:
            full_url = url
        
        logging.info(f"Downloading: {full_url}")
        
        try:
            # Make request with proper headers
            response = requests.get(
                full_url,
                headers=self._get_request_headers(),
                timeout=timeout
            )
            
            # Handle response
            self._handle_sec_response(response, full_url)
            
            # Save or return content
            if save_path:
                # Create directory if it doesn't exist
                save_path = Path(save_path)
                os.makedirs(save_path.parent, exist_ok=True)
                
                # Save file
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                
                logging.info(f"Downloaded {len(response.content)} bytes to {save_path}")
                return save_path
            else:
                # Return content
                logging.info(f"Downloaded {len(response.content)} bytes")
                return response.content
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")
            raise Exception(f"Failed to download {full_url}: {str(e)}")
    
    def lookup_cik(self, ticker):
        """
        Look up a company's CIK number by ticker symbol.
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            CIK number as string
        """
        ticker = ticker.upper()
        
        # Try to get CIK from SEC's ticker --> CIK JSON file
        ticker_url = "https://www.sec.gov/files/company_tickers.json"
        
        self._enforce_rate_limit()
        
        try:
            # Make request with proper headers
            response = requests.get(
                ticker_url,
                headers=self._get_request_headers(),
                timeout=DEFAULT_TIMEOUT
            )
            
            # Handle response
            self._handle_sec_response(response, ticker_url)
            
            # Parse JSON and search for ticker
            data = response.json()
            
            # The SEC's JSON is structured as {"0":{"cik_str":1750, "ticker":"TWOU", "title":"2U, Inc."}, "1":{...}}
            for _, company in data.items():
                if company.get("ticker") == ticker:
                    cik = str(company.get("cik_str"))
                    logging.info(f"Found CIK {cik} for ticker {ticker}")
                    return cik
            
            logging.error(f"Could not find CIK for ticker {ticker}")
            raise Exception(f"Could not find CIK for ticker {ticker}")
            
        except Exception as e:
            logging.error(f"Failed to lookup CIK for {ticker}: {str(e)}")
            raise Exception(f"Failed to lookup CIK for {ticker}: {str(e)}")
    
    def get_company_filings_direct(self, ticker=None, cik=None, filing_type="10-K", count=1):
        """
        Get recent filings for a company using direct EDGAR browsing.
        
        This method avoids the submissions API and directly crawls the EDGAR
        browser interface to find recent filings.
        
        Args:
            ticker: Company ticker symbol (optional if CIK provided)
            cik: Company CIK number (optional if ticker provided)
            filing_type: Type of filing to retrieve (10-K, 10-Q, etc.)
            count: Number of recent filings to retrieve
            
        Returns:
            List of filing information dictionaries
        """
        if not ticker and not cik:
            raise ValueError("Either ticker or CIK must be provided")
        
        # Look up CIK if only ticker provided
        if not cik and ticker:
            cik = self.lookup_cik(ticker)
        
        # Format CIK without leading zeros for URL
        cik_no_zeros = str(cik).lstrip('0')
        
        # Construct company filings browse URL
        browse_url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcompany&CIK={cik_no_zeros}&type={filing_type}&count={count*2}"
        
        # Download filings page
        self._enforce_rate_limit()
        
        try:
            # Make request with proper headers
            response = requests.get(
                browse_url,
                headers=self._get_request_headers(),
                timeout=DEFAULT_TIMEOUT
            )
            
            # Handle response
            self._handle_sec_response(response, browse_url)
            
            # For debugging, save the HTML content
            debug_path = Path(self.download_dir) / "debug_edgar_page.html"
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            logging.info(f"Saved debug HTML to {debug_path}")
            
            # Parse using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            filings = []
            
            # Find all filing rows in the document
            # Interactive filings table - modern SEC EDGAR uses this table
            filing_table = soup.find('table', {'class': 'tableFile2'})
            
            if filing_table:
                # Get rows from the table (skip header row)
                rows = filing_table.find_all('tr')[1:]  # Skip header row
                
                # Process each row to get filing info
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 5:  # Ensure enough columns
                        try:
                            # Extract filing data from columns
                            filing_type_col = cols[0].get_text().strip()
                            
                            # Only process rows that match our filing type
                            if filing_type.lower() in filing_type_col.lower():
                                # Check if this is an amended filing (10-K/A or 10-Q/A)
                                is_amended = False
                                # Check multiple patterns for amended filings
                                amended_patterns = [
                                    r'10-[KQ]/A',           # Standard SEC format: 10-K/A
                                    r'10-[KQ]\s*\(Amended', # Alternative format: 10-K (Amended)
                                    r'Amended\s*10-[KQ]',   # Another format: Amended 10-K
                                    r'10[KQ]/A'             # Compact format: 10K/A
                                ]
                                
                                for pattern in amended_patterns:
                                    if re.search(pattern, filing_type_col, re.IGNORECASE):
                                        is_amended = True
                                        logging.info(f"Found amended filing: {filing_type_col} (matched pattern: {pattern})")
                                        break
                                
                                # If this is an amended filing, skip it entirely
                                # This is crucial to prevent processing amendments in the normal workflow
                                if is_amended:
                                    logging.info(f"SKIPPING amended filing {filing_type_col} in regular mode")
                                    continue  # Skip this filing entirely
                                
                                # Extract the base filing type (10-K or 10-Q) for consistency
                                base_filing_type = filing_type
                                if is_amended and filing_type in ["10-K", "10-Q"]:
                                    # Store the actual displayed type for reference
                                    filing_amendment_info = filing_type_col
                                
                                # Extract data from other columns
                                documents_link = cols[1].find('a')
                                description = cols[2].get_text().strip() if len(cols) > 2 else ""
                                filing_date = cols[3].get_text().strip() if len(cols) > 3 else ""
                                file_number = cols[4].get_text().strip() if len(cols) > 4 else ""
                                
                                # Extract documents URL
                                documents_url = None
                                if documents_link and documents_link.has_attr('href'):
                                    documents_url = documents_link['href']
                                
                                # Extract accession number from documents URL
                                accession_number = None
                                if documents_url:
                                    # Try different formats of accession numbers
                                    
                                    # First try: accession_number parameter in URL (older format)
                                    acc_match = re.search(r'accession_number=(\d+-\d+-\d+)', documents_url)
                                    
                                    # Second try: direct pattern in the URL path (newer format)
                                    if not acc_match:
                                        acc_match = re.search(r'/(\d+-\d+-\d+)-index', documents_url)
                                    
                                    # Third try: extract from directory structure
                                    if not acc_match:
                                        # Format like /Archives/edgar/data/789019/000095017025010491/
                                        dir_match = re.search(r'/data/\d+/(\d+)/', documents_url)
                                        if dir_match:
                                            # Format the directory number as an accession number
                                            acc_dir = dir_match.group(1)
                                            if len(acc_dir) >= 18:  # Length of standard SEC accession with no dashes
                                                # Format as SEC standard: 0000950170-25-010491
                                                fmt_acc = f"{acc_dir[:10]}-{acc_dir[10:12]}-{acc_dir[12:]}"
                                                accession_number = fmt_acc
                                                logging.info(f"Extracted accession number {accession_number} from directory {acc_dir}")
                                    
                                    # If we found a match in any format
                                    if acc_match:
                                        accession_number = acc_match.group(1)
                                        logging.info(f"Found accession number: {accession_number}")
                                
                                # Only include if we got an accession number
                                if accession_number:
                                    # Create base filing object
                                    filing = {
                                        "accession_number": accession_number,
                                        "filing_date": filing_date,
                                        "filing_type": filing_type,
                                        "description": description,
                                        "file_number": file_number,
                                        "documents_url": documents_url,
                                        "cik": cik,
                                        "ticker": ticker,
                                        "is_amended": is_amended,
                                        "original_filing_type": filing_type_col
                                    }
                                    
                                    # Add specific amendment information if this is an amended filing
                                    if is_amended:
                                        filing["amendment_info"] = {
                                            "original_type": base_filing_type,
                                            "displayed_type": filing_type_col,
                                            "amendment_number": 1  # Default to 1, could extract from name if needed
                                        }
                                        # Add a special flag to indicate this should be stored in the "/a" subdirectory
                                        filing["use_amendment_subdirectory"] = True
                                        logging.info(f"Marked filing as amended, will use '/a' subdirectory")
                                    
                                    # We need to extract period_end_date for proper fiscal year calculation
                                    # This requires downloading and parsing the filing's index page
                                    try:
                                        full_doc_url = f"{SEC_BASE_URL}{documents_url}" if documents_url.startswith('/') else documents_url
                                        logging.info(f"Fetching details from {full_doc_url} to extract period end date")
                                        
                                        # Apply rate limiting
                                        self._enforce_rate_limit()
                                        
                                        # Download the document page
                                        doc_response = requests.get(
                                            full_doc_url,
                                            headers=self._get_request_headers(),
                                            timeout=DEFAULT_TIMEOUT
                                        )
                                        
                                        # Check if successful
                                        if doc_response.status_code == 200:
                                            # Look for period of report
                                            period_match = re.search(r'Period of Report</div>\s*<div[^>]*>([^<]+)', doc_response.text)
                                            if period_match:
                                                raw_period_end_date = period_match.group(1).strip()
                                                
                                                # CRITICAL: Validate the period_end_date at extraction
                                                # This is the first defense against bad data entering the system
                                                try:
                                                    # Import validation function
                                                    from src2.sec.fiscal.fiscal_data import validate_period_end_date, FiscalDataError
                                                    
                                                    # Normalize and validate the date
                                                    normalized_date = validate_period_end_date(raw_period_end_date)
                                                    
                                                    # Only set validated date in filing info with complete metadata
                                                    filing["period_end_date"] = normalized_date
                                                    filing["period_end_date_raw"] = raw_period_end_date
                                                    filing["period_end_date_validated"] = True
                                                    filing["period_end_date_validation_timestamp"] = datetime.datetime.now().isoformat()
                                                    
                                                    # Add data integrity metadata for audit
                                                    filing["data_integrity"] = {
                                                        "period_end_date_validated": True,
                                                        "validation_timestamp": datetime.datetime.now().isoformat(),
                                                        "validation_source": "downloader.py:extract_validation",
                                                        "raw_value": raw_period_end_date,
                                                        "normalized_value": normalized_date
                                                    }
                                                    
                                                    logging.info(f"DATA INTEGRITY: Extracted and validated period_end_date: {normalized_date} for {accession_number}")
                                                except (ImportError, FiscalDataError) as e:
                                                    logging.error(f"DATA INTEGRITY ERROR: Period end date validation failed: {str(e)}")
                                                    
                                                    # Circuit breaker pattern - fail early to prevent bad data
                                                    # Note: Setting to None will trigger errors in downstream components
                                                    # rather than allowing bad data to propagate
                                                    filing["period_end_date"] = None
                                                    filing["period_end_date_raw"] = raw_period_end_date
                                                    filing["period_end_date_validated"] = False
                                                    filing["period_end_date_error"] = str(e)
                                                    
                                                    # Add data integrity error metadata for audit
                                                    filing["data_integrity"] = {
                                                        "period_end_date_validated": False,
                                                        "validation_timestamp": datetime.datetime.now().isoformat(),
                                                        "validation_source": "downloader.py:extract_validation",
                                                        "raw_value": raw_period_end_date,
                                                        "error": str(e)
                                                    }
                                    except Exception as e:
                                        logging.warning(f"Error getting period_end_date for {accession_number}: {str(e)}")
                                        # Continue without period_end_date - we'll handle missing dates elsewhere
                                    
                                    # Calculate URLs for downstream use
                                    acc_no_dashes = accession_number.replace('-', '')
                                    filing["index_url"] = f"/Archives/edgar/data/{cik_no_zeros}/{acc_no_dashes}/{accession_number}-index.htm"
                                    
                                    filings.append(filing)
                                    
                                    # Break if we have enough filings
                                    if len(filings) >= count:
                                        break
                        except Exception as row_e:
                            logging.warning(f"Error processing row: {str(row_e)}")
                            continue
            
            if not filings:
                # No filings found through the standard method
                logging.info(f"No {filing_type} filings found for {ticker or cik} using standard method")
                
                # We could add alternative search methods here in the future
                # For example: search by different URL patterns, try alternative APIs, etc.
            
            # Sort filings to prioritize non-amended filings
            # This ensures original filings come before amendments
            if filings:
                # Count amended vs non-amended filings
                amended_count = sum(1 for f in filings if f.get("is_amended", False))
                original_count = len(filings) - amended_count
                
                if amended_count > 0:
                    logging.info(f"Found {amended_count} amended filings and {original_count} original filings")
                    
                    # Sort filings with non-amended first
                    filings.sort(key=lambda x: (x.get("is_amended", False), x.get("filing_date", "")))
                    
                    # Log the first few filings after sorting
                    for i, f in enumerate(filings[:3]):
                        amended_status = "AMENDED" if f.get("is_amended", False) else "ORIGINAL"
                        logging.info(f"Filing {i+1}: {amended_status} {f.get('original_filing_type')} from {f.get('filing_date')}")
            
            logging.info(f"Found {len(filings)} {filing_type} filings for {ticker or cik}")
            return filings
            
        except Exception as e:
            logging.error(f"Error getting company filings: {str(e)}")
            raise Exception(f"Error getting company filings: {str(e)}")
    
    def download_filing(self, filing_info, resolve_urls=True):
        """
        Download a filing and its associated files.
        
        Args:
            filing_info: Filing information dictionary
            resolve_urls: Whether to resolve document URLs by downloading index
            
        Returns:
            Dictionary with paths to downloaded files
        """
        # Extract necessary info
        ticker = filing_info.get("ticker", "unknown")
        cik = filing_info.get("cik", "").lstrip('0')
        filing_type = filing_info.get("filing_type", "unknown")
        accession_number = filing_info.get("accession_number", "")
        
        if not cik or not accession_number:
            return {"error": "Missing CIK or accession number"}
        
        # Create safe filename version of accession number
        acc_no_dashes = accession_number.replace('-', '')
        
        # Create directory structure
        filing_dir = Path(self.download_dir) / ticker / filing_type / acc_no_dashes
        os.makedirs(filing_dir, exist_ok=True)
        
        # First download the index page to find the actual document URLs
        index_url = filing_info.get("index_url")
        
        if not index_url:
            index_url = f"/Archives/edgar/data/{cik}/{acc_no_dashes}/{accession_number}-index.htm"
        
        # Download index file
        index_path = filing_dir / "index.htm"
        try:
            self.download_file(index_url, index_path)
        except Exception as idx_e:
            # If the first index URL fails, try an alternative format
            logging.warning(f"Failed to download index from {index_url}: {str(idx_e)}")
            alt_index_url = f"/Archives/edgar/data/{cik}/{acc_no_dashes}/index.htm"
            logging.info(f"Trying alternative index URL: {alt_index_url}")
            try:
                self.download_file(alt_index_url, index_path)
                index_url = alt_index_url  # Update for downstream use
            except Exception as alt_idx_e:
                logging.error(f"Failed to download alternative index: {str(alt_idx_e)}")
                return {"error": f"Failed to download index: {str(alt_idx_e)}"}
        
        # Parse index file to find the primary document
        result = {
            "filing_info": filing_info,
            "filing_dir": str(filing_dir),
            "index_path": str(index_path)
        }
        
        if resolve_urls:
            try:
                # Read index file
                with open(index_path, 'r', encoding='utf-8') as f:
                    index_content = f.read()
                
                # Use BeautifulSoup for more robust parsing
                soup = BeautifulSoup(index_content, 'html.parser')
                
                # Find document tables - look in all tables
                all_tables = soup.find_all('table', {'class': 'tableFile'})
                
                # Debug output
                logging.info(f"Found {len(all_tables)} tableFile tables in index file")
                
                # Initialize variables
                primary_doc_url = None
                primary_doc_name = None
                
                # Process all tables
                for doc_table in all_tables:
                    if not primary_doc_url:  # Only continue if we haven't found a primary document
                        # Find document links
                        rows = doc_table.find_all('tr')[1:]  # Skip header row
                        
                        for row in rows:
                            cells = row.find_all('td')
                            if len(cells) >= 3:  # Need at least 3 cells
                                # Look for document link in any cell (usually 3rd cell in newer format)
                                doc_link = None
                                doc_cell_idx = 0
                                
                                # Find the first cell with a link
                                for i, cell in enumerate(cells):
                                    if cell.find('a'):
                                        doc_link = cell.find('a')
                                        doc_cell_idx = i
                                        logging.info(f"Found document link in cell {i}: {doc_link['href']}")
                                        break
                                
                                if doc_link and doc_link.has_attr('href'):
                                    doc_url = doc_link['href']
                                    doc_name = doc_link.get_text().strip()
                                    
                                    # In newer format, description is in cell before the link, and type is in cell after
                                    if doc_cell_idx > 0 and doc_cell_idx < len(cells) - 1:
                                        description = cells[doc_cell_idx - 1].get_text().strip()
                                        doc_type = cells[doc_cell_idx + 1].get_text().strip()
                                    else:
                                        # Fallback to original positions
                                        description = cells[1].get_text().strip() if len(cells) > 1 else ""
                                        doc_type = cells[3].get_text().strip() if len(cells) > 3 else ""
                                        
                                    # Log document info
                                    logging.info(f"Found document in table: {doc_name}, type: {doc_type}, description: {description}")
                                
                                # Check for primary document indicators
                                is_primary = False
                                
                                # Various heuristics to identify the primary document
                                if filing_type.lower() == doc_type.lower():
                                    is_primary = True
                                elif "complete submission" in description.lower():
                                    is_primary = True
                                elif filing_type.lower() in doc_name.lower():
                                    is_primary = True
                                elif doc_name.lower().endswith((".htm", ".html")) and not is_primary and not primary_doc_url:
                                    # Take first HTML file as fallback
                                    is_primary = True
                                
                                if is_primary:
                                    primary_doc_url = doc_url
                                    primary_doc_name = doc_name
                                    break
                    
                    if not primary_doc_url:
                        # If we couldn't find a primary document in the table, use document naming patterns
                        # This is a more robust approach that works for multiple companies and filing types
                        logging.info(f"No primary document found in tables, trying document naming patterns")
                        
                        # Try standard naming patterns
                        ticker_lower = ticker.lower() if ticker else ""
                        if acc_no_dashes and ticker_lower:
                            # Look for possible primary document filenames
                            possible_filenames = []
                            
                            # If we have a period_end_date, include date-based patterns
                            period_end_date = filing_info.get("period_end_date", "")
                            if period_end_date:
                                date_str = period_end_date.replace('-', '')
                                possible_filenames.extend([
                                    f"{ticker_lower}-{date_str}.htm",  # msft-20241231.htm
                                    f"{ticker_lower}_{date_str}.htm",  # msft_20241231.htm
                                    f"{ticker_lower}-{filing_type.lower()}_{date_str}.htm",  # msft-10k_20241231.htm
                                ])
                            
                            # Also include non-date patterns
                            possible_filenames.extend([
                                f"{ticker_lower}-{filing_type.lower()}.htm",  # msft-10k.htm
                                f"{ticker_lower}_{filing_type.lower()}.htm",  # msft_10k.htm
                                f"{ticker_lower.upper()}.htm",  # MSFT.htm
                                f"{ticker_lower}.htm",         # msft.htm
                                "index.htm",                   # index.htm
                                "Filing.htm"                   # Filing.htm
                            ])
                            
                            # Try each filename and test if it exists
                            for filename in possible_filenames:
                                test_url = f"/Archives/edgar/data/{cik}/{acc_no_dashes}/{filename}"
                                test_full_url = f"{SEC_BASE_URL}{test_url}"
                                logging.info(f"Trying potential document URL: {test_url}")
                                
                                try:
                                    # Apply rate limiting
                                    self._enforce_rate_limit()
                                    
                                    # Send a HEAD request to check if the URL exists
                                    head_response = requests.head(
                                        test_full_url,
                                        headers=self._get_request_headers(),
                                        timeout=DEFAULT_TIMEOUT / 2  # Shorter timeout for HEAD
                                    )
                                    
                                    if head_response.status_code == 200:
                                        logging.info(f"Found valid document URL: {test_url}")
                                        primary_doc_url = test_url
                                        primary_doc_name = filename
                                        break
                                except Exception as e:
                                    logging.debug(f"Error checking URL {test_url}: {str(e)}")
                                    # Continue with next filename
                    
                    if primary_doc_url:
                        # Special handling for iXBRL links which start with "/ix?doc="
                        if primary_doc_url.startswith('/ix?doc='):
                            # Extract the actual document path from the iXBRL viewer URL
                            ixbrl_doc_path = primary_doc_url.replace('/ix?doc=', '')
                            logging.info(f"Converting iXBRL link to direct document link: {ixbrl_doc_path}")
                            primary_doc_url = ixbrl_doc_path
                        
                        # Convert relative URL to absolute if needed
                        if not primary_doc_url.startswith(('http://', 'https://')):
                            if primary_doc_url.startswith('/'):
                                primary_doc_url = f"{SEC_BASE_URL}{primary_doc_url}"
                            else:
                                # Relative to index URL directory
                                base_url = '/'.join(index_url.split('/')[:-1])
                                base_url = f"{SEC_BASE_URL}{base_url}" if not base_url.startswith(('http://', 'https://')) else base_url
                                primary_doc_url = f"{base_url}/{primary_doc_url}"
                                
                        logging.info(f"Final document URL: {primary_doc_url}")
                        
                        # Download primary document
                        primary_doc_path = filing_dir / primary_doc_name
                        self.download_file(primary_doc_url, primary_doc_path)
                        
                        # Store the path
                        result["doc_path"] = str(primary_doc_path)
                        
                        # Update the filing_info with the actual document URL
                        filing_info["primary_doc_url"] = primary_doc_url
                        filing_info["primary_doc_name"] = primary_doc_name
                        
                        # Construct iXBRL viewer URL
                        sec_path = primary_doc_url.replace(SEC_BASE_URL, "")
                        filing_info["ixbrl_url"] = f"/ix?doc={sec_path}"
                
            except Exception as e:
                logging.error(f"Error parsing index file: {str(e)}")
                result["error"] = f"Error parsing index file: {str(e)}"
        
        # Save filing info as JSON for reference
        info_path = filing_dir / "filing_info.json"
        with open(info_path, 'w') as f:
            json.dump(filing_info, f, indent=2)
        
        result["info_path"] = str(info_path)
        
        return result

    def get_company_filings(self, ticker=None, cik=None, filing_type="10-K", count=1):
        """
        Get recent filings for a company.
        
        Args:
            ticker: Company ticker symbol (optional if CIK provided)
            cik: Company CIK number (optional if ticker provided)
            filing_type: Type of filing to retrieve (10-K, 10-Q, etc.)
            count: Number of recent filings to retrieve
            
        Returns:
            List of filing information dictionaries
        """
        # Use direct EDGAR browsing method
        return self.get_company_filings_direct(ticker, cik, filing_type, count)


# Example usage
if __name__ == "__main__":
    import argparse
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Download SEC filings")
    parser.add_argument("ticker", help="Ticker symbol of the company")
    parser.add_argument("--filing-type", default="10-K", help="Filing type (10-K, 10-Q, etc.)")
    parser.add_argument("--count", type=int, default=1, help="Number of recent filings to download")
    parser.add_argument("--email", help="Contact email for User-Agent")
    parser.add_argument("--output", help="Output directory for downloaded files")
    
    args = parser.parse_args()
    
    # Create downloader
    downloader = SECDownloader(
        user_agent=f"NativeLLM_SECDownloader/1.0",
        contact_email=args.email or "user@example.com",
        download_dir=args.output or "./sec_downloads"
    )
    
    # Get filings
    filings = downloader.get_company_filings(
        ticker=args.ticker,
        filing_type=args.filing_type,
        count=args.count
    )
    
    # Download each filing
    for filing in filings:
        result = downloader.download_filing(filing)
        print(f"Downloaded {filing['filing_type']} ({filing['filing_date']}) to {result.get('doc_path', 'unknown location')}")