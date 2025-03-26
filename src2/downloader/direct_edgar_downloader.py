"""
Direct SEC Edgar Downloader Module

Alternative downloader that uses direct HTTP requests to SEC EDGAR.
"""

import os
import re
import json
import time
import logging
import tempfile
import requests
from pathlib import Path


class DirectEdgarDownloader:
    """
    Download SEC filings directly using HTTP requests to SEC EDGAR
    """
    
    def __init__(self, user_agent):
        """
        Initialize with a valid SEC user agent
        
        Args:
            user_agent: Valid user agent for SEC EDGAR (Company Email format)
        """
        self.user_agent = user_agent
        self.headers = {'User-Agent': user_agent}
        self.base_url = "https://www.sec.gov"
        self.archives_url = "https://www.sec.gov/Archives/edgar/data"
        logging.info(f"Initialized DirectEdgarDownloader with user agent: {user_agent}")
    
    def get_cik_from_ticker(self, ticker):
        """
        Convert ticker to CIK
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            CIK number or None if not found
        """
        url = f"{self.base_url}/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany"
        logging.info(f"Looking up CIK for ticker: {ticker}")
        
        try:
            response = self._make_request(url)
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
        
        except Exception as e:
            logging.error(f"Error getting CIK for {ticker}: {str(e)}")
            return None
    
    def find_filings_by_type(self, ticker, filing_type, count=1):
        """
        Find filings by type for a ticker
        
        Args:
            ticker: Company ticker symbol
            filing_type: Filing type (10-K or 10-Q)
            count: Number of filings to return
            
        Returns:
            List of filing URLs
        """
        # Get CIK first
        cik = self.get_cik_from_ticker(ticker)
        if not cik:
            return {"error": f"Could not find CIK for ticker {ticker}"}
        
        # Remove leading zeros for URL construction
        cik_no_zeros = cik.lstrip('0')
        
        # Get the index page for this filing type
        url = f"{self.base_url}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}&count={count}"
        logging.info(f"Finding {filing_type} filings for {ticker} (CIK: {cik})")
        
        try:
            response = self._make_request(url)
            if response.status_code != 200:
                logging.error(f"Failed to get filing index for {ticker}. Status code: {response.status_code}")
                return {"error": f"Failed to get filing index. Status code: {response.status_code}"}
            
            # Parse the index page to find filings
            filing_links = re.findall(r'href="(/Archives/edgar/data/[^"]+)"', response.text)
            documents_links = [link for link in filing_links if 'index.htm' in link]
            
            if not documents_links:
                logging.error(f"No {filing_type} filings found for {ticker}")
                return {"error": f"No {filing_type} filings found"}
            
            # Process each filing
            filings_data = []
            for idx, doc_link in enumerate(documents_links[:count]):
                try:
                    # Get the document page URL
                    doc_url = f"{self.base_url}{doc_link}"
                    
                    # Get the accession number from URL - handle different formats
                    # Try the common format first: 0000950170-24-087843
                    accession_match = re.search(r'/(\d+-\d+-\d+)/', doc_url)
                    
                    # If that fails, try extracting from the directory name (e.g., 000095017024087843)
                    if not accession_match:
                        # This regex extracts the numeric part after /data/CIK/
                        dir_match = re.search(r'/data/\d+/(\d+)/', doc_url)
                        if dir_match:
                            # Format it as an accession number
                            acc_num = dir_match.group(1)
                            # Special handling for directory format
                            logging.info(f"Found directory-style accession number: {acc_num}")
                            accession_match = re.search(r'(\d+)', acc_num)
                    
                    # If both methods fail
                    if not accession_match:
                        logging.warning(f"Could not extract accession number from URL: {doc_url}")
                        # Instead of skipping, create a synthetic accession number from the URL
                        # This allows us to proceed even without a proper accession number
                        synthetic_acc = f"synthetic_{int(time.time())}"
                        logging.info(f"Using synthetic accession number: {synthetic_acc}")
                        accession_number = synthetic_acc
                        accession_number_no_dash = synthetic_acc
                        # Continue processing with synthetic number
                    else:
                        accession_number = accession_match.group(1)
                        accession_number_no_dash = accession_number.replace('-', '')
                    
                    # Create temp directory for this filing
                    filing_dir = Path(tempfile.mkdtemp(prefix=f"edgar_{ticker}_{filing_type}_{idx}_"))
                    
                    # Get the index page
                    index_response = self._make_request(doc_url)
                    if index_response.status_code != 200:
                        logging.warning(f"Failed to get document page for {accession_number}. Status code: {index_response.status_code}")
                        continue
                    
                    # Save the index page
                    index_file_path = os.path.join(filing_dir, "index.htm")
                    with open(index_file_path, 'w', encoding='utf-8') as f:
                        f.write(index_response.text)
                    
                    # Find XBRL and HTML document links
                    xbrl_links = re.findall(r'href="([^"]+\.xml)"', index_response.text)
                    html_links = re.findall(r'href="([^"]+\.htm)"', index_response.text)
                    
                    # Extract document URLs
                    xbrl_url = None
                    html_url = None
                    
                    # Find main HTML document - usually the largest
                    main_doc_match = re.search(r'<td scope="row">(\d+)</td>\s*<td scope="row">([^<]+)</td>\s*<td scope="row"><a[^>]+href="([^"]+)"', index_response.text)
                    if main_doc_match:
                        main_doc_url = main_doc_match.group(3)
                        if main_doc_url.startswith('/'):
                            html_url = f"{self.base_url}{main_doc_url}"
                        else:
                            html_url = f"{doc_url.rsplit('/', 1)[0]}/{main_doc_url}"
                    
                    # Find XBRL file - look for patterns indicating an XBRL instance document
                    for link in xbrl_links:
                        if (link.endswith('_htm.xml') or 
                            re.search(r'\-\d{8}\.xml$', link) or 
                            f"{ticker.lower()}.xml" in link.lower()):
                            if link.startswith('/'):
                                xbrl_url = f"{self.base_url}{link}"
                            else:
                                xbrl_url = f"{doc_url.rsplit('/', 1)[0]}/{link}"
                            break
                    
                    # If we couldn't find a specific XBRL file, use the first one
                    if not xbrl_url and xbrl_links:
                        link = xbrl_links[0]
                        if link.startswith('/'):
                            xbrl_url = f"{self.base_url}{link}"
                        else:
                            xbrl_url = f"{doc_url.rsplit('/', 1)[0]}/{link}"
                    
                    # If we couldn't find a specific HTML file, use the first one
                    if not html_url and html_links:
                        for link in html_links:
                            # Skip the index file
                            if 'index' in link.lower():
                                continue
                            if link.startswith('/'):
                                html_url = f"{self.base_url}{link}"
                            else:
                                html_url = f"{doc_url.rsplit('/', 1)[0]}/{link}"
                            break
                    
                    # Download HTML document if found
                    html_file_path = None
                    if html_url:
                        try:
                            html_response = self._make_request(html_url)
                            if html_response.status_code == 200:
                                html_filename = html_url.split('/')[-1]
                                html_file_path = os.path.join(filing_dir, html_filename)
                                with open(html_file_path, 'w', encoding='utf-8') as f:
                                    f.write(html_response.text)
                        except Exception as e:
                            logging.warning(f"Error downloading HTML document: {str(e)}")
                    
                    # Download XBRL document if found
                    xbrl_file_path = None
                    if xbrl_url:
                        try:
                            xbrl_response = self._make_request(xbrl_url)
                            if xbrl_response.status_code == 200:
                                xbrl_filename = xbrl_url.split('/')[-1]
                                xbrl_file_path = os.path.join(filing_dir, xbrl_filename)
                                with open(xbrl_file_path, 'w', encoding='utf-8') as f:
                                    f.write(xbrl_response.text)
                        except Exception as e:
                            logging.warning(f"Error downloading XBRL document: {str(e)}")
                    
                    # Extract filing date and period end date
                    filing_date = None
                    period_end_date = None
                    
                    filing_date_match = re.search(r'Filing Date</div>\s*<div[^>]*>([^<]+)', index_response.text)
                    if filing_date_match:
                        filing_date = filing_date_match.group(1).strip()
                    
                    period_match = re.search(r'Period of Report</div>\s*<div[^>]*>([^<]+)', index_response.text)
                    if period_match:
                        period_end_date = period_match.group(1).strip()
                    
                    # Create filing data object
                    filing_data = {
                        "accession_number": accession_number,
                        "base_dir": str(filing_dir),
                        "index_url": doc_url,
                        "index_file": index_file_path,
                        "html_url": html_url,
                        "html_file": html_file_path,
                        "xbrl_url": xbrl_url,
                        "xbrl_file": xbrl_file_path,
                        "filing_date": filing_date,
                        "period_end_date": period_end_date
                    }
                    
                    filings_data.append(filing_data)
                    logging.info(f"Successfully downloaded filing {accession_number} for {ticker}")
                    
                except Exception as e:
                    logging.error(f"Error processing filing: {str(e)}")
                    continue
            
            if not filings_data:
                return {"error": "Failed to download any filings"}
            
            return {
                "ticker": ticker,
                "filing_type": filing_type,
                "cik": cik,
                "filings": filings_data
            }
            
        except Exception as e:
            logging.error(f"Error finding filings: {str(e)}")
            return {"error": str(e)}
    
    def download_filing(self, ticker, filing_type, count=1):
        """
        Download SEC filings for a ticker
        
        Args:
            ticker: Company ticker symbol
            filing_type: Filing type (10-K or 10-Q)
            count: Number of filings to download
            
        Returns:
            Dict with filing information and download paths
        """
        return self.find_filings_by_type(ticker, filing_type, count)
    
    def _make_request(self, url, retries=3, backoff_factor=1.0):
        """
        Make a request to SEC EDGAR with retry logic
        
        Args:
            url: URL to request
            retries: Number of retries
            backoff_factor: Backoff factor between retries
            
        Returns:
            Response object
        """
        for retry in range(retries):
            try:
                time.sleep(0.1 * (retry + 1))  # Rate limiting with backoff
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 429:
                    wait_time = backoff_factor * (2 ** retry)
                    logging.warning(f"Rate limited by SEC. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                
                return response
            except Exception as e:
                logging.error(f"Request error: {str(e)}")
                if retry < retries - 1:
                    wait_time = backoff_factor * (2 ** retry)
                    logging.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
        
        raise Exception(f"Failed to get response after {retries} retries")

# Helper function to test direct downloader
def test_direct_download(user_agent, ticker="MSFT", filing_type="10-K"):
    """
    Test the direct downloader
    
    Args:
        user_agent: SEC user agent 
        ticker: Ticker to test
        filing_type: Filing type to test
        
    Returns:
        Result of the download test
    """
    try:
        downloader = DirectEdgarDownloader(user_agent)
        result = downloader.download_filing(ticker, filing_type, count=1)
        
        if "error" in result:
            return {
                "success": False,
                "error": result["error"]
            }
        
        return {
            "success": True,
            "filings_found": len(result["filings"]),
            "filings": result["filings"]
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == "__main__":
    import argparse
    
    # Configure logging for direct execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("direct_edgar.log"),
            logging.StreamHandler()
        ]
    )
    
    parser = argparse.ArgumentParser(description="Direct SEC EDGAR Downloader")
    parser.add_argument("ticker", help="Company ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], default="10-K", help="Filing type (10-K or 10-Q)")
    parser.add_argument("--count", type=int, default=1, help="Number of filings to download")
    parser.add_argument("--user-agent", default="Exascale Capital info@exascale.capital", help="SEC EDGAR user agent")
    parser.add_argument("--output", help="Output file for results (JSON format)")
    
    args = parser.parse_args()
    
    # Run the downloader
    print(f"\n⏳ Starting direct SEC EDGAR download for {args.ticker} {args.filing_type}...")
    
    downloader = DirectEdgarDownloader(args.user_agent)
    result = downloader.download_filing(args.ticker, args.filing_type, args.count)
    
    # Print formatted results summary
    if "error" in result:
        print("\n❌ Download failed:")
        print(f"Error: {result['error']}")
    else:
        filings = result.get("filings", [])
        print(f"\n✅ Download successful. Found {len(filings)} filing(s)")
        
        # Print details of each filing
        for i, filing in enumerate(filings):
            print(f"\nFiling #{i+1}:")
            print(f"  Accession: {filing.get('accession_number', 'unknown')}")
            if "filing_date" in filing:
                print(f"  Filing Date: {filing.get('filing_date')}")
            if "period_end_date" in filing:
                print(f"  Period End Date: {filing.get('period_end_date')}")
            
            # Print base directory
            print(f"  Directory: {filing.get('base_dir', 'unknown')}")
            
            # Count files
            html_files = filing.get("html_files", [])
            xml_files = filing.get("xml_files", [])
            txt_files = filing.get("txt_files", [])
            
            html_count = len(html_files)
            xml_count = len(xml_files)
            txt_count = len(txt_files)
            
            print(f"  Files: {html_count} HTML, {xml_count} XML, {txt_count} TXT")
            
            # Primary documents
            if "html_file" in filing:
                print(f"  Primary HTML: {os.path.basename(filing['html_file'])}")
            if "xbrl_file" in filing:
                print(f"  XBRL Document: {os.path.basename(filing['xbrl_file'])}")
    
    # Save results to JSON file if requested
    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"\nResults saved to {args.output}")
        except Exception as e:
            print(f"\nError saving results: {str(e)}")