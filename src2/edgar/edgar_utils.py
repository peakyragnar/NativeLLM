# src2/edgar/edgar_utils.py
import requests
import re
import time
import os
import logging
from bs4 import BeautifulSoup
import sys

# SEC EDGAR constants
SEC_BASE_URL = "https://www.sec.gov"
DEFAULT_USER_AGENT = "Exascale Capital info@exascale.capital"

def get_cik_from_ticker(ticker, user_agent=None):
    """
    Convert ticker symbol to CIK number
    
    Args:
        ticker: Company ticker symbol
        user_agent: Optional user agent string for SEC EDGAR
        
    Returns:
        CIK number or None if not found
    """
    if user_agent is None:
        user_agent = DEFAULT_USER_AGENT
        
    logging.info(f"Looking up CIK for ticker: {ticker}")
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany"
    
    response = sec_request(url, user_agent)  # Using enhanced sec_request with retries
    if response.status_code != 200:
        logging.error(f"Failed to get CIK for {ticker}. Status code: {response.status_code}")
        with open(f"error_{ticker}_response.html", "w") as f:
            f.write(response.text)
        logging.info(f"Saved error response to error_{ticker}_response.html")
        return None
    
    # Find CIK in the response
    cik_match = re.search(r'CIK=(\d{10})', response.text)
    if not cik_match:
        logging.error(f"CIK pattern not found in response for {ticker}")
        with open(f"error_{ticker}_no_cik.html", "w") as f:
            f.write(response.text)
        logging.info(f"Saved response without CIK to error_{ticker}_no_cik.html")
        return None
    
    cik = cik_match.group(1)
    logging.info(f"Found CIK for {ticker}: {cik}")
    return cik

def get_company_name_from_cik(cik, user_agent=None):
    """
    Get company name from CIK using multiple approaches for maximum reliability.
    
    This function implements a robust, multi-step approach to extract company names:
    1. First checks against a hardcoded list of major companies
    2. Tries multiple HTML parsing approaches for different SEC page formats
    3. Provides fallbacks to ensure we always get a usable company name
    
    Args:
        cik: Company CIK number as string
        user_agent: Optional user agent string for SEC EDGAR
        
    Returns:
        Company name or fallback if name cannot be determined
    """
    if user_agent is None:
        user_agent = DEFAULT_USER_AGENT
        
    logging.info(f"Looking up company name for CIK: {cik}")
    
    # Step 1: Check hardcoded list of major companies for immediate reliable results
    # This avoids unnecessary network requests for common companies
    company_map = {
        "0000320193": "Apple Inc.",
        "0000789019": "Microsoft Corporation",
        "0001652044": "Alphabet Inc.",
        "0001018724": "Amazon.com, Inc.",
        "0001326801": "Meta Platforms, Inc.",
        "0001045810": "NVIDIA Corporation",
        "0000200406": "Coca-Cola Company",
        "0000051143": "JPMorgan Chase & Co.",
        "0000732717": "The Procter & Gamble Company",
        "0000080424": "Johnson & Johnson",
        "0000037996": "The Boeing Company",
        "0000018230": "Caterpillar Inc.",
        "0000072971": "Exxon Mobil Corporation",
        "0001467373": "General Motors Company",
        "0000040545": "General Electric Company",
        "0000066740": "Intel Corporation",
        "0000021344": "Walmart Inc.",
        "0000097476": "Bank of America Corporation",
        "0000101829": "Visa Inc."
    }
    
    # Remove leading zeros for matching (some CIKs might be passed with or without leading zeros)
    cik_normalized = cik.lstrip('0')
    
    # Check if we have a hardcoded mapping for this company
    for mapped_cik, company_name in company_map.items():
        if mapped_cik.lstrip('0') == cik_normalized:
            logging.info(f"Using hardcoded company name for CIK {cik}: {company_name}")
            return company_name
    
    # Step 2: Make a request to SEC EDGAR
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?CIK={cik}&owner=exclude&action=getcompany"
    
    try:
        response = sec_request(url, user_agent)
        if response.status_code != 200:
            logging.error(f"Failed to get company info for CIK {cik}. Status code: {response.status_code}")
            # Don't return immediately, try fallback mechanisms
        else:
            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check for common error indicating no results ("EDGAR | Company Search")
            title_text = soup.find('title').text if soup.find('title') else ""
            if "EDGAR | Company Search" in title_text:
                logging.warning(f"SEC returned search results page instead of company page for CIK {cik}")
                # Continue to more parsing attempts
            
            # Try different selectors for company name (ordered by reliability)
            
            # Method 1: Check for <span class="companyName">
            company_name_span = soup.select_one('span.companyName')
            if company_name_span and company_name_span.text and len(company_name_span.text) > 1:
                name = company_name_span.text.strip()
                # Verify it's not the standard "EDGAR..." text
                if "EDGAR" not in name and "Company Search" not in name:
                    logging.info(f"Found company name from span.companyName for CIK {cik}: {name}")
                    return name
            
            # Method 2: Look for company name in <div class="companyInfo">
            company_info = soup.select_one('.companyInfo')
            if company_info:
                company_name = company_info.select_one('.companyName')
                if company_name and company_name.text and "EDGAR" not in company_name.text:
                    name = company_name.text.strip()
                    logging.info(f"Found company name from companyInfo for CIK {cik}: {name}")
                    return name
            
            # Method 3: Check heading elements (h1, h2)
            for heading in soup.find_all(['h1', 'h2']):
                if heading.text and len(heading.text.strip()) > 3 and "EDGAR" not in heading.text and "Search" not in heading.text:
                    name = heading.text.strip()
                    logging.info(f"Found company name from heading for CIK {cik}: {name}")
                    return name
            
            # Method 4: Look for text close to the CIK mention
            cik_text = soup.find(string=re.compile(r'CIK[: ]*0*' + cik_normalized))
            if cik_text:
                parent = cik_text.parent
                # Look at siblings or parent elements for company name
                if parent:
                    for element in [parent.previous_sibling, parent.parent, parent.parent.previous_sibling]:
                        if element and element.text and len(element.text.strip()) > 3:
                            candidate = element.text.strip()
                            # Check if it looks like a company name (not SEC boilerplate)
                            if "EDGAR" not in candidate and "SEC" not in candidate and "Search" not in candidate:
                                logging.info(f"Found company name near CIK mention for CIK {cik}: {candidate}")
                                return candidate
            
            # Method 5: Extract from page title
            if title_text and "EDGAR" not in title_text.split(' - ')[0]:
                # Title often follows format "Company Name - SEC Filings"
                candidate = title_text.split(' - ')[0].strip()
                if len(candidate) > 3:
                    logging.info(f"Extracted company name from title for CIK {cik}: {candidate}")
                    return candidate
            
            # Method 6: Look for strong/b tags that might contain company name
            for emphasis in soup.find_all(['strong', 'b']):
                if emphasis.text and len(emphasis.text.strip()) > 3 and "EDGAR" not in emphasis.text and "SEC" not in emphasis.text:
                    name = emphasis.text.strip()
                    logging.info(f"Found company name from emphasis for CIK {cik}: {name}")
                    return name
    except Exception as e:
        logging.error(f"Exception when getting company name for CIK {cik}: {str(e)}")
    
    # Step 3: If we can't determine the company name, use fallback mechanisms
    
    # Fallback 1: Use a more generic company name based on the CIK
    fallback_name = f"Company CIK:{cik}"
    logging.warning(f"Using fallback company name for CIK {cik}: {fallback_name}")
    return fallback_name

# SEC has rate limits, so add a delay between requests
def sec_request(url, user_agent=None, max_retries=3):
    """
    Make a request to SEC with appropriate rate limiting and retry logic
    
    Args:
        url: URL to request
        user_agent: User agent string for SEC EDGAR
        max_retries: Maximum number of retry attempts
        
    Returns:
        Response object
    """
    if user_agent is None:
        user_agent = DEFAULT_USER_AGENT
        
    headers = {'User-Agent': user_agent}
    logging.info(f"Making SEC request to: {url}")
    logging.info(f"Using User-Agent: {user_agent}")
    
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

def test_cik_lookup(ticker="MSFT"):
    """Test the CIK lookup function"""
    cik = get_cik_from_ticker(ticker)
    if cik:
        print(f"✅ CIK for {ticker}: {cik}")
        company_name = get_company_name_from_cik(cik)
        print(f"✅ Company name: {company_name}")
        return True
    else:
        print(f"❌ Failed to find CIK for {ticker}")
        return False
        
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SEC EDGAR Utility Functions")
    parser.add_argument("ticker", help="Ticker symbol to lookup")
    parser.add_argument("--user-agent", help="User agent for SEC EDGAR requests")
    
    args = parser.parse_args()
    
    # Configure logging for direct execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("edgar_utils.log"),
            logging.StreamHandler()
        ]
    )
    
    print(f"Looking up CIK for {args.ticker}...")
    cik = get_cik_from_ticker(args.ticker, args.user_agent)
    
    if cik:
        print(f"\nCIK for {args.ticker}: {cik}")
        print(f"Looking up company name...")
        company_name = get_company_name_from_cik(cik, args.user_agent)
        print(f"Company name: {company_name}")
    else:
        print(f"\nFailed to find CIK for {args.ticker}")