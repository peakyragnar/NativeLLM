# src/edgar/edgar_utils.py
import requests
import re
import time
import os
import logging
from bs4 import BeautifulSoup
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sec_api.log"),
        logging.StreamHandler()
    ]
)

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import SEC_BASE_URL, USER_AGENT, COMPANY_NAME, COMPANY_EMAIL

def get_cik_from_ticker(ticker):
    """Convert ticker symbol to CIK number"""
    logging.info(f"Looking up CIK for ticker: {ticker}")
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany"
    
    response = sec_request(url)  # Using enhanced sec_request with retries
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

def get_company_name_from_cik(cik):
    """Get company name from CIK"""
    headers = {'User-Agent': USER_AGENT}
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?CIK={cik}&owner=exclude&action=getcompany"
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None
    
    # Parse company name from response
    soup = BeautifulSoup(response.text, 'html.parser')
    company_info = soup.select_one('.companyInfo')
    if not company_info:
        return None
    
    company_name = company_info.select_one('.companyName')
    if not company_name:
        return None
    
    return company_name.text.strip()

# SEC has rate limits, so add a delay between requests
def sec_request(url, max_retries=3):
    """Make a request to SEC with appropriate rate limiting and retry logic"""
    headers = {'User-Agent': USER_AGENT}
    logging.info(f"Making SEC request to: {url}")
    logging.info(f"Using User-Agent: {USER_AGENT}")
    
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