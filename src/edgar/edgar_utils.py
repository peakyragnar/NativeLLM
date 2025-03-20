# src/edgar/edgar_utils.py
import requests
import re
import time
import os
from bs4 import BeautifulSoup
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import SEC_BASE_URL, USER_AGENT

def get_cik_from_ticker(ticker):
    """Convert ticker symbol to CIK number"""
    headers = {'User-Agent': USER_AGENT}
    url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?CIK={ticker}&owner=exclude&action=getcompany"
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None
    
    # Find CIK in the response
    cik_match = re.search(r'CIK=(\d{10})', response.text)
    if not cik_match:
        return None
    
    return cik_match.group(1)

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
def sec_request(url):
    """Make a request to SEC with appropriate rate limiting"""
    headers = {'User-Agent': USER_AGENT}
    time.sleep(0.1)  # Rate limiting
    response = requests.get(url, headers=headers)
    return response