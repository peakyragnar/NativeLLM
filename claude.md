# LLM-Native Financial Data Project

This document outlines a step-by-step plan to build a system that extracts financial data from SEC XBRL filings and converts it to an LLM-optimized format without imposing traditional financial frameworks or biases.

## Project Overview

The goal is to create a pipeline that:
1. Retrieves raw XBRL data directly from SEC EDGAR
2. Preserves all data exactly as reported by companies
3. Converts it to a format optimized for LLM consumption
4. Avoids imposing traditional financial frameworks or interpretations

## Phase 1: Environment Setup (1-2 days)

### Step 1: Set Up Development Environment
```bash
# Create project directory
mkdir llm-financial-data
cd llm-financial-data

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Create project structure
mkdir -p src/edgar src/xbrl src/formatter data/raw data/processed
```

### Step 2: Install Required Dependencies
```bash
# Install required packages
pip install requests lxml beautifulsoup4 pandas

# Create requirements.txt
pip freeze > requirements.txt
```

### Step 3: Create Configuration File
Create a file named `config.py` in the src directory:

```python
# src/config.py

# SEC EDGAR settings
SEC_BASE_URL = "https://www.sec.gov"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
USER_AGENT = "Your Company Name user@example.com"  # Replace with your information

# Output settings
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"

# Initial companies to process
INITIAL_COMPANIES = [
    {"ticker": "AAPL", "name": "Apple Inc."},
    {"ticker": "MSFT", "name": "Microsoft Corporation"},
    {"ticker": "GOOGL", "name": "Alphabet Inc."},
    {"ticker": "AMZN", "name": "Amazon.com, Inc."},
    {"ticker": "META", "name": "Meta Platforms, Inc."}
]

# Filing types to process
FILING_TYPES = ["10-K", "10-Q"]
```

## Phase 2: SEC EDGAR Access Module (2-3 days)

### Step 1: Implement CIK Lookup
Create a file named `edgar_utils.py` in the src/edgar directory:

```python
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
```

### Step 2: Implement Filing Finder
Create a file named `filing_finder.py` in the src/edgar directory:

```python
# src/edgar/filing_finder.py
import os
import sys
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
```

## Phase 3: XBRL Processing Module (2-3 days)

### Step 1: Implement XBRL Downloader
Create a file named `xbrl_downloader.py` in the src/xbrl directory:

```python
# src/xbrl/xbrl_downloader.py
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.edgar.edgar_utils import sec_request
from src.config import RAW_DATA_DIR

def download_xbrl_instance(filing_metadata):
    """Download XBRL instance document for a filing"""
    # Create directory path
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    accession_number = filing_metadata.get("accession_number", "unknown")
    
    # Create directory structure
    dir_path = os.path.join(RAW_DATA_DIR, ticker, filing_type)
    os.makedirs(dir_path, exist_ok=True)
    
    # Download the instance document
    instance_url = filing_metadata.get("instance_url")
    if not instance_url:
        return {"error": "No instance URL provided"}
    
    try:
        response = sec_request(instance_url)
        if response.status_code != 200:
            return {"error": f"Failed to download instance document: {response.status_code}"}
        
        # Save the file
        file_path = os.path.join(dir_path, f"{accession_number}_instance.xml")
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        return {
            "success": True,
            "file_path": file_path,
            "size": len(response.content)
        }
    except Exception as e:
        return {"error": f"Exception downloading instance document: {str(e)}"}
```

### Step 2: Implement XBRL Parser
Create a file named `xbrl_parser.py` in the src/xbrl directory:

```python
# src/xbrl/xbrl_parser.py
import os
import sys
from lxml import etree

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def parse_xbrl_file(file_path):
    """Parse an XBRL instance document"""
    try:
        # Parse XML
        parser = etree.XMLParser(recover=True)  # Recover from bad XML
        tree = etree.parse(file_path, parser)
        root = tree.getroot()
        
        # Get all namespaces
        namespaces = {k: v for k, v in root.nsmap.items() if k is not None}
        
        # Extract contexts
        contexts = {}
        for context in tree.xpath("//*[local-name()='context']"):
            context_id = context.get("id")
            
            # Extract period
            period = context.find(".//*[local-name()='period']")
            period_info = {}
            if period is not None:
                instant = period.find(".//*[local-name()='instant']")
                if instant is not None:
                    period_info["instant"] = instant.text
                
                start_date = period.find(".//*[local-name()='startDate']")
                end_date = period.find(".//*[local-name()='endDate']")
                if start_date is not None and end_date is not None:
                    period_info["startDate"] = start_date.text
                    period_info["endDate"] = end_date.text
            
            # Extract dimensions (segments and scenarios)
            dimensions = {}
            segment = context.find(".//*[local-name()='segment']")
            if segment is not None:
                for dim in segment.xpath(".//*[local-name()='explicitMember']"):
                    dimension = dim.get("dimension").split(":")[-1]
                    value = dim.text
                    dimensions[dimension] = value
            
            # Store context
            contexts[context_id] = {
                "id": context_id,
                "period": period_info,
                "dimensions": dimensions,
                "xml": etree.tostring(context).decode('utf-8')
            }
        
        # Extract units
        units = {}
        for unit in tree.xpath("//*[local-name()='unit']"):
            unit_id = unit.get("id")
            measure = unit.find(".//*[local-name()='measure']")
            
            if measure is not None:
                units[unit_id] = measure.text
        
        # Extract facts
        facts = []
        for element in tree.xpath("//*"):
            context_ref = element.get("contextRef")
            if context_ref is not None:  # This is a fact
                # Extract namespace and tag name
                tag = element.tag
                if "}" in tag:
                    namespace = tag.split("}")[0].strip("{")
                    tag_name = tag.split("}")[1]
                else:
                    namespace = None
                    tag_name = tag
                
                # Create fact object
                fact = {
                    "concept": tag_name,
                    "namespace": namespace,
                    "value": element.text.strip() if element.text else "",
                    "context_ref": context_ref,
                    "unit_ref": element.get("unitRef"),
                    "decimals": element.get("decimals"),
                    "xml": etree.tostring(element).decode('utf-8')
                }
                
                facts.append(fact)
        
        return {
            "success": True,
            "contexts": contexts,
            "units": units,
            "facts": facts
        }
    except Exception as e:
        return {"error": f"Error parsing XBRL: {str(e)}"}
```

## Phase 4: LLM Format Generator (1-2 days)

### Step 1: Implement LLM Format Generator
Create a file named `llm_formatter.py` in the src/formatter directory:

```python
# src/formatter/llm_formatter.py
import os
import sys
import json

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import PROCESSED_DATA_DIR

def generate_llm_format(parsed_xbrl, filing_metadata):
    """Generate LLM-native format from parsed XBRL"""
    output = []
    
    # Add document metadata
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    company_name = filing_metadata.get("company_name", "unknown")
    cik = filing_metadata.get("cik", "unknown")
    filing_date = filing_metadata.get("filing_date", "unknown")
    period_end = filing_metadata.get("period_end_date", "unknown")
    
    output.append(f"@DOCUMENT: {ticker}-{filing_type}-{period_end}")
    output.append(f"@FILING_DATE: {filing_date}")
    output.append(f"@COMPANY: {company_name}")
    output.append(f"@CIK: {cik}")
    output.append("")
    
    # Add context definitions
    output.append("@CONTEXTS")
    for context_id, context in parsed_xbrl.get("contexts", {}).items():
        period_info = context.get("period", {})
        if "startDate" in period_info and "endDate" in period_info:
            output.append(f"@CONTEXT_DEF: {context_id} | Period: {period_info['startDate']} to {period_info['endDate']}")
        elif "instant" in period_info:
            output.append(f"@CONTEXT_DEF: {context_id} | Instant: {period_info['instant']}")
    output.append("")
    
    # Add units
    output.append("@UNITS")
    for unit_id, unit_value in parsed_xbrl.get("units", {}).items():
        output.append(f"@UNIT_DEF: {unit_id} | {unit_value}")
    output.append("")
    
    # Add all facts
    sorted_facts = sorted(parsed_xbrl.get("facts", []), key=lambda x: x.get("concept", ""))
    for fact in sorted_facts:
        output.append(f"@CONCEPT: {fact.get('concept', '')}")
        output.append(f"@VALUE: {fact.get('value', '')}")
        if fact.get("unit_ref"):
            output.append(f"@UNIT_REF: {fact.get('unit_ref', '')}")
        if fact.get("decimals"):
            output.append(f"@DECIMALS: {fact.get('decimals', '')}")
        output.append(f"@CONTEXT_REF: {fact.get('context_ref', '')}")
        output.append("")
    
    return "\n".join(output)

def save_llm_format(llm_content, filing_metadata):
    """Save LLM format to a file"""
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    period_end = filing_metadata.get("period_end_date", "unknown").replace("-", "")
    
    # Create directory
    dir_path = os.path.join(PROCESSED_DATA_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    
    # Create filename
    filename = f"{ticker}_{filing_type}_{period_end}_llm.txt"
    file_path = os.path.join(dir_path, filename)
    
    # Save file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(llm_content)
    
    return {
        "success": True,
        "file_path": file_path,
        "size": len(llm_content)
    }
```

## Phase 5: Main Processing Pipeline (1-2 days)

### Step 1: Implement Main Processing Pipeline
Create a file named `process_company.py` in the src directory:

```python
# src/process_company.py
import os
import sys
import json
import time

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.xbrl.xbrl_downloader import download_xbrl_instance
from src.xbrl.xbrl_parser import parse_xbrl_file
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import FILING_TYPES

def process_company(ticker):
    """Process all specified filings for a company"""
    print(f"Processing company: {ticker}")
    
    # Step 1: Find company filings
    filings_result = find_company_filings(ticker, FILING_TYPES)
    if "error" in filings_result:
        return {"error": filings_result["error"]}
    
    print(f"Found {len(filings_result.get('filings', {}))} filings for {ticker}")
    
    results = {
        "ticker": ticker,
        "cik": filings_result.get("cik"),
        "company_name": filings_result.get("company_name"),
        "filings_processed": []
    }
    
    # Step 2: Process each filing
    for filing_type, filing_metadata in filings_result.get("filings", {}).items():
        print(f"Processing {ticker} {filing_type}")
        
        # Add ticker and company name to metadata
        filing_metadata["ticker"] = ticker
        filing_metadata["company_name"] = filings_result.get("company_name")
        
        # Download XBRL instance
        download_result = download_xbrl_instance(filing_metadata)
        if "error" in download_result:
            print(f"Error downloading XBRL for {ticker} {filing_type}: {download_result['error']}")
            continue
        
        file_path = download_result.get("file_path")
        
        # Parse XBRL
        parsed_result = parse_xbrl_file(file_path)
        if "error" in parsed_result:
            print(f"Error parsing XBRL for {ticker} {filing_type}: {parsed_result['error']}")
            continue
        
        # Generate LLM format
        llm_content = generate_llm_format(parsed_result, filing_metadata)
        
        # Save LLM format
        save_result = save_llm_format(llm_content, filing_metadata)
        if "error" in save_result:
            print(f"Error saving LLM format for {ticker} {filing_type}: {save_result['error']}")
            continue
        
        results["filings_processed"].append({
            "filing_type": filing_type,
            "filing_date": filing_metadata.get("filing_date"),
            "period_end_date": filing_metadata.get("period_end_date"),
            "llm_file_path": save_result.get("file_path")
        })
        
        print(f"Successfully processed {ticker} {filing_type}")
        
        # Rate limiting
        time.sleep(1)
    
    return results
```

### Step 2: Implement Batch Processing Script
Create a file named `process_companies.py` in the src directory:

```python
# src/process_companies.py
import os
import sys
import json
import time
import argparse

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.process_company import process_company
from src.config import INITIAL_COMPANIES, PROCESSED_DATA_DIR

def process_companies(tickers=None):
    """Process a list of companies"""
    if tickers is None:
        tickers = [company["ticker"] for company in INITIAL_COMPANIES]
    
    results = []
    errors = []
    
    for ticker in tickers:
        try:
            result = process_company(ticker)
            results.append(result)
            if "error" in result:
                errors.append({"ticker": ticker, "error": result["error"]})
        except Exception as e:
            print(f"Exception processing {ticker}: {str(e)}")
            errors.append({"ticker": ticker, "error": str(e)})
        
        # Add delay between companies to respect SEC rate limits
        time.sleep(2)
    
    # Save summary report
    report = {
        "companies_processed": len(results),
        "successful_companies": len(results) - len(errors),
        "errors": errors,
        "results": results
    }
    
    # Create report directory
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    report_path = os.path.join(PROCESSED_DATA_DIR, "processing_report.json")
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Processing complete: {len(results) - len(errors)}/{len(results)} companies successful")
    print(f"Report saved to {report_path}")
    
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process SEC filings for companies")
    parser.add_argument('--tickers', nargs='+', help='List of tickers to process')
    
    args = parser.parse_args()
    
    if args.tickers:
        process_companies(args.tickers)
    else:
        process_companies()
```

## Phase 6: Testing and Validation (2-3 days)

### Step 1: Create Test Script
Create a file named `test_pipeline.py` in the project root:

```python
# test_pipeline.py
from src.process_company import process_company

def test_single_company():
    """Test processing a single company"""
    ticker = "AAPL"  # Use a well-known company for testing
    result = process_company(ticker)
    
    print("Test result:")
    print(f"Ticker: {result.get('ticker')}")
    print(f"CIK: {result.get('cik')}")
    print(f"Company Name: {result.get('company_name')}")
    print(f"Filings Processed: {len(result.get('filings_processed', []))}")
    
    for filing in result.get('filings_processed', []):
        print(f"  - {filing.get('filing_type')} ({filing.get('filing_date')})")
        
        # Display a sample of the LLM format
        llm_file_path = filing.get('llm_file_path')
        if llm_file_path:
            with open(llm_file_path, 'r', encoding='utf-8') as f:
                content = f.read(1000)  # Read first 1000 characters for preview
            
            print("\nLLM Format Sample:")
            print(content)
            print("...")

if __name__ == "__main__":
    test_single_company()
```

### Step 2: Test LLM Integration
Create a file named `test_llm_integration.py` in the project root:

```python
# test_llm_integration.py
import os
import sys
import glob
import argparse

def read_llm_file(file_path):
    """Read an LLM format file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def display_llm_file_info(file_path):
    """Display information about an LLM format file"""
    content = read_llm_file(file_path)
    
    # Extract basic metadata
    lines = content.split('\n')
    metadata = {}
    
    for line in lines[:20]:  # Look in first 20 lines for metadata
        if line.startswith('@DOCUMENT:'):
            metadata['document'] = line.replace('@DOCUMENT:', '').strip()
        elif line.startswith('@FILING_DATE:'):
            metadata['filing_date'] = line.replace('@FILING_DATE:', '').strip()
        elif line.startswith('@COMPANY:'):
            metadata['company'] = line.replace('@COMPANY:', '').strip()
        elif line.startswith('@CIK:'):
            metadata['cik'] = line.replace('@CIK:', '').strip()
    
    # Count facts
    fact_count = content.count('@CONCEPT:')
    
    print(f"File: {os.path.basename(file_path)}")
    print(f"Size: {len(content):,} bytes")
    print(f"Document: {metadata.get('document', 'Unknown')}")
    print(f"Company: {metadata.get('company', 'Unknown')}")
    print(f"Filing Date: {metadata.get('filing_date', 'Unknown')}")
    print(f"Facts: {fact_count:,}")
    print()

def list_available_files():
    """List all available LLM format files"""
    from src.config import PROCESSED_DATA_DIR
    
    pattern = os.path.join(PROCESSED_DATA_DIR, "**", "*_llm.txt")
    files = glob.glob(pattern, recursive=True)
    
    if not files:
        print("No LLM format files found.")
        return []
    
    print(f"Found {len(files)} LLM format files:")
    for i, file_path in enumerate(files, 1):
        print(f"{i}. {os.path.basename(file_path)}")
    
    return files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test LLM format files")
    parser.add_argument('--file', help='Path to a specific LLM format file to examine')
    parser.add_argument('--list', action='store_true', help='List all available LLM format files')
    
    args = parser.parse_args()
    
    if args.list:
        list_available_files()
    elif args.file:
        display_llm_file_info(args.file)
    else:
        files = list_available_files()
        if files and len(files) > 0:
            display_llm_file_info(files[0])  # Display the first file
```

## Phase 7: Scaling and Optimization (3-4 days)

### Step 1: Create a Company List
Create a file named `company_list.py` in the src directory:

```python
# src/company_list.py
import csv
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def load_sp500_companies():
    """Load S&P 500 companies from a CSV file"""
    # This is a simplified version - in practice, you might download
    # this list from a financial data provider or Wikipedia
    
    companies = [
        {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Information Technology"},
        {"ticker": "MSFT", "name": "Microsoft Corporation", "sector": "Information Technology"},
        {"ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
        {"ticker": "GOOGL", "name": "Alphabet Inc.", "sector": "Communication Services"},
        {"ticker": "META", "name": "Meta Platforms Inc.", "sector": "Communication Services"},
        {"ticker": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Discretionary"},
        {"ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Information Technology"},
        {"ticker": "BRK.B", "name": "Berkshire Hathaway Inc.", "sector": "Financials"},
        {"ticker": "UNH", "name": "UnitedHealth Group Inc.", "sector": "Health Care"},
        {"ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Health Care"},
        # Add more companies as needed
    ]
    
    return companies

def get_companies_by_sector(sector=None):
    """Get companies by sector"""
    companies = load_sp500_companies()
    
    if sector:
        return [c for c in companies if c["sector"] == sector]
    
    return companies

def get_top_companies(count=10):
    """Get top N companies"""
    companies = load_sp500_companies()
    return companies[:count]
```

### Step 2: Add Rate Limiting and Error Handling
Update the `edgar_utils.py` file to add better rate limiting:

```python
# Add to src/edgar/edgar_utils.py

# Add a better rate limiting function
def sec_request_with_retry(url, max_retries=3, backoff_factor=1.0):
    """Make a request to SEC with retry and exponential backoff"""
    headers = {'User-Agent': USER_AGENT}
    
    for retry in range(max_retries):
        try:
            time.sleep(0.1 + backoff_factor * retry)  # Rate limiting with backoff
            response = requests.get(url, headers=headers)
            
            # If rate limited, wait and retry
            if response.status_code == 429:
                wait_time = 10 * (retry + 1)  # Increasing wait time
                print(f"Rate limited by SEC. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            
            return response
        except Exception as e:
            print(f"Error on attempt {retry+1}: {str(e)}")
            if retry == max_retries - 1:
                raise
            time.sleep(5 * (retry + 1))  # Wait before retrying
    
    return None
```

### Step 3: Create a Parallel Processing Script
Create a file named `parallel_processor.py` in the src directory:

```python
# src/parallel_processor.py
import os
import sys
import json
import time
import argparse
import concurrent.futures
from tqdm import tqdm

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.process_company import process_company
from src.company_list import get_top_companies, get_companies_by_sector
from src.config import PROCESSED_DATA_DIR

def process_company_safe(ticker):
    """Process a company with error handling"""
    try:
        return process_company(ticker)
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}

def process_companies_parallel(tickers, max_workers=3):
    """Process companies in parallel with limited concurrency"""
    results = []
    errors = []
    
    # Use ThreadPoolExecutor to limit concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and track them with tqdm for progress
        future_to_ticker = {executor.submit(process_company_safe, ticker): ticker for ticker in tickers}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(tickers), desc="Processing companies"):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                results.append(result)
                if "error" in result:
                    errors.append({"ticker": ticker, "error": result["error"]})
            except Exception as e:
                print(f"Exception processing {ticker}: {str(e)}")
                errors.append({"ticker": ticker, "error": str(e)})
    
    # Save summary report
    report = {
        "companies_processed": len(results),
        "successful_companies": len(results) - len(errors),
        "errors": errors,
        "results": results
    }
    
    # Create report directory
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(PROCESSED_DATA_DIR, f"processing_report_{timestamp}.json")
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Processing complete: {len(results) - len(errors)}/{len(results)} companies successful")
    print(f"Report saved to {report_path}")
    
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process SEC filings for companies in parallel")
    parser.add_argument('--tickers', nargs='+', help='List of tickers to process')
    parser.add_argument('--sector', help='Process companies in a specific sector')
    parser.add_argument('--top', type=int, default=10, help='Process top N companies')
    parser.add_argument('--workers', type=int, default=3, help='Maximum number of concurrent workers')
    
    args = parser.parse_args()
    
    if args.tickers:
        tickers = args.tickers
    elif args.sector:
        tickers = [c["ticker"] for c in get_companies_by_sector(args.sector)]
    else:
        tickers = [c["ticker"] for c in get_top_companies(args.top)]
    
    process_companies_parallel(tickers, args.workers)
```

## Phase 8: Usage with LLMs (1-2 days)

### Step 1: Create an LLM Query Script
Create a file named `query_llm.py` in the project root:

```python
# query_llm.py
import os
import sys
import glob
import argparse

def find_llm_file(ticker, filing_type=None):
    """Find LLM format file for a specific company and filing type"""
    from src.config import PROCESSED_DATA_DIR
    
    if filing_type:
        pattern = os.path.join(PROCESSED_DATA_DIR, ticker, f"{ticker}_{filing_type}_*_llm.txt")
    else:
        pattern = os.path.join(PROCESSED_DATA_DIR, ticker, f"{ticker}_*_llm.txt")
    
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # Sort by modification time (newest first)
    return sorted(files, key=os.path.getmtime, reverse=True)[0]

def read_llm_file(file_path):
    """Read an LLM format file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def prepare_prompt(llm_content, question):
    """Prepare a prompt for the LLM"""
    prompt = f"""Below is financial data from an SEC filing in a special format optimized for AI analysis.
Each data point is presented with its context and original value.

{llm_content[:50000]}  # Limit content length to avoid token limits

Based on the financial data above, please answer the following question:
{question}

Provide your answer based solely on the data provided, with no additional information or assumptions.
"""
    return prompt

def main():
    parser = argparse.ArgumentParser(description="Query LLM with financial data")
    parser.add_argument('--ticker', required=True, help='Company ticker symbol')
    parser.add_argument('--filing_type', help='Filing type (10-K, 10-Q, etc.)')
    parser.add_argument('--question', required=True, help='Question to ask about the financial data')
    
    args = parser.parse_args()
    
    # Find the LLM format file
    file_path = find_llm_file(args.ticker, args.filing_type)
    if not file_path:
        print(f"No LLM format file found for {args.ticker}")
        return
    
    print(f"Using file: {file_path}")
    
    # Read the file
    llm_content = read_llm_file(file_path)
    
    # Prepare the prompt
    prompt = prepare_prompt(llm_content, args.question)
    
    print("\nPrompt prepared. You can now use this prompt with an LLM API of your choice.")
    print(f"Prompt length: {len(prompt)} characters")
    
    # Here you would typically call an LLM API with the prompt
    # For this example, we'll just provide instructions
    print("\nTo use with Claude or GPT-4, copy the prompt and send it to the API.")
    
    # Optionally save the prompt to a file
    prompt_file = f"{args.ticker}_prompt.txt"
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    print(f"Prompt saved to {prompt_file}")

if __name__ == "__main__":
    main()
```

## Phase 9: Running the Complete Pipeline

### Step 1: Create a Main Runner Script
Create a file named `run_pipeline.py` in the project root:

```python
# run_pipeline.py
import os
import sys
import argparse
import time

def setup_directories():
    """Set up project directories"""
    from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR
    
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    print(f"Set up directories: {RAW_DATA_DIR}, {PROCESSED_DATA_DIR}")

def run_initial_companies():
    """Run the pipeline for initial companies"""
    from src.process_companies import process_companies
    
    print("Processing initial companies...")
    result = process_companies()
    return result

def run_specific_company(ticker):
    """Run the pipeline for a specific company"""
    from src.process_company import process_company
    
    print(f"Processing company: {ticker}")
    result = process_company(ticker)
    return result

def run_parallel_processing(count, workers):
    """Run parallel processing for top N companies"""
    from src.parallel_processor import process_companies_parallel
    from src.company_list import get_top_companies
    
    companies = get_top_companies(count)
    tickers = [c["ticker"] for c in companies]
    
    print(f"Processing top {len(tickers)} companies with {workers} workers...")
    result = process_companies_parallel(tickers, workers)
    return result

def main():
    parser = argparse.ArgumentParser(description="Run the SEC filing to LLM format pipeline")
    parser.add_argument('--setup', action='store_true', help='Set up project directories')
    parser.add_argument('--initial', action='store_true', help='Process initial companies from config')
    parser.add_argument('--company', help='Process a specific company by ticker')
    parser.add_argument('--top', type=int, help='Process top N companies in parallel')
    parser.add_argument('--workers', type=int, default=3, help='Number of parallel workers')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_directories()
    
    if args.initial:
        run_initial_companies()
    
    if args.company:
        run_specific_company(args.company)
    
    if args.top:
        run_parallel_processing(args.top, args.workers)
    
    # If no action specified, show help
    if not (args.setup or args.initial or args.company or args.top):
        parser.print_help()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
```

## Running the Project

1. **Setup environment and directories**:
   ```bash
   python run_pipeline.py --setup
   ```

2. **Process a single company to test**:
   ```bash
   python run_pipeline.py --company AAPL
   ```

3. **Process initial companies from config**:
   ```bash
   python run_pipeline.py --initial
   ```

4. **Process top N companies in parallel**:
   ```bash
   python run_pipeline.py --top 10 --workers 3
   ```

5. **Query a processed filing with an LLM**:
   ```bash
   python query_llm.py --ticker AAPL --filing_type 10-Q --question "What was the revenue for the most recent quarter and how does it compare to the previous quarter?"
   ```

## Best Practices and Tips

1. **SEC Rate Limiting**: 
   - SEC EDGAR has rate limits (about 10 requests per second)
   - Implement proper delays between requests
   - Use exponential backoff for retries

2. **Error Handling**:
   - XBRL documents can vary significantly between companies
   - Implement robust error handling and logging
   - Have fallback mechanisms for parsing issues

3. **Storage Considerations**:
   - XBRL files can be large (several MB each)
   - LLM format files will also be substantial
   - Plan for adequate storage (at least 10GB for 500 companies)

4. **LLM Integration**:
   - Different LLMs have different token limits
   - You may need to truncate content for very large filings
   - Consider chunking strategies for very large documents

5. **Scaling Considerations**:
   - Start with a small set of companies and validate results
   - Gradually increase to more companies
   - Monitor SEC rate limiting and adjust concurrency

## Extending the Project

1. **Add more filing types**: Expand beyond 10-K and 10-Q to include 8-K, DEF 14A, etc.
2. **Implement incremental updates**: Only process new filings since last run
3. **Add data validation**: Implement checks to ensure data quality
4. **Create a web interface**: Build a simple web app to browse and query filings
5. **Implement LLM API integration**: Directly connect to LLM APIs for automated analysis

## Troubleshooting

1. **SEC Access Issues**:
   - Ensure your User-Agent header is properly set
   - Check for rate limiting (HTTP 429 responses)
   - Implement longer delays between requests

2. **XBRL Parsing Errors**:
   - Some XBRL files may have non-standard formatting
   - Use the `recover=True` option in the XML parser
   - Implement special handling for problematic companies

3. **LLM Format Issues**:
   - Very large filings may exceed token limits
   - Implement strategies for truncating or chunking content
   - Consider focusing on the most relevant sections

4. **Performance Issues**:
   - Adjust the number of workers based on your system's capabilities
   - Consider using a cloud server for processing large batches
   - Implement caching for frequently accessed data