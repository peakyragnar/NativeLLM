"""
Specialized test script for extracting Google's iXBRL data from SEC filings.
This script focuses on thoroughly analyzing the document structure to determine
exactly how Google's SEC filings embed financial data.
"""

import os
import sys
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse, parse_qs

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik, sec_request
from src.edgar.filing_finder import find_company_filings
from src.xbrl.ixbrl_downloader import download_sec_primary_document, direct_download_from_sec_viewer

def analyze_google_filing_structure(html_file_path=None, filing_type="10-K", force_download=False):
    """
    Analyze the structure of Google's SEC filing to determine how financial data is embedded.
    
    This function downloads Google's filing, analyzes its structure in detail, and reports
    on how the financial data is embedded in the document.
    """
    # Step 1: Find Google's filing
    ticker = "GOOGL"
    filing_result = find_company_filings(ticker, [filing_type])
    
    if "error" in filing_result:
        print(f"Error finding Google's filing: {filing_result['error']}")
        return
    
    if filing_type not in filing_result.get("filings", {}):
        print(f"No {filing_type} filing found for Google")
        return
    
    # Step 2: Get filing metadata
    filing_metadata = filing_result["filings"][filing_type]
    filing_metadata["ticker"] = ticker
    filing_metadata["company_name"] = filing_result.get("company_name")
    
    # Step 3: If no existing HTML file, download the document
    if not html_file_path or force_download:
        print(f"Downloading Google's {filing_type} filing...")
        download_result = download_sec_primary_document(filing_metadata)
        
        if "error" in download_result:
            print(f"Error downloading filing: {download_result['error']}")
            return
        
        html_file_path = download_result.get("file_path")
        is_sec_viewer = download_result.get("is_sec_viewer", False)
        
        print(f"Downloaded filing to: {html_file_path}")
        print(f"Is SEC Viewer: {is_sec_viewer}")
    
    # Step 4: Analyze the document structure
    print(f"\nAnalyzing document structure for {html_file_path}...")
    
    with open(html_file_path, 'rb') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Check if this is the SEC's XBRL Viewer
    is_viewer = False
    if soup.title and "XBRL Viewer" in soup.title.text:
        is_viewer = True
        print("This is the SEC XBRL Viewer document")
        
        # Try to extract the actual document URL
        parsed_url = None
        instance_url = filing_metadata.get("instance_url", "")
        if instance_url:
            parsed_url = urlparse(instance_url)
            query_params = parse_qs(parsed_url.query)
            
            if 'doc' in query_params:
                actual_doc_path = query_params['doc'][0]
                print(f"Actual document path: {actual_doc_path}")
                
                # Try to retrieve the actual document
                actual_doc_url = f"https://www.sec.gov{actual_doc_path}" if actual_doc_path.startswith('/') else f"https://www.sec.gov/{actual_doc_path}"
                print(f"Fetching actual document from: {actual_doc_url}")
                
                response = sec_request(actual_doc_url)
                if response and response.status_code == 200:
                    print("Successfully retrieved actual document")
                    # Parse and analyze the actual document
                    actual_soup = BeautifulSoup(response.content, 'html.parser')
                    perform_structure_analysis(actual_soup, "Google's Filing (Actual Document)")
                else:
                    print(f"Failed to retrieve actual document: {response.status_code if response else 'No response'}")
    
    # Perform full structure analysis of the current document
    perform_structure_analysis(soup, "Google's Filing (Current Document)")
    
    # Check for MetaLinks.json if this is the viewer
    if is_viewer and parsed_url:
        base_url = f"https://www.sec.gov/Archives/edgar/data/{filing_metadata.get('cik')}/{filing_metadata.get('accession_number').replace('-', '')}/"
        metalinks_url = f"{base_url}MetaLinks.json"
        print(f"\nFetching MetaLinks.json from: {metalinks_url}")
        
        response = sec_request(metalinks_url)
        if response and response.status_code == 200:
            try:
                metalinks_data = response.json()
                print("Successfully retrieved MetaLinks.json")
                print(f"Contents: {json.dumps(metalinks_data, indent=2)[:500]}...")
                
                # Look for instance documents in the MetaLinks data
                if "instance" in metalinks_data:
                    print("\nInstance documents found in MetaLinks.json:")
                    for instance_name, instance_data in metalinks_data["instance"].items():
                        print(f"  - {instance_name}")
                        if "baseTaxonomies" in instance_data:
                            print(f"    Base taxonomies: {list(instance_data['baseTaxonomies'].keys())[:3]}...")
                
                # Look for reportFiles
                if "reportFiles" in metalinks_data:
                    print("\nReport files found in MetaLinks.json:")
                    for file_name in list(metalinks_data["reportFiles"].keys())[:5]:
                        print(f"  - {file_name}")
                        
                        # If this is an HTML or XBRL file, try to fetch it
                        if file_name.endswith('.htm') or file_name.endswith('.xml'):
                            file_url = f"{base_url}{file_name}"
                            print(f"    Fetching: {file_url}")
                            file_response = sec_request(file_url)
                            if file_response and file_response.status_code == 200:
                                print(f"    Successfully retrieved file ({len(file_response.content)} bytes)")
                                
                                # If HTML file, check for iXBRL content
                                if file_name.endswith('.htm'):
                                    file_soup = BeautifulSoup(file_response.content, 'html.parser')
                                    
                                    # Check for iXBRL namespace
                                    html_tag = file_soup.find('html')
                                    is_ixbrl = False
                                    if html_tag:
                                        for attr_name, attr_value in html_tag.attrs.items():
                                            if 'xmlns:ix' in attr_name or (isinstance(attr_value, str) and 'inline' in attr_value.lower() and 'xbrl' in attr_value.lower()):
                                                is_ixbrl = True
                                                break
                                    
                                    if is_ixbrl:
                                        print(f"    This file contains iXBRL content!")
                                        # Save this file for further analysis
                                        output_path = os.path.join(os.path.dirname(html_file_path), f"direct_{file_name}")
                                        with open(output_path, 'wb') as f:
                                            f.write(file_response.content)
                                        print(f"    Saved to: {output_path}")
                                        
                                        # Perform structure analysis on this file
                                        perform_structure_analysis(file_soup, f"Google's Filing (Direct {file_name})")
                            else:
                                print(f"    Failed to retrieve file: {file_response.status_code if file_response else 'No response'}")
            except Exception as e:
                print(f"Error parsing MetaLinks.json: {str(e)}")
        else:
            print(f"Failed to retrieve MetaLinks.json: {response.status_code if response else 'No response'}")

def perform_structure_analysis(soup, document_title):
    """Perform detailed structure analysis on the document"""
    print(f"\n===== STRUCTURE ANALYSIS: {document_title} =====")
    
    # Document type
    print("\nDocument type:")
    doctype = soup.find('!DOCTYPE')
    print(f"DOCTYPE: {doctype}")
    
    # HTML namespace
    html_tag = soup.find('html')
    print("\nHTML namespaces:")
    if html_tag:
        namespaces = []
        for attr_name, attr_value in html_tag.attrs.items():
            if 'xmlns' in attr_name:
                namespaces.append(f"{attr_name}=\"{attr_value}\"")
        print("\n".join(namespaces) if namespaces else "No xmlns attributes found")
    
    # Check for iXBRL elements
    print("\nChecking for iXBRL elements:")
    ix_elements = soup.find_all(lambda tag: tag.name and ':' in tag.name and tag.name.split(':')[0] == 'ix')
    if ix_elements:
        print(f"Found {len(ix_elements)} iXBRL elements")
        tag_counts = {}
        for elem in ix_elements:
            tag_name = elem.name
            tag_counts[tag_name] = tag_counts.get(tag_name, 0) + 1
        
        print("iXBRL element types:")
        for tag_name, count in tag_counts.items():
            print(f"  - {tag_name}: {count}")
    else:
        print("No iXBRL elements found")
    
    # Check for hidden sections
    print("\nChecking for hidden data sections:")
    hidden_divs = soup.find_all('div', {'style': re.compile('display:\\s*none')})
    if hidden_divs:
        print(f"Found {len(hidden_divs)} hidden divs")
        for i, div in enumerate(hidden_divs[:3]):  # Show only first 3
            print(f"Hidden div {i+1} classes: {div.get('class', [])}")
            # Check if this div contains XBRL elements
            xbrl_in_div = div.find_all(lambda tag: tag.name and ':' in tag.name)
            print(f"  Contains {len(xbrl_in_div)} XBRL elements")
            
            # List first few elements
            for elem in xbrl_in_div[:5]:
                print(f"  - {elem.name}")
    else:
        print("No hidden divs found")
    
    # Check for ix:references
    ix_refs = soup.find_all(['ix:references', 'ix:resources'])
    if ix_refs:
        print(f"\nFound {len(ix_refs)} ix:references/resources sections")
        for i, ref in enumerate(ix_refs[:2]):  # Show only first 2
            print(f"Section {i+1}:")
            # Check for contexts
            contexts = ref.find_all(['context', 'xbrli:context'])
            print(f"  Contains {len(contexts)} contexts")
            
            # Check for units
            units = ref.find_all(['unit', 'xbrli:unit'])
            print(f"  Contains {len(units)} units")
    else:
        print("\nNo ix:references/resources sections found")
    
    # Check for potential fact elements
    print("\nChecking for potential fact elements:")
    potential_facts = 0
    
    # Standard iXBRL facts
    std_ix_facts = soup.find_all(['ix:nonfraction', 'ix:nonnumeric'])
    potential_facts += len(std_ix_facts)
    print(f"Standard iXBRL facts (ix:nonfraction, ix:nonnumeric): {len(std_ix_facts)}")
    
    # Elements with ix:* attributes
    ix_attr_elements = soup.find_all(lambda tag: tag.attrs and any(attr.startswith('ix:') for attr in tag.attrs))
    potential_facts += len(ix_attr_elements)
    print(f"Elements with ix:* attributes: {len(ix_attr_elements)}")
    
    # Elements with data-xbrl attributes
    data_xbrl_elements = soup.find_all(lambda tag: tag.attrs and any(attr.startswith('data-xbrl') for attr in tag.attrs))
    potential_facts += len(data_xbrl_elements)
    print(f"Elements with data-xbrl* attributes: {len(data_xbrl_elements)}")
    
    print(f"Total potential fact elements: {potential_facts}")
    
    # Check for JavaScript data
    print("\nChecking for JavaScript with XBRL data:")
    script_tags = soup.find_all('script')
    xbrl_scripts = []
    
    for script in script_tags:
        if script.string and any(term in script.string for term in ['ixData', 'xbrlValues', 'ix.data', 'xbrl:data']):
            xbrl_scripts.append(script)
    
    if xbrl_scripts:
        print(f"Found {len(xbrl_scripts)} scripts potentially containing XBRL data")
        for i, script in enumerate(xbrl_scripts[:2]):  # Show only first 2
            print(f"Script {i+1}:")
            # Try to extract JSON data
            json_match = re.search(r'var\s+\w+\s*=\s*(\{.+?\});\s*', script.string, re.DOTALL)
            if json_match:
                print("  Contains JSON data assignment")
            else:
                print("  No obvious JSON assignment found")
            
            # Look for facts references
            if script.string and 'fact' in script.string.lower():
                print("  Contains references to facts")
            
            # Look for ix:data or xbrl:data assignments
            if script.string and re.search(r'ix\.data|xbrl\.data', script.string, re.DOTALL):
                print("  Contains ix.data or xbrl.data assignments")
    else:
        print("No scripts with XBRL data found")
    
    # Summary
    print("\nDocument Analysis Summary:")
    print(f"- iXBRL elements: {len(ix_elements)}")
    print(f"- Hidden sections: {len(hidden_divs)}")
    print(f"- ix:references sections: {len(ix_refs)}")
    print(f"- Potential fact elements: {potential_facts}")
    print(f"- Scripts with XBRL data: {len(xbrl_scripts)}")
    
    return {
        "ix_elements": len(ix_elements),
        "hidden_sections": len(hidden_divs),
        "ix_references": len(ix_refs),
        "potential_facts": potential_facts,
        "xbrl_scripts": len(xbrl_scripts)
    }

def fetch_sec_ixbrl_report(filing_metadata):
    """
    Fetch the SEC's iXBRL report for a given filing by constructing the URL
    to access the actual rendered HTML document, rather than the viewer wrapper.
    """
    ticker = filing_metadata.get("ticker", "")
    cik = filing_metadata.get("cik", "")
    accession_number = filing_metadata.get("accession_number", "")
    period_end_date = filing_metadata.get("period_end_date", "")
    
    if not cik or not accession_number:
        print("Missing required metadata (CIK or accession number)")
        return None
    
    # Remove dashes from accession number
    clean_accn = accession_number.replace("-", "")
    
    # Base URL for SEC EDGAR archives
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_accn}/"
    
    # Try several possible file patterns
    potential_files = []
    
    # Try the common pattern: ticker-YYYYMMDD.htm
    if ticker and period_end_date:
        date_str = period_end_date.replace("-", "")
        potential_files.append(f"{ticker.lower()}-{date_str}.htm")
    
    # Other common patterns
    potential_files.extend([
        "index.htm",
        "primary_doc.htm",
        "form10-k.htm",
        "form10-q.htm",
        "10k.htm",
        "10q.htm",
        "filing.htm",
        "report.htm"
    ])
    
    # Try each potential file
    for file_name in potential_files:
        url = f"{base_url}{file_name}"
        print(f"Trying: {url}")
        
        response = sec_request(url)
        if response and response.status_code == 200:
            print(f"Found document at: {url}")
            
            # Save the document
            output_dir = os.path.join("data", "raw", ticker, filing_metadata.get("filing_type", ""))
            os.makedirs(output_dir, exist_ok=True)
            
            output_path = os.path.join(output_dir, f"direct_{file_name}")
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            print(f"Saved to: {output_path}")
            
            # Analyze the document
            soup = BeautifulSoup(response.content, 'html.parser')
            perform_structure_analysis(soup, f"SEC iXBRL Report ({file_name})")
            
            return output_path
    
    print("Could not find any matching document")
    return None

def check_ixbrl_viewer_version(filing_metadata):
    """Check which iXBRL viewer version is being used for a filing"""
    print("Checking iXBRL viewer version...")
    
    # Create the URL to the SEC's iXBRL viewer for this filing
    cik = filing_metadata.get("cik", "")
    accession_number = filing_metadata.get("accession_number", "")
    period_end_date = filing_metadata.get("period_end_date", "")
    ticker = filing_metadata.get("ticker", "").lower()
    
    if not cik or not accession_number:
        print("Missing required metadata (CIK or accession number)")
        return
    
    # Clean accession number
    clean_accn = accession_number.replace("-", "")
    
    # Construct document URL
    if period_end_date:
        formatted_date = period_end_date.replace("-", "")
        doc_name = f"{ticker}-{formatted_date}.htm"
    else:
        doc_name = f"{ticker}.htm"
    
    viewer_url = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{clean_accn}/{doc_name}"
    print(f"Accessing viewer URL: {viewer_url}")
    
    # Fetch the viewer page
    response = sec_request(viewer_url)
    if not response or response.status_code != 200:
        print(f"Failed to access viewer: {response.status_code if response else 'No response'}")
        return
    
    # Parse the viewer page
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Check for viewer version indicators
    script_tags = soup.find_all('script')
    viewer_version = "Unknown"
    
    for script in script_tags:
        if script.string:
            # Look for ixviewer-plus
            if "ixviewer-plus" in script.string:
                viewer_version = "iXBRL Viewer Plus (newer version)"
                break
            # Look for ixviewer without plus
            elif "ixviewer" in script.string and "ixviewer-plus" not in script.string:
                viewer_version = "iXBRL Viewer (older version)"
                break
    
    print(f"iXBRL viewer version: {viewer_version}")
    
    # Check for iframe
    iframe = soup.find('iframe', {'id': 'ixvFrame'})
    if iframe:
        print("Document uses iframe to load the actual content")
    
    # Check what the JavaScript is doing
    viewer_target = None
    for script in script_tags:
        if script.string and "loadViewer" in script.string:
            target_match = re.search(r'loadViewer\("([^"]+)"', script.string)
            if target_match:
                viewer_target = target_match.group(1)
                break
    
    if viewer_target:
        print(f"Viewer loads content from: {viewer_target}")
    
    # Try to access the MetaLinks.json file
    metalinks_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_accn}/MetaLinks.json"
    print(f"Checking for MetaLinks.json: {metalinks_url}")
    
    metalinks_response = sec_request(metalinks_url)
    if metalinks_response and metalinks_response.status_code == 200:
        print("MetaLinks.json is available")
        try:
            metalinks_data = metalinks_response.json()
            print("MetaLinks.json contains the following keys:")
            for key in metalinks_data.keys():
                print(f"  - {key}")
                
            # Look for main HTML file in report files
            if "reportFiles" in metalinks_data:
                html_files = [f for f in metalinks_data["reportFiles"].keys() if f.endswith('.htm')]
                if html_files:
                    print(f"Found HTML files in reportFiles: {html_files}")
                    
                    # Try to download the first HTML file
                    html_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{clean_accn}/{html_files[0]}"
                    print(f"Fetching HTML file: {html_url}")
                    
                    html_response = sec_request(html_url)
                    if html_response and html_response.status_code == 200:
                        print(f"Successfully fetched HTML file ({len(html_response.content)} bytes)")
                        
                        # Save the file
                        output_dir = os.path.join("data", "raw", ticker.upper(), filing_metadata.get("filing_type", ""))
                        os.makedirs(output_dir, exist_ok=True)
                        
                        output_path = os.path.join(output_dir, f"metalinks_{html_files[0]}")
                        with open(output_path, 'wb') as f:
                            f.write(html_response.content)
                        
                        print(f"Saved to: {output_path}")
                        
                        # Analyze the document
                        html_soup = BeautifulSoup(html_response.content, 'html.parser')
                        perform_structure_analysis(html_soup, f"HTML from MetaLinks ({html_files[0]})")
        except Exception as e:
            print(f"Error parsing MetaLinks.json: {str(e)}")
    else:
        print(f"MetaLinks.json is not available: {metalinks_response.status_code if metalinks_response else 'No response'}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze Google's SEC filing structure")
    parser.add_argument("--filing-type", default="10-K", help="Filing type to analyze")
    parser.add_argument("--html-file", help="Path to existing HTML file to analyze")
    parser.add_argument("--force-download", action="store_true", help="Force download of documents")
    parser.add_argument("--check-viewer", action="store_true", help="Check iXBRL viewer version")
    parser.add_argument("--fetch-report", action="store_true", help="Fetch the SEC's iXBRL report")
    
    args = parser.parse_args()
    
    # Get filing metadata for Google
    ticker = "GOOGL"
    filing_result = find_company_filings(ticker, [args.filing_type])
    
    if "error" in filing_result:
        print(f"Error finding Google's filing: {filing_result['error']}")
        sys.exit(1)
    
    if args.filing_type not in filing_result.get("filings", {}):
        print(f"No {args.filing_type} filing found for Google")
        sys.exit(1)
    
    filing_metadata = filing_result["filings"][args.filing_type]
    filing_metadata["ticker"] = ticker
    filing_metadata["company_name"] = filing_result.get("company_name")
    
    if args.check_viewer:
        check_ixbrl_viewer_version(filing_metadata)
    elif args.fetch_report:
        fetch_sec_ixbrl_report(filing_metadata)
    else:
        analyze_google_filing_structure(args.html_file, args.filing_type, args.force_download)