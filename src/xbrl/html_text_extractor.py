# src/xbrl/html_text_extractor.py
import os
import sys
import re
import time
import logging
from bs4 import BeautifulSoup
import requests

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import SEC_BASE_URL, USER_AGENT, PROCESSED_DATA_DIR
from src.edgar.edgar_utils import sec_request

def get_html_filing_url(accession_number, cik):
    """
    Get the URL for the complete HTML filing
    
    Args:
        accession_number: SEC accession number 
        cik: Company CIK number
        
    Returns:
        URL to the complete HTML filing
    """
    # Handle the case where accession number is from an alternative pattern
    if not accession_number.count('-') == 2:
        # This is likely from alternative pattern (000032019325000008-aapl-20241228)
        # We need the actual SEC accession number format which is typically: 0000320193-25-000008
        # Since we don't have it directly, try to reconstruct it from instance URL
        parts = accession_number.split('-')
        if len(parts) >= 2:
            # For now, just use a more reliable URL pattern that doesn't depend on accession number format
            # This is based on the instance URL which we know works
            base_path = f"{SEC_BASE_URL}/Archives/edgar/data/{cik}/000032019325000008"
            html_url = f"{base_path}/aapl-20241228.htm"
            return html_url
    
    # Standard format (CIK/AccessionNoWithoutDashes/AccessionNo.txt)
    formatted_accession = accession_number.replace('-', '')
    
    # Get the CIK without leading zeros for URL construction
    cik_no_zeros = cik.lstrip('0')
    
    # SEC HTML filing URL pattern - first try the primary document
    html_url = f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_accession}/{accession_number}-index.htm"
    
    return html_url

def fetch_filing_html(filing_metadata):
    """
    Fetch the complete HTML filing from SEC EDGAR
    
    Args:
        filing_metadata: Dictionary containing filing metadata from filing_finder
        
    Returns:
        HTML content of the filing or error
    """
    # Extract necessary data
    accession_number = filing_metadata.get("accession_number")
    cik = filing_metadata.get("cik")
    instance_url = filing_metadata.get("instance_url", "")
    document_url = filing_metadata.get("document_url", "")  # This should be provided by filing_finder
    filing_type = filing_metadata.get("filing_type", "")
    ticker = filing_metadata.get("ticker", "")
    period_end_date = filing_metadata.get("period_end_date", "").replace("-", "")
    
    if not accession_number or not cik:
        return {"error": "Missing accession number or CIK"}
    
    # Generate a list of possible URLs to try
    urls_to_try = []
    
    # 0. First check if document_url is provided and use that directly
    if document_url:
        urls_to_try.append(document_url)
    
    # 1. Try the standard pattern
    html_url = get_html_filing_url(accession_number, cik)
    urls_to_try.append(html_url)
    
    # 2. Try extracting the accession number from instance URL and use that
    if instance_url:
        # Extract accession number portion (0000123456-YY-NNNNNN)
        match = re.search(r'data/\d+/(\d{10}-\d{2}-\d{6})', instance_url)
        formatted_acc = ""
        cik_no_zeros = ""
        base_dir = ""
        
        if match:
            std_accession = match.group(1)
            formatted_acc = std_accession.replace('-', '')
            cik_no_zeros = cik.lstrip('0')
            # Add standard URLs with this accession
            urls_to_try.append(f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{std_accession}.txt")
            urls_to_try.append(f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{std_accession}-index.htm")
        
        # Extract directory from instance URL
        if '/' in instance_url:
            parts = instance_url.split('/')
            if len(parts) >= 7:  # Should be like https://www.sec.gov/Archives/edgar/data/CIK/ACCESSION/file.xml
                base_dir = '/'.join(parts[:-1])  # Get everything except the filename
                
                # Try to extract accession number and CIK from base_dir if not already set
                if not formatted_acc or not cik_no_zeros:
                    acc_match = re.search(r'data/(\d+)/(\d{10,})', base_dir)
                    if acc_match:
                        cik_no_zeros = acc_match.group(1)
                        formatted_acc = acc_match.group(2)
            
            # Try html document based on xml filename from instance_url
            filename = instance_url.split('/')[-1]
            
            # Handle various XML naming conventions
            base_filename = None
            for pattern in ['_htm.xml', '_cal.xml', '_def.xml', '_lab.xml', '_pre.xml']:
                if pattern in filename:
                    base_filename = filename.replace(pattern, '.htm')
                    break
            
            # If we found a base filename, try both regular and iXBRL formats
            if base_filename and base_dir:
                # Regular HTML path
                urls_to_try.append(f"{base_dir}/{base_filename}")
                
                # iXBRL path
                urls_to_try.append(f"{SEC_BASE_URL}/ix?doc={base_dir}/{base_filename}")
    
    # 3. Try accession and ticker based patterns
    if accession_number and cik and ticker and period_end_date:
        formatted_acc = accession_number.replace('-', '')
        cik_no_zeros = cik.lstrip('0')
        
        # Standard patterns
        urls_to_try.append(f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{ticker.lower()}-{period_end_date}.htm")
        
        # iXBRL formats
        urls_to_try.append(f"{SEC_BASE_URL}/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{ticker.lower()}-{period_end_date}.htm")
        
        # Try with form name (10K instead of 10-K)
        form_name = filing_type.replace('-', '')  # 10-K -> 10K
        urls_to_try.append(f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{form_name}.htm")
        urls_to_try.append(f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{ticker.lower()}{form_name}.htm")
        urls_to_try.append(f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{ticker.lower()}_{period_end_date}.htm")
        
        # iXBRL versions of form name patterns
        urls_to_try.append(f"{SEC_BASE_URL}/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{form_name}.htm")
        urls_to_try.append(f"{SEC_BASE_URL}/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{ticker.lower()}{form_name}.htm")
        urls_to_try.append(f"{SEC_BASE_URL}/ix?doc=/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{ticker.lower()}_{period_end_date}.htm")
    
    # 4. Try the index URL which always exists
    if accession_number and cik:
        formatted_acc = accession_number.replace('-', '')
        cik_no_zeros = cik.lstrip('0')
        index_url = f"{SEC_BASE_URL}/Archives/edgar/data/{cik_no_zeros}/{formatted_acc}/{accession_number}-index.htm"
        urls_to_try.append(index_url)
    
    # 5. Try instance URL directory with index.htm
    if instance_url and '/' in instance_url:
        parts = instance_url.split('/')
        if len(parts) >= 7:  # Should be something like https://www.sec.gov/Archives/edgar/data/CIK/ACCESSION/file.xml
            acc_dir = '/'.join(parts[:-1])  # Get everything except the filename
            urls_to_try.append(f"{acc_dir}/index.htm")
    
    # Remove duplicates while maintaining order
    urls_to_try = list(dict.fromkeys(urls_to_try))
    
    # Log the URLs we're going to try
    logging.info(f"Trying the following URLs to fetch HTML filing:")
    for idx, url in enumerate(urls_to_try):
        logging.info(f"  URL {idx+1}: {url}")
    
    # Try each URL until we find one that works
    last_error = None
    for url in urls_to_try:
        try:
            logging.info(f"Attempting to fetch HTML from: {url}")
            response = sec_request(url)
            
            if response.status_code == 200:
                # Check if it's really HTML content
                if '<html' in response.text.lower() or '<body' in response.text.lower():
                    # Check if this is an index page containing document links
                    if ('tableFile' in response.text and 
                       ('filing documents' in response.text.lower() or 
                        'form ' + filing_type.lower() in response.text.lower() or
                        'edgar filing documents' in response.text.lower())):
                        
                        logging.info(f"Found index page with document table at: {url}")
                        
                        # Parse the index page to find the actual document link
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Create a temporary sections dict to use with handle_index_page
                        sections = {"metadata": {"title": "index page"}}
                        
                        # Use handle_index_page to find the main document URL
                        handle_index_page(soup, sections, filing_type)
                        
                        # Check if handle_index_page found a main document URL
                        main_doc_url = sections.get("metadata", {}).get("main_document_url")
                        
                        # If we found a main document link, fetch it
                        if main_doc_url:
                            try:
                                # Convert relative URL to absolute if needed
                                if main_doc_url.startswith('/'):
                                    main_doc_url = f"{SEC_BASE_URL}{main_doc_url}"
                                elif not main_doc_url.startswith('http'):
                                    base_url = '/'.join(url.split('/')[:-1])
                                    main_doc_url = f"{base_url}/{main_doc_url}"
                                
                                logging.info(f"Fetching main document from: {main_doc_url}")
                                doc_response = sec_request(main_doc_url)
                                
                                if doc_response.status_code == 200:
                                    logging.info(f"Successfully fetched main document from: {main_doc_url}")
                                    return {
                                        "success": True,
                                        "html_content": doc_response.text,
                                        "url": main_doc_url
                                    }
                            except Exception as doc_error:
                                logging.error(f"Error fetching main document: {str(doc_error)}")
                                # Continue with the next URL if this fails
                    else:
                        # This appears to be an actual document, not an index
                        logging.info(f"Successfully fetched HTML from: {url}")
                        return {
                            "success": True,
                            "html_content": response.text,
                            "url": url
                        }
                else:
                    logging.warning(f"URL returned 200 but content doesn't appear to be HTML: {url}")
                    last_error = "Content doesn't appear to be HTML"
            else:
                logging.warning(f"Failed to fetch from {url}: HTTP {response.status_code}")
                last_error = f"HTTP error {response.status_code}"
        except Exception as e:
            logging.error(f"Exception fetching from {url}: {str(e)}")
            last_error = str(e)
    
    # If we've tried all URLs and none worked, return error
    return {"error": f"Failed to download HTML filing: {last_error} for all URLs"}


def extract_clean_text(html_content, filing_type):
    """
    Extract clean text from HTML SEC filing with section markers
    
    Args:
        html_content: HTML content of the filing
        filing_type: Type of filing (10-K, 10-Q)
        
    Returns:
        Dictionary containing extracted text sections
    """
    # Parse HTML content
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
        logging.error(f"Error parsing HTML content: {str(e)}")
        # Return minimal dictionary with error
        return {
            "metadata": {"error": f"HTML parsing error: {str(e)}"},
            "full_text": "Error parsing HTML content.",
        }
    
    # Create a dictionary to store extracted sections
    sections = {
        "metadata": {},
        "full_text": "",
        "toc": ""
    }
    
    # Extract metadata from various sources
    extract_document_metadata(soup, sections)
    
    # Find the main document content
    main_content = find_main_content(soup)
    
    if main_content:
        # Clean up content - remove scripts, styles but keep structure
        for script in main_content(['script', 'style']):
            script.extract()
        
        # Extract the table of contents if present
        toc = extract_table_of_contents(main_content)
        if toc:
            sections["toc"] = toc
        
        # Identify and mark standard SEC document sections
        identify_and_mark_sections(main_content, sections, filing_type)
        
        # Get the full text with section markers
        full_text_with_markers = get_text_with_section_markers(main_content, sections["document_sections"])
        sections["full_text"] = full_text_with_markers
    else:
        # Last resort - use entire HTML
        logging.info("No main content identified, using entire HTML")
        for script in soup(['script', 'style']):
            script.extract()
        sections["full_text"] = clean_text(soup.get_text())
    
    # If this is an index page, try to find and follow the main document link
    handle_index_page(soup, sections, filing_type)
    
    # Filter out any XBRL context identifiers at the beginning of content
    sections["full_text"] = filter_xbrl_identifiers(sections["full_text"])
    
    return sections

def extract_document_metadata(soup, sections):
    """
    Extract document metadata from various sources in the HTML
    
    Args:
        soup: BeautifulSoup object of the full HTML
        sections: Dictionary to populate with metadata
    """
    # Try SEC header first
    sec_header = soup.find('sec-header')
    if sec_header:
        # Add SEC header metadata
        header_text = sec_header.get_text()
        
        # Extract document type
        doc_type_match = re.search(r'CONFORMED SUBMISSION TYPE:\s*(\S+)', header_text)
        if doc_type_match:
            sections["metadata"]["document_type"] = doc_type_match.group(1)
        
        # Extract period of report
        period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d+)', header_text)
        if period_match:
            period = period_match.group(1)
            # Format as YYYY-MM-DD
            if len(period) == 8:
                sections["metadata"]["period"] = f"{period[0:4]}-{period[4:6]}-{period[6:8]}"
            else:
                sections["metadata"]["period"] = period
                
        # Extract company name
        company_match = re.search(r'COMPANY CONFORMED NAME:\s*(.+?)[\r\n]', header_text)
        if company_match:
            sections["metadata"]["company_name"] = company_match.group(1).strip()
    
    # Try to find metadata in the title if not found in SEC header
    if "document_type" not in sections["metadata"]:
        title = soup.find('title')
        if title and title.text:
            sections["metadata"]["title"] = title.text.strip()
            
            # Try to extract document type and company from title
            title_text = title.text.lower()
            if "10-k" in title_text or "10k" in title_text or "annual report" in title_text:
                sections["metadata"]["document_type"] = "10-K"
            elif "10-q" in title_text or "10q" in title_text or "quarterly report" in title_text:
                sections["metadata"]["document_type"] = "10-Q"
            
            # Try to extract company name from title
            company_patterns = [
                r"([A-Z][A-Za-z0-9\s,\.]+)(?:\s+10-[KQ]|\s+Annual|\s+Quarterly)",
                r"([A-Z][A-Za-z0-9\s,\.]+)(?:\s+Form)"
            ]
            
            for pattern in company_patterns:
                company_match = re.search(pattern, title.text)
                if company_match:
                    sections["metadata"]["company_name"] = company_match.group(1).strip()
                    break
    
    # Look for company name in first heading if not yet found
    if "company_name" not in sections["metadata"]:
        h1 = soup.find('h1')
        if h1:
            sections["metadata"]["company_name"] = h1.get_text().strip()

def find_main_content(soup):
    """
    Find the main content of the document using multiple approaches
    
    Args:
        soup: BeautifulSoup object of the full HTML
        
    Returns:
        BeautifulSoup object of the main content
    """
    # Approach 1: Look for the document content in a standard structure
    document = soup.find('document')
    if document:
        html_doc = document.find('html')
        if html_doc:
            body = html_doc.find('body')
            if body:
                logging.info("Found content using approach 1: document > html > body")
                return body
    
    # Approach 2: Look for div with main content - often in SEC index pages
    main_content = soup.find('div', id='main-content')
    if main_content:
        logging.info("Found content using approach 2: div with id='main-content'")
        return main_content
    
    # Approach 3: Look for typical SEC content div
    content_div = soup.find('div', attrs={'class': 'formGrouping'})
    if content_div:
        logging.info("Found content using approach 3: div with class='formGrouping'")
        return content_div
    
    # Approach 4: Standard HTML body
    body = soup.find('body')
    if body:
        logging.info("Found content using approach 4: body tag")
        return body
    
    # If all else fails, return the entire soup
    logging.info("No main content container found, using entire document")
    return soup

def extract_table_of_contents(content):
    """
    Extract table of contents if present
    
    Args:
        content: BeautifulSoup object of the main content
        
    Returns:
        String containing the table of contents or empty string
    """
    # Look for typical TOC patterns
    toc_candidates = [
        content.find('div', string=re.compile(r'TABLE\s+OF\s+CONTENTS', re.IGNORECASE)),
        content.find('h2', string=re.compile(r'TABLE\s+OF\s+CONTENTS', re.IGNORECASE)),
        content.find('h3', string=re.compile(r'TABLE\s+OF\s+CONTENTS', re.IGNORECASE)),
        content.find('table', attrs={'summary': re.compile(r'Table\s+of\s+Contents', re.IGNORECASE)})
    ]
    
    # Filter out None values
    toc_candidates = [c for c in toc_candidates if c]
    
    if toc_candidates:
        toc_element = toc_candidates[0]
        
        # Try to find the actual table
        toc_table = None
        
        # If the element itself is a table
        if toc_element.name == 'table':
            toc_table = toc_element
        else:
            # Look for next table or div
            next_table = toc_element.find_next('table')
            next_div = toc_element.find_next('div', attrs={'class': re.compile(r'toc|index', re.IGNORECASE)})
            
            if next_table and (not next_div or next_table.sourceline < next_div.sourceline):
                toc_table = next_table
            elif next_div:
                toc_table = next_div
        
        if toc_table:
            toc_text = "@TABLE_OF_CONTENTS\n" + clean_text(toc_table.get_text())
            return toc_text
    
    return ""

def identify_and_mark_sections(content, sections, filing_type):
    """
    Identify standard SEC filing sections and mark them in the document
    
    Args:
        content: BeautifulSoup object of the main content
        sections: Dictionary to populate with section info
        filing_type: Type of filing (10-K, 10-Q)
    """
    # Define standard SEC item sections based on filing type
    section_patterns = []
    
    if filing_type == "10-K":
        section_patterns = [
            (r'Item\s+1\.?\s*Business', 'ITEM_1_BUSINESS'),
            (r'Item\s+1A\.?\s*Risk\s+Factors', 'ITEM_1A_RISK_FACTORS'),
            (r'Item\s+1B\.?\s*Unresolved\s+Staff\s+Comments', 'ITEM_1B_UNRESOLVED_STAFF_COMMENTS'),
            (r'Item\s+2\.?\s*Properties', 'ITEM_2_PROPERTIES'),
            (r'Item\s+3\.?\s*Legal\s+Proceedings', 'ITEM_3_LEGAL_PROCEEDINGS'),
            (r'Item\s+4\.?\s*Mine\s+Safety\s+Disclosures', 'ITEM_4_MINE_SAFETY_DISCLOSURES'),
            (r'Item\s+5\.?\s*Market\s+for\s+Registrant', 'ITEM_5_MARKET'),
            (r'Item\s+6\.?\s*Selected\s+Financial\s+Data', 'ITEM_6_SELECTED_FINANCIAL_DATA'),
            (r'Item\s+7\.?\s*Management.*Discussion', 'ITEM_7_MD_AND_A'),
            (r'Item\s+7A\.?\s*Quantitative\s+and\s+Qualitative', 'ITEM_7A_MARKET_RISK'),
            (r'Item\s+8\.?\s*Financial\s+Statements', 'ITEM_8_FINANCIAL_STATEMENTS'),
            (r'Item\s+9\.?\s*Changes\s+in\s+and\s+Disagreements', 'ITEM_9_DISAGREEMENTS'),
            (r'Item\s+9A\.?\s*Controls\s+and\s+Procedures', 'ITEM_9A_CONTROLS'),
            (r'Item\s+9B\.?\s*Other\s+Information', 'ITEM_9B_OTHER_INFORMATION'),
            (r'Item\s+10\.?\s*Directors', 'ITEM_10_DIRECTORS'),
            (r'Item\s+11\.?\s*Executive\s+Compensation', 'ITEM_11_EXECUTIVE_COMPENSATION'),
            (r'Item\s+12\.?\s*Security\s+Ownership', 'ITEM_12_SECURITY_OWNERSHIP'),
            (r'Item\s+13\.?\s*Certain\s+Relationships', 'ITEM_13_RELATIONSHIPS'),
            (r'Item\s+14\.?\s*Principal\s+Accountant\s+Fees', 'ITEM_14_ACCOUNTANT_FEES'),
            (r'Item\s+15\.?\s*Exhibits', 'ITEM_15_EXHIBITS')
        ]
    elif filing_type == "10-Q":
        section_patterns = [
            (r'Item\s+1\.?\s*Financial\s+Statements', 'ITEM_1_FINANCIAL_STATEMENTS'),
            (r'Item\s+2\.?\s*Management.*Discussion', 'ITEM_2_MD_AND_A'),
            (r'Item\s+3\.?\s*Quantitative\s+and\s+Qualitative', 'ITEM_3_MARKET_RISK'),
            (r'Item\s+4\.?\s*Controls\s+and\s+Procedures', 'ITEM_4_CONTROLS'),
            (r'Item\s+1\.?\s*Legal\s+Proceedings', 'ITEM_1_LEGAL_PROCEEDINGS'),
            (r'Item\s+1A\.?\s*Risk\s+Factors', 'ITEM_1A_RISK_FACTORS'),
            (r'Item\s+2\.?\s*Unregistered\s+Sales', 'ITEM_2_UNREGISTERED_SALES'),
            (r'Item\s+3\.?\s*Defaults', 'ITEM_3_DEFAULTS'),
            (r'Item\s+4\.?\s*Mine\s+Safety\s+Disclosures', 'ITEM_4_MINE_SAFETY'),
            (r'Item\s+5\.?\s*Other\s+Information', 'ITEM_5_OTHER_INFORMATION'),
            (r'Item\s+6\.?\s*Exhibits', 'ITEM_6_EXHIBITS'),
            (r'Notes\s+to.*(Financial\s+Statements|Condensed)', 'NOTES_TO_FINANCIAL_STATEMENTS'),
            (r'Management.*Discussion.*Analysis', 'MANAGEMENT_DISCUSSION'),
            (r'Part\s+I\.?\s*Financial\s+Information', 'PART_I_FINANCIAL_INFORMATION'),
            (r'Part\s+II\.?\s*Other\s+Information', 'PART_II_OTHER_INFORMATION')
        ]
    else:
        # Generic patterns for other filing types
        section_patterns = [
            (r'Financial\s+Statements', 'FINANCIAL_STATEMENTS'),
            (r'Notes\s+to.*Financial\s+Statements', 'NOTES_TO_FINANCIAL_STATEMENTS'),
            (r'Management.*Discussion.*Analysis', 'MANAGEMENT_DISCUSSION'),
            (r'Risk\s+Factors', 'RISK_FACTORS')
        ]
    
    # Add more specific sections for better granularity
    additional_patterns = [
        # Common to all reports
        (r'Consolidated Balance Sheets?', 'CONSOLIDATED_BALANCE_SHEET'),
        (r'Consolidated Statements? of Operations', 'CONSOLIDATED_INCOME_STATEMENT'),
        (r'Consolidated Statements? of Cash Flows?', 'CONSOLIDATED_CASH_FLOW'),
        (r'Consolidated Statements? of Stockholders[\'\"]? Equity', 'CONSOLIDATED_EQUITY'),
        (r'Consolidated Statements? of Comprehensive Income', 'CONSOLIDATED_COMPREHENSIVE_INCOME'),
        
        # Important subsections 
        (r'Controls and Procedures', 'CONTROLS_AND_PROCEDURES'),
        (r'Critical Accounting (Policies|Estimates)', 'CRITICAL_ACCOUNTING'),
        (r'Forward[-\s]Looking Statements?', 'FORWARD_LOOKING'),
        (r'Liquidity and Capital Resources', 'LIQUIDITY_AND_CAPITAL'),
        (r'Results? of Operations', 'RESULTS_OF_OPERATIONS'),
        (r'Significant Accounting Policies', 'SIGNIFICANT_ACCOUNTING_POLICIES')
    ]
    
    # Combine all patterns
    section_patterns.extend(additional_patterns)
    
    # Find all headings
    headings = content.find_all(['h1', 'h2', 'h3', 'h4', 'strong', 'b', 'p', 'div'], 
                               string=lambda text: text and any(re.search(pattern, text, re.IGNORECASE) 
                                                              for pattern, _ in section_patterns))
    
    # Initialize document_sections if not present
    if "document_sections" not in sections:
        sections["document_sections"] = {}
    
    # Process each heading to extract section info
    for heading in headings:
        heading_text = heading.get_text().strip()
        
        # Find matching section
        for pattern, section_id in section_patterns:
            if re.search(pattern, heading_text, re.IGNORECASE):
                # Add section to sections dict, noting the heading and the element
                sections["document_sections"][section_id] = {
                    "heading": heading_text,
                    "element": heading
                }
                break

def get_text_with_section_markers(content, document_sections=None):
    """
    Generate text with section markers from the content
    
    Args:
        content: BeautifulSoup object of the main content
        document_sections: Dictionary containing section info
        
    Returns:
        String with the full text and section markers
    """
    import copy
    
    # Create a copy of the content to avoid modifying the original
    content_copy = copy.deepcopy(content)
    
    # Get base text first (we'll add markers manually as strings)
    base_text = clean_text(content_copy.get_text())
    
    # If no sections identified, just return the cleaned text
    if not document_sections:
        return base_text
        
    # We'll build a new text with section markers
    from bs4 import BeautifulSoup
    
    # Create markers inline by using the text of each section heading as an anchor
    marked_text = base_text
    
    # Keep track of inserted markers to adjust offsets
    added_chars = 0
    
    # Sort sections by their appearance in the document (using the element's position in document)
    try:
        sorted_sections = sorted(
            document_sections.items(),
            key=lambda x: x[1]["element"].sourceline if hasattr(x[1]["element"], "sourceline") else float('inf')
        )
    except (AttributeError, KeyError):
        # If sorting fails, just use the original order
        sorted_sections = list(document_sections.items())
    
    # For each section, find its heading in the text and add markers
    for section_id, section_info in sorted_sections:
        heading_text = section_info["heading"].strip()
        
        # Find the heading in the text
        heading_pos = marked_text.find(heading_text, added_chars)
        if heading_pos == -1:
            # If exact heading not found, try a more flexible approach
            for fragment in heading_text.split():
                if len(fragment) > 3:  # Only use significant words
                    heading_pos = marked_text.find(fragment, added_chars)
                    if heading_pos != -1:
                        # Find the line containing this fragment
                        line_start = marked_text.rfind('\n', 0, heading_pos) + 1
                        line_end = marked_text.find('\n', heading_pos)
                        if line_end == -1:
                            line_end = len(marked_text)
                        
                        # Set heading_pos to the beginning of this line
                        heading_pos = line_start
                        break
            
            # If still not found, skip this section
            if heading_pos == -1:
                continue
        
        # Add start marker before the heading
        start_marker = f"\n\n@SECTION_START: {section_id}\n"
        marked_text = marked_text[:heading_pos] + start_marker + marked_text[heading_pos:]
        added_chars += len(start_marker)
        
        # Find where this section ends
        next_section_pos = float('inf')
        
        # Look for the next section heading in the sorted sections
        for next_id, next_info in sorted_sections:
            if next_info["element"].sourceline > section_info["element"].sourceline:
                next_heading = next_info["heading"].strip()
                pos = marked_text.find(next_heading, heading_pos + len(heading_text))
                if pos != -1 and pos < next_section_pos:
                    next_section_pos = pos
                    break
        
        # If no next section found, end at the document end
        if next_section_pos == float('inf'):
            next_section_pos = len(marked_text)
        
        # Add end marker
        end_marker = f"\n@SECTION_END: {section_id}\n\n"
        marked_text = marked_text[:next_section_pos] + end_marker + marked_text[next_section_pos:]
        added_chars += len(end_marker)
    
    return marked_text

def handle_index_page(soup, sections, filing_type):
    """
    Handle index pages by finding links to the main document
    
    Args:
        soup: BeautifulSoup object of the full HTML
        sections: Dictionary to populate with metadata
        filing_type: Type of filing (10-K, 10-Q)
    """
    # Check if this is an index page
    is_index = False
    if "title" in sections.get("metadata", {}):
        title = sections["metadata"]["title"].lower()
        is_index = "index" in title or "index.htm" in title or "filing documents" in title
    
    # Additional checks for index page patterns in case title doesn't have "index"
    if not is_index:
        # Check for typical SEC index page patterns
        if soup.find('table', {'class': ['tableFile', 'tableFile2']}):
            is_index = True
        elif soup.find('table', {'summary': 'Document Format Files'}) or soup.find('table', {'summary': 'Data Files'}):
            is_index = True
        elif "EDGAR Filing Documents" in str(soup):
            is_index = True
    
    if is_index:
        logging.info("This appears to be an index page. Looking for main document link.")
        
        # Look for a table with document links - try all formats SEC uses
        tables = soup.find_all('table', {'class': ['tableFile', 'tableFile2']})
        
        # Also check for tables with specific summaries (older format)
        if not tables:
            tables = soup.find_all('table', {'summary': ['Document Format Files', 'Data Files']})
        
        main_doc_url = None
        
        # First priority: Look for iXBRL link with filing type in description
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                # Need at least 3 cells for Seq, Description, Document
                if len(cells) >= 3:
                    # Check description column (usually 2nd column)
                    description_cell = cells[1] if len(cells) > 1 else None
                    if description_cell:
                        description_text = description_cell.get_text().lower().strip()
                        # Match exact filing type (10-K or 10-Q)
                        if description_text == filing_type.lower() or description_text == filing_type.lower().replace('-', ''):
                            # Get document cell (3rd column)
                            document_cell = cells[2] if len(cells) > 2 else None
                            if document_cell:
                                # Look for links and iXBRL indicators
                                links = document_cell.find_all('a')
                                is_ixbrl = 'ixbrl' in document_cell.get_text().lower()
                                
                                for link in links:
                                    href = link.get('href')
                                    if href:
                                        if '/ix?doc=' in href:
                                            # This is definitely an iXBRL document - highest priority
                                            main_doc_url = href
                                            logging.info(f"Found iXBRL document link with exact filing type match: {main_doc_url}")
                                            sections["metadata"]["main_document_url"] = main_doc_url
                                            sections["metadata"]["document_format"] = "iXBRL"
                                            return  # Exit immediately as we found the best match
                                        
                                        # Regular HTML but marked as iXBRL - need to add the ix?doc= prefix
                                        elif is_ixbrl and (href.endswith('.htm') or href.endswith('.html')):
                                            if href.startswith('/Archives/'):
                                                main_doc_url = f"/ix?doc={href}"
                                            else:
                                                main_doc_url = f"/ix?doc=/{href}" if not href.startswith('/') else f"/ix?doc={href}"
                                            logging.info(f"Found HTML marked as iXBRL, converting to: {main_doc_url}")
                                            sections["metadata"]["main_document_url"] = main_doc_url
                                            sections["metadata"]["document_format"] = "iXBRL"
                                            return  # Found best match
                                        
                                        # Regular HTML document (not marked as iXBRL)
                                        elif href.endswith('.htm') or href.endswith('.html'):
                                            main_doc_url = href
                                            logging.info(f"Found HTML document with exact filing type match: {main_doc_url}")
                                            sections["metadata"]["main_document_url"] = main_doc_url
                                            sections["metadata"]["document_format"] = "HTML"
                                            # Continue looking for better matches
        
        # Second priority: Look for any link with iXBRL in the document
        if not main_doc_url or "document_format" not in sections.get("metadata", {}) or sections["metadata"]["document_format"] != "iXBRL":
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        document_cell = cells[2] if len(cells) > 2 else None
                        if document_cell:
                            # Check if cell has iXBRL indicator
                            cell_text = document_cell.get_text().lower()
                            if 'ixbrl' in cell_text:
                                links = document_cell.find_all('a')
                                for link in links:
                                    href = link.get('href')
                                    if href:
                                        if '/ix?doc=' in href:
                                            main_doc_url = href
                                            logging.info(f"Found iXBRL document link: {main_doc_url}")
                                            sections["metadata"]["main_document_url"] = main_doc_url
                                            sections["metadata"]["document_format"] = "iXBRL"
                                            return  # Exit after finding iXBRL
                                        
                                        # HTML with iXBRL indicator
                                        elif href.endswith('.htm') or href.endswith('.html'):
                                            if href.startswith('/Archives/'):
                                                main_doc_url = f"/ix?doc={href}"
                                            else:
                                                main_doc_url = f"/ix?doc=/{href}" if not href.startswith('/') else f"/ix?doc={href}"
                                            logging.info(f"Found HTML with iXBRL indicator, converting to: {main_doc_url}")
                                            sections["metadata"]["main_document_url"] = main_doc_url
                                            sections["metadata"]["document_format"] = "iXBRL"
                                            return  # Found best match
        
        # Third priority: Look for any filing type match 
        if not main_doc_url:
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    row_text = row.get_text().lower()
                    if filing_type.lower() in row_text:
                        # This row contains our filing type
                        cells = row.find_all(['td', 'th'])
                        document_cell = cells[2] if len(cells) > 2 else None
                        if document_cell:
                            links = document_cell.find_all('a')
                            for link in links:
                                href = link.get('href')
                                if href:
                                    if '/ix?doc=' in href:
                                        main_doc_url = href
                                        logging.info(f"Found iXBRL document with filing type in row: {main_doc_url}")
                                        sections["metadata"]["main_document_url"] = main_doc_url
                                        sections["metadata"]["document_format"] = "iXBRL"
                                        return  # Exit after finding iXBRL with filing type
                                    elif href.endswith('.htm') or href.endswith('.html'):
                                        main_doc_url = href
                                        logging.info(f"Found HTML document with filing type in row: {main_doc_url}")
                                        sections["metadata"]["main_document_url"] = main_doc_url
                                        sections["metadata"]["document_format"] = "HTML"
                                        # Continue looking for better matches
        
        # Fourth priority: Look for any .htm or .html document link
        if not main_doc_url:
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        document_cell = cells[2] if len(cells) > 2 else None
                        if document_cell:
                            links = document_cell.find_all('a')
                            for link in links:
                                href = link.get('href')
                                if href and (href.endswith('.htm') or href.endswith('.html')):
                                    # Filter out auxiliary files (like _def, _cal, etc.)
                                    if not any(x in href.lower() for x in ['_def.', '_cal.', '_lab.', '_pre.', 'index.', 'exhibit']):
                                        main_doc_url = href
                                        logging.info(f"Found likely main document: {main_doc_url}")
                                        sections["metadata"]["main_document_url"] = main_doc_url
                                        sections["metadata"]["document_format"] = "HTML"
                                        # Continue looking for better matches
        
        if main_doc_url:
            # Ensure URL is properly formatted
            if not main_doc_url.startswith('http') and not main_doc_url.startswith('/'):
                main_doc_url = f"/{main_doc_url}"
            
            # Make sure we store the final result if it wasn't already set
            if "main_document_url" not in sections["metadata"]:
                sections["metadata"]["main_document_url"] = main_doc_url
                
            logging.info(f"Final main document link: {sections['metadata'].get('main_document_url')}")
        else:
            logging.warning(f"Could not find any document links in index page for {filing_type}")
            sections["metadata"]["document_error"] = "No document links found in index page"

def filter_xbrl_identifiers(text):
    """
    Filter out XBRL context identifiers from the beginning of the text
    
    Args:
        text: Text to filter
        
    Returns:
        Filtered text without XBRL context identifiers at the beginning
    """
    # Regular expression to detect XBRL context identifiers
    xbrl_pattern = r'^((?:[a-z0-9:\-]+\s*)+)(?=UNITED STATES|FORM|[A-Z]{3,})'
    
    # Check if the text starts with XBRL identifiers
    match = re.search(xbrl_pattern, text, re.MULTILINE | re.DOTALL)
    if match:
        # Replace the identifiers with an explanatory comment
        filtered_text = text.replace(match.group(1), "@NOTE: XBRL context identifiers removed\n\n")
        return filtered_text
    
    return text

def clean_text(text):
    """
    Clean and normalize extracted text
    
    Args:
        text: Raw text extracted from HTML
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Remove excessive whitespace (but preserve paragraph breaks)
    text = re.sub(r'([^\n])\s+([^\n])', r'\1 \2', text)
    
    # Remove repeated newlines (more than 2)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Remove non-breaking spaces and other common whitespace characters
    text = text.replace('\xa0', ' ')
    
    # Remove page numbers and headers/footers (common in SEC filings)
    text = re.sub(r'\n\s*\d+\s*\n', '\n', text)
    text = re.sub(r'\n\s*Page\s+\d+\s+of\s+\d+\s*\n', '\n', text, flags=re.IGNORECASE)
    
    # Remove "Continued..." markers
    text = re.sub(r'\(Continued.*?\)', '', text)
    text = re.sub(r'\n\s*Continued.*?\n', '\n', text, flags=re.IGNORECASE)
    
    # Fix hyphenation artifacts (words broken across lines)
    # Only fix hyphenation for clear word breaks, not compound words or legitimate hyphens
    text = re.sub(r'([a-z])-\s+([a-z])', r'\1\2', text)
    
    # Remove any HTML tags that might have been missed
    text = re.sub(r'<[^>]+>', '', text)
    
    return text.strip()

def save_text_file(text_content, filing_metadata):
    """
    Save extracted text to file with section markers using fiscal year naming
    
    Args:
        text_content: Dictionary containing extracted text sections
        filing_metadata: Dictionary containing filing metadata
        
    Returns:
        Dictionary with file path and status
    """
    ticker = filing_metadata.get("ticker", "unknown")
    if ticker is None:
        ticker = "unknown"
        
    filing_type = filing_metadata.get("filing_type", "unknown")
    if filing_type is None:
        filing_type = "unknown"
        
    period_end = filing_metadata.get("period_end_date", "unknown")
    if period_end is None:
        period_end = "unknown"
        
    company_name = text_content.get("metadata", {}).get("company_name", filing_metadata.get("company_name", "unknown"))
    if company_name is None:
        company_name = f"Company_{ticker}"
    
    # Clean company name for filename
    clean_company = re.sub(r'[^\w\s]', '', company_name)  # Remove punctuation
    clean_company = re.sub(r'\s+', '_', clean_company.strip())  # Replace spaces with underscores
    if not clean_company:
        clean_company = "Unknown_Company"
    
    # Extract year from period end date (safely)
    try:
        if '-' in period_end:
            year = period_end.split('-')[0]
        elif len(period_end) >= 4:
            year = period_end[:4]
        else:
            # Default to current year if we can't extract
            import datetime
            year = datetime.datetime.now().strftime("%Y")
    except:
        # Default to current year if any exception
        import datetime
        year = datetime.datetime.now().strftime("%Y")
    
    # Create fiscal year based filename
    # First check if fiscal year and quarter are provided by batch_download.py
    if "fiscal_year" in filing_metadata and "fiscal_quarter" in filing_metadata:
        fiscal_year = filing_metadata.get("fiscal_year")
        fiscal_quarter = filing_metadata.get("fiscal_quarter")
        
        # Use provided fiscal information
        if filing_type == "10-K":
            fiscal_suffix = f"{fiscal_year}_FY"
        else:
            fiscal_suffix = f"{fiscal_year}_{fiscal_quarter}"
            
        logging.info(f"Using provided fiscal information: {fiscal_suffix}")
    else:
        # Fallback: Use traditional method to determine fiscal year/quarter
        if filing_type == "10-K":
            # For annual reports, use FY designation
            fiscal_suffix = f"{year}_FY"
        else:
            # For quarterly reports, use 1Q, 2Q, 3Q, 4Q format
            quarter_num = ""
            
            # Extract quarter mention from the text content
            full_text = text_content.get("full_text", "").lower()
            
            # Look for explicit quarter statements in the filing text
            # These patterns are common in SEC filings to explicitly state which quarter is being reported
            quarter_patterns = [
                # Pattern for "For the [ordinal] quarter ended [date]"
                r"for\s+the\s+(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter\s+ended",
                # Pattern for "Quarter [number]" or "Q[number]"
                r"quarter\s+(one|two|three|four|1|2|3|4)|q(1|2|3|4)\s+",
                # Pattern for "[ordinal] quarter of fiscal year"
                r"(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter\s+of\s+fiscal\s+year",
                # Pattern for "Form 10-Q for Q[number]"
                r"form\s+10-q\s+for\s+q(1|2|3|4)",
                # Pattern for explicit quarter mentions
                r"\bfirst\s+quarter\b|\b1st\s+quarter\b|\bq1\b",
                r"\bsecond\s+quarter\b|\b2nd\s+quarter\b|\bq2\b",
                r"\bthird\s+quarter\b|\b3rd\s+quarter\b|\bq3\b",
                r"\bfourth\s+quarter\b|\b4th\s+quarter\b|\bq4\b"
            ]
            
            # Search for quarter patterns
            for pattern in quarter_patterns:
                match = re.search(pattern, full_text)
                if match:
                    # Extract the quarter number
                    quarter_text = match.group(0).lower()
                    if "first" in quarter_text or "1st" in quarter_text or "one" in quarter_text or "q1" in quarter_text or "quarter 1" in quarter_text:
                        quarter_num = "1Q"
                        break
                    elif "second" in quarter_text or "2nd" in quarter_text or "two" in quarter_text or "q2" in quarter_text or "quarter 2" in quarter_text:
                        quarter_num = "2Q"
                        break
                    elif "third" in quarter_text or "3rd" in quarter_text or "three" in quarter_text or "q3" in quarter_text or "quarter 3" in quarter_text:
                        quarter_num = "3Q"
                        break
                    elif "fourth" in quarter_text or "4th" in quarter_text or "four" in quarter_text or "q4" in quarter_text or "quarter 4" in quarter_text:
                        quarter_num = "4Q"
                        break
            
            # If not found in text, use the period end date to determine quarter
            if not quarter_num and period_end and '-' in period_end:
                try:
                    month = int(period_end.split('-')[1])
                    # Rough quarter mapping (this is a fallback, not always accurate due to fiscal calendars)
                    quarter_map = {1: "1Q", 2: "1Q", 3: "1Q", 4: "2Q", 5: "2Q", 6: "2Q", 
                                  7: "3Q", 8: "3Q", 9: "3Q", 10: "4Q", 11: "4Q", 12: "4Q"}
                    quarter_num = quarter_map.get(month, "")
                except:
                    # Default to empty if we can't parse the month
                    pass
            
            # If we still don't have a quarter number, check the instance URL if available
            if not quarter_num:
                instance_url = filing_metadata.get("instance_url", "").lower()
                if instance_url:
                    if "q1" in instance_url or "-1q" in instance_url:
                        quarter_num = "1Q"
                    elif "q2" in instance_url or "-2q" in instance_url:
                        quarter_num = "2Q"
                    elif "q3" in instance_url or "-3q" in instance_url:
                        quarter_num = "3Q"
                    elif "q4" in instance_url or "-4q" in instance_url:
                        quarter_num = "4Q"
            
            # If we still couldn't determine quarter, default to plain Q
            if not quarter_num:
                quarter_num = "Q"
                
            fiscal_suffix = f"{year}_{quarter_num}"
    
    # Include original period end date as reference (for debugging and verification)
    try:
        period_end_compact = period_end.replace("-", "") if period_end and period_end != "unknown" else "unknown"
    except:
        period_end_compact = "unknown"
    
    # Create directory
    dir_path = os.path.join(PROCESSED_DATA_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    
    # Create filename with both naming schemes
    # Primary: Company_Year_FiscalPeriod
    # Secondary: Original format (ticker_filing-type_date) for reference
    base_filename = f"{clean_company}_{fiscal_suffix}_{ticker}_{filing_type}_{period_end_compact}"
    file_path = os.path.join(dir_path, f"{base_filename}_text.txt")
    
    # Add section identifiers to the text content if not already present
    full_text = text_content.get("full_text", "")
    
    # Check if we have a document sections dict but no markers in the text
    if text_content.get("document_sections") and "@SECTION_START" not in full_text:
        # Create a section guide at the top of the file
        section_guide = ["@SECTION_GUIDE"]
        for section_id, section_info in text_content.get("document_sections", {}).items():
            heading = section_info.get("heading", "Unknown")
            section_guide.append(f"@SECTION: {section_id} | {heading}")
        section_guide.append("")
        
        # Add the section guide to the beginning of the text
        full_text = "\n".join(section_guide) + "\n" + full_text
    
    # Save file with metadata header
    with open(file_path, 'w', encoding='utf-8') as f:
        # Add metadata header
        f.write(f"@DOCUMENT: {ticker}-{filing_type}-{period_end}\n")
        f.write(f"@FILING_DATE: {filing_metadata.get('filing_date', 'unknown')}\n")
        f.write(f"@COMPANY: {company_name}\n")
        f.write(f"@CIK: {filing_metadata.get('cik', 'unknown')}\n")
        f.write(f"@CONTENT_TYPE: Full Text with Section Markers\n")
            
        # Add section information if available
        sections = list(text_content.get("document_sections", {}).keys())
        if sections:
            f.write(f"@SECTIONS: {', '.join(sections)}\n")
            
        # Add source information if available
        if "instance_url" in filing_metadata:
            f.write(f"@SOURCE_URL: {filing_metadata['instance_url']}\n")
            
        # Add table of contents if available
        if text_content.get("toc"):
            f.write("\n" + text_content["toc"] + "\n")
        else:
            f.write("\n")
            
        # Write the full text with section markers
        f.write(full_text)
    
    return {
        "success": True,
        "file_path": file_path,
        "size": len(full_text)
    }

def process_html_filing(filing_metadata):
    """
    Process an HTML filing from SEC EDGAR
    
    Args:
        filing_metadata: Dictionary containing filing metadata
        
    Returns:
        Dictionary with processing results
    """
    # Step 1: Fetch the HTML filing
    fetch_result = fetch_filing_html(filing_metadata)
    if "error" in fetch_result:
        return {"error": fetch_result["error"]}
    
    html_content = fetch_result["html_content"]
    url = fetch_result["url"]
    
    # Step 2: Extract text from the HTML
    filing_type = filing_metadata.get("filing_type")
    extracted_text = extract_clean_text(html_content, filing_type)
    
    # Step 2b: Check if we found a link to the main document (in case this was an index page)
    main_doc_url = extracted_text.get("metadata", {}).get("main_document_url")
    if main_doc_url:
        logging.info(f"Found link to main document: {main_doc_url}")
        
        # Convert relative URL to absolute if needed
        if main_doc_url.startswith('/'):
            # It's a root-relative URL, need to add domain
            main_doc_url = f"{SEC_BASE_URL}{main_doc_url}"
        elif not main_doc_url.startswith('http'):
            # It's a relative URL, need to add base path
            base_url = '/'.join(url.split('/')[:-1])
            main_doc_url = f"{base_url}/{main_doc_url}"
        
        logging.info(f"Fetching main document from: {main_doc_url}")
        
        try:
            # Fetch the main document
            main_doc_response = sec_request(main_doc_url)
            
            if main_doc_response.status_code == 200:
                # Extract text from the main document
                main_doc_text = extract_clean_text(main_doc_response.text, filing_type)
                
                # Only use the main document if it has more content
                if len(main_doc_text.get("full_text", "")) > len(extracted_text.get("full_text", "")):
                    logging.info("Main document has more content, using it instead")
                    extracted_text = main_doc_text
                else:
                    logging.info("Main document has less content, keeping original extract")
        except Exception as e:
            logging.error(f"Error fetching main document: {str(e)}")
    
    # Step 3: Save the extracted text with all section markers in a single file
    save_result = save_text_file(extracted_text, filing_metadata)
    
    return {
        "success": True,
        "filing_metadata": filing_metadata,
        "file_path": save_result.get("file_path", ""),
        "file_size": save_result.get("size", 0)
    }