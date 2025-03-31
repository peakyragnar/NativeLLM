"""
HTML Processor Module

Responsible for processing HTML files to extract clean text.
"""

import os
import re
import logging
from bs4 import BeautifulSoup
from .html_optimizer import html_optimizer

class HTMLProcessor:
    """
    Process HTML files from SEC filings with advanced section recognition
    """
    
    def __init__(self):
        """
        Initialize HTML processor
        """
        pass
    
    def extract_text_from_filing(self, html_file_path, ticker=None, filing_type=None):
        """
        Extract clean text from an HTML filing
        
        Args:
            html_file_path: Path to the HTML file
            ticker: Optional ticker symbol for context
            filing_type: Optional filing type for context
            
        Returns:
            Dict with extracted text data
        """
        logging.info(f"Extracting text from HTML file: {html_file_path}")
        
        try:
            # Read HTML file
            with open(html_file_path, 'r', encoding='utf-8', errors='replace') as f:
                html_content = f.read()
            
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract text with sections
            return self.extract_clean_text(html_content, filing_type)
            
        except Exception as e:
            logging.error(f"Error extracting text from HTML: {str(e)}")
            return {"error": f"Error extracting text from HTML: {str(e)}"}

    def extract_clean_text(self, html_content, filing_type):
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
        self.extract_document_metadata(soup, sections)
        
        # Find the main document content
        main_content = self.find_main_content(soup)
        
        if main_content:
            # Clean up content - remove scripts, styles but keep structure
            for script in main_content(['script', 'style']):
                script.extract()
            
            # Extract the table of contents if present
            toc = self.extract_table_of_contents(main_content)
            if toc:
                sections["toc"] = toc
            
            # Identify and mark standard SEC document sections
            self.identify_and_mark_sections(main_content, sections, filing_type)
            
            # Get the full text with section markers
            full_text_with_markers = self.get_text_with_section_markers(main_content, sections.get("document_sections", {}))
            sections["full_text"] = full_text_with_markers
        else:
            # Last resort - use entire HTML
            logging.info("No main content identified, using entire HTML")
            for script in soup(['script', 'style']):
                script.extract()
            sections["full_text"] = self.clean_text(soup.get_text())

        # Filter out any XBRL context identifiers at the beginning of content
        sections["full_text"] = self.filter_xbrl_identifiers(sections["full_text"])
        
        return sections
    
    def extract_document_metadata(self, soup, sections):
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
    
    def find_main_content(self, soup):
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
    
    def extract_table_of_contents(self, content):
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
                toc_text = "@TABLE_OF_CONTENTS\n" + self.clean_text(toc_table.get_text())
                return toc_text
        
        return ""
    
    def identify_and_mark_sections(self, content, sections, filing_type):
        """
        Identify standard SEC filing sections and mark them in the document
        
        Args:
            content: BeautifulSoup object of the main content
            sections: Dictionary to populate with section info
            filing_type: Type of filing (10-K, 10-Q)
        """
        # Standard section IDs and their titles for each filing type
        standard_sections = {}
        
        if filing_type == "10-K":
            standard_sections = {
                'ITEM_1_BUSINESS': 'Business',
                'ITEM_1A_RISK_FACTORS': 'Risk Factors',
                'ITEM_1B_UNRESOLVED_STAFF_COMMENTS': 'Unresolved Staff Comments',
                'ITEM_1C_CYBERSECURITY': 'Cybersecurity',
                'ITEM_2_PROPERTIES': 'Properties',
                'ITEM_3_LEGAL_PROCEEDINGS': 'Legal Proceedings',
                'ITEM_4_MINE_SAFETY_DISCLOSURES': 'Mine Safety Disclosures',
                'ITEM_5_MARKET': 'Market for Registrant\'s Common Equity',
                'ITEM_6_SELECTED_FINANCIAL_DATA': 'Selected Financial Data',
                'ITEM_7_MD_AND_A': 'Management\'s Discussion and Analysis',
                'ITEM_7A_MARKET_RISK': 'Quantitative and Qualitative Disclosures About Market Risk',
                'ITEM_8_FINANCIAL_STATEMENTS': 'Financial Statements and Supplementary Data',
                'ITEM_9_DISAGREEMENTS': 'Changes in and Disagreements With Accountants',
                'ITEM_9A_CONTROLS': 'Controls and Procedures',
                'ITEM_9B_OTHER_INFORMATION': 'Other Information',
                'ITEM_9C_FOREIGN_JURISDICTIONS': 'Disclosure Regarding Foreign Jurisdictions',
                'ITEM_10_DIRECTORS': 'Directors, Executive Officers and Corporate Governance',
                'ITEM_11_EXECUTIVE_COMPENSATION': 'Executive Compensation',
                'ITEM_12_SECURITY_OWNERSHIP': 'Security Ownership of Certain Beneficial Owners',
                'ITEM_13_RELATIONSHIPS': 'Certain Relationships and Related Transactions',
                'ITEM_14_ACCOUNTANT_FEES': 'Principal Accountant Fees and Services',
                'ITEM_15_EXHIBITS': 'Exhibits, Financial Statement Schedules',
                'ITEM_16_SUMMARY': 'Form 10-K Summary'
            }
        elif filing_type == "10-Q":
            standard_sections = {
                'ITEM_1_FINANCIAL_STATEMENTS': 'Financial Statements',
                'ITEM_2_MD_AND_A': 'Management\'s Discussion and Analysis',
                'ITEM_3_MARKET_RISK': 'Quantitative and Qualitative Disclosures About Market Risk',
                'ITEM_4_CONTROLS': 'Controls and Procedures',
                'ITEM_1_LEGAL_PROCEEDINGS': 'Legal Proceedings',
                'ITEM_1A_RISK_FACTORS': 'Risk Factors',
                'ITEM_2_UNREGISTERED_SALES': 'Unregistered Sales of Equity Securities',
                'ITEM_3_DEFAULTS': 'Defaults Upon Senior Securities',
                'ITEM_4_MINE_SAFETY': 'Mine Safety Disclosures',
                'ITEM_5_OTHER_INFORMATION': 'Other Information',
                'ITEM_6_EXHIBITS': 'Exhibits'
            }
        else:
            # Generic sections for other filing types
            standard_sections = {
                'FINANCIAL_STATEMENTS': 'Financial Statements',
                'NOTES_TO_FINANCIAL_STATEMENTS': 'Notes to Financial Statements',
                'MANAGEMENT_DISCUSSION': 'Management\'s Discussion and Analysis',
                'RISK_FACTORS': 'Risk Factors'
            }
        
        # Basic financial statement sections for any filing type
        financial_sections = {
            'CONSOLIDATED_BALANCE_SHEET': 'Consolidated Balance Sheet',
            'CONSOLIDATED_INCOME_STATEMENT': 'Consolidated Income Statement',
            'CONSOLIDATED_CASH_FLOW': 'Consolidated Cash Flow Statement',
            'CONSOLIDATED_EQUITY': 'Consolidated Statement of Stockholders\' Equity',
            'CONSOLIDATED_COMPREHENSIVE_INCOME': 'Consolidated Statement of Comprehensive Income',
            'CONTROLS_AND_PROCEDURES': 'Controls and Procedures',
            'CRITICAL_ACCOUNTING': 'Critical Accounting Policies',
            'FORWARD_LOOKING': 'Forward-Looking Statements',
            'LIQUIDITY_AND_CAPITAL': 'Liquidity and Capital Resources',
            'RESULTS_OF_OPERATIONS': 'Results of Operations',
            'SIGNIFICANT_ACCOUNTING_POLICIES': 'Significant Accounting Policies'
        }
        
        # Combine standard sections with financial sections
        all_sections = {**standard_sections, **financial_sections}
        
        # Initialize document_sections if not present
        if "document_sections" not in sections:
            sections["document_sections"] = {}
        
        # First try to extract Table of Contents to identify all sections present
        toc_sections = self.extract_sections_from_toc(content, filing_type)
        if toc_sections:
            logging.info(f"Found {len(toc_sections)} sections in Table of Contents")
            # Add the sections from TOC to document_sections
            for section_id, section_info in toc_sections.items():
                if section_id in all_sections:
                    sections["document_sections"][section_id] = section_info
        
        # Generate regex patterns for each standard section
        section_patterns = []
        
        # For 10-K sections, create patterns that match different variations of section headers
        for section_id, section_title in standard_sections.items():
            # Extract the item number from the section_id (e.g., "1" from "ITEM_1_BUSINESS")
            if section_id.startswith('ITEM_'):
                parts = section_id.split('_')
                if len(parts) > 1 and parts[1].isdigit():
                    item_number = parts[1]
                    item_letter = parts[2][0] if len(parts) > 2 and parts[2] and parts[2][0].isalpha() else ''
                    
                    # Create pattern with and without period after the number
                    # Also handle both cases (ITEM vs Item) and various punctuation
                    pattern = r'(?:[Ii]tem|ITEM)\s+' + item_number
                    if item_letter:
                        pattern += r'(?:' + item_letter + r'|' + item_letter.upper() + r')\.?\s*'
                    
                    # Add keywords from the title for additional matching
                    title_keywords = section_title.split()
                    if title_keywords:
                        main_keyword = title_keywords[0]
                        pattern += r'(?:.*?' + main_keyword + r'|:[^.]*?' + main_keyword + r')'
                    
                    section_patterns.append((pattern, section_id))
            else:
                # For non-ITEM sections, use the title directly
                section_patterns.append((section_title.replace("'", "['\"]?"), section_id))
        
        # Add financial section patterns
        for section_id, section_title in financial_sections.items():
            pattern = section_title.replace("'", "['\"]?").replace(" ", r"\s+")
            section_patterns.append((pattern, section_id))
        
        # Find all potential section headers in the document
        all_text_elements = content.find_all(['h1', 'h2', 'h3', 'h4', 'strong', 'b', 'p', 'div', 'span', 'td'], 
                                           string=lambda text: text and len(text) > 3)
        
        # Track found sections
        found_sections = set()
        
        # Process each text element to check if it's a section header
        for element in all_text_elements:
            element_text = element.get_text().strip()
            
            # Skip very short text or already processed elements
            if len(element_text) < 4:
                continue
            
            # Check if text matches any section pattern
            for pattern, section_id in section_patterns:
                if re.search(pattern, element_text, re.IGNORECASE):
                    # Only add if this section wasn't already found
                    if section_id not in found_sections:
                        sections["document_sections"][section_id] = {
                            "heading": element_text,
                            "element": element
                        }
                        found_sections.add(section_id)
                        logging.info(f"Found section: {section_id} - {element_text}")
                        break
        
        # Additional checks for ITEM identifiers in a specific format (e.g., Item 1., Item 1:, etc.)
        item_pattern = re.compile(r'(?:[Ii]tem|ITEM)\s+(\d+[A-C]?)[\s\.:]', re.IGNORECASE)
        for element in all_text_elements:
            element_text = element.get_text().strip()
            match = item_pattern.search(element_text)
            if match:
                item_number = match.group(1)
                # Try to match this to a standard section
                for section_id in standard_sections.keys():
                    if section_id.startswith(f'ITEM_{item_number}') or section_id.startswith(f'ITEM_{item_number}_'):
                        if section_id not in found_sections:
                            sections["document_sections"][section_id] = {
                                "heading": element_text,
                                "element": element
                            }
                            found_sections.add(section_id)
                            logging.info(f"Found section via item number match: {section_id} - {element_text}")
                            break
        
        # Create missing placeholder sections for any missing standard sections
        if toc_sections:
            # If we found a TOC, use those sections as the definitive list
            for section_id in toc_sections.keys():
                if section_id not in sections["document_sections"] and section_id in all_sections:
                    sections["document_sections"][section_id] = {
                        "heading": all_sections[section_id],
                        "element": None,
                        "missing": True
                    }
        else:
            # If no TOC found, include all standard sections as placeholders
            for section_id, section_title in standard_sections.items():
                if section_id not in sections["document_sections"]:
                    sections["document_sections"][section_id] = {
                        "heading": section_title,
                        "element": None,
                        "missing": True
                    }
        
        # Log the total sections found
        logging.info(f"Total sections found: {len(found_sections)} of {len(standard_sections)} standard sections")
        
    def extract_sections_from_toc(self, content, filing_type):
        """
        Extract sections from Table of Contents if present
        
        Args:
            content: BeautifulSoup object of the main content
            filing_type: Type of filing (10-K, 10-Q)
            
        Returns:
            Dictionary of section_id -> section_info mappings or None if TOC not found
        """
        # Look for Table of Contents
        toc_elements = []
        
        # Check for text containing "Table of Contents" or "Contents"
        for element in content.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'div', 'span', 'td'], 
                                      string=lambda text: text and re.search(r'(?:Table\s+of\s+Contents|CONTENTS)', text, re.IGNORECASE)):
            toc_elements.append(element)
        
        if not toc_elements:
            return None
        
        # Find the most likely TOC element
        toc_element = toc_elements[0]
        
        # Try to find the TOC as a list or table after this element
        potential_toc = None
        
        # Look for structured TOCs (tables, lists)
        # 1. Try finding a table
        toc_table = toc_element.find_next('table')
        if toc_table and toc_table.find_all('tr'):
            potential_toc = toc_table
        
        # 2. Try finding a list
        if not potential_toc:
            toc_list = toc_element.find_next(['ul', 'ol'])
            if toc_list and toc_list.find_all('li'):
                potential_toc = toc_list
        
        # 3. Look for a div with multiple paragraphs that might be TOC entries
        if not potential_toc:
            toc_div = toc_element.find_next('div')
            if toc_div and len(toc_div.find_all(['p', 'div'], recursive=False)) > 3:
                potential_toc = toc_div
        
        # If still not found, use a broader search approach
        if not potential_toc:
            # Look for all <p> elements with item entries after the TOC header
            item_pattern = re.compile(r'item\s+\d+[A-C]?', re.IGNORECASE)
            toc_paragraphs = []
            
            # Start looking from the TOC element
            current = toc_element.next_sibling
            while current and len(toc_paragraphs) < 30:  # Limit search space
                if hasattr(current, 'name'):
                    # If we hit a major heading, stop searching
                    if current.name in ['h1', 'h2'] and not item_pattern.search(current.get_text()):
                        break
                    
                    # If this element contains an Item reference, add it
                    if item_pattern.search(current.get_text()):
                        toc_paragraphs.append(current)
                        
                current = current.next_sibling
            
            if toc_paragraphs:
                potential_toc = toc_paragraphs
        
        # If we found something that looks like a TOC, extract the sections
        if potential_toc:
            sections = {}
            item_pattern = re.compile(r'(?:item|ITEM)\s+(\d+[A-C]?)(?:\.|\s|:)', re.IGNORECASE)
            
            # Different approaches based on TOC structure
            if isinstance(potential_toc, list):
                # Process a list of paragraphs
                for p in potential_toc:
                    text = p.get_text().strip()
                    match = item_pattern.search(text)
                    if match:
                        item_num = match.group(1)
                        section_id = self.map_item_to_section_id(item_num, filing_type)
                        if section_id:
                            sections[section_id] = {
                                "heading": text,
                                "element": p
                            }
            elif potential_toc.name == 'table':
                # Process a table
                for row in potential_toc.find_all('tr'):
                    text = row.get_text().strip()
                    match = item_pattern.search(text)
                    if match:
                        item_num = match.group(1)
                        section_id = self.map_item_to_section_id(item_num, filing_type)
                        if section_id:
                            sections[section_id] = {
                                "heading": text,
                                "element": row
                            }
            elif potential_toc.name in ['ul', 'ol']:
                # Process a list
                for item in potential_toc.find_all('li'):
                    text = item.get_text().strip()
                    match = item_pattern.search(text)
                    if match:
                        item_num = match.group(1)
                        section_id = self.map_item_to_section_id(item_num, filing_type)
                        if section_id:
                            sections[section_id] = {
                                "heading": text,
                                "element": item
                            }
            else:
                # Process a div or other container
                for element in potential_toc.find_all(['p', 'div', 'span']):
                    text = element.get_text().strip()
                    match = item_pattern.search(text)
                    if match:
                        item_num = match.group(1)
                        section_id = self.map_item_to_section_id(item_num, filing_type)
                        if section_id:
                            sections[section_id] = {
                                "heading": text,
                                "element": element
                            }
            
            return sections
        
        return None
    
    def map_item_to_section_id(self, item_num, filing_type):
        """
        Maps an item number (e.g., "1", "1A") to a section ID
        
        Args:
            item_num: Item number as string (e.g., "1", "1A")
            filing_type: Type of filing (10-K, 10-Q)
            
        Returns:
            Section ID or None if no mapping found
        """
        item_num = item_num.upper()  # Normalize to uppercase
        
        # 10-K mappings
        if filing_type == "10-K":
            mapping = {
                "1": "ITEM_1_BUSINESS",
                "1A": "ITEM_1A_RISK_FACTORS",
                "1B": "ITEM_1B_UNRESOLVED_STAFF_COMMENTS",
                "1C": "ITEM_1C_CYBERSECURITY",
                "2": "ITEM_2_PROPERTIES",
                "3": "ITEM_3_LEGAL_PROCEEDINGS",
                "4": "ITEM_4_MINE_SAFETY_DISCLOSURES",
                "5": "ITEM_5_MARKET",
                "6": "ITEM_6_SELECTED_FINANCIAL_DATA",
                "7": "ITEM_7_MD_AND_A",
                "7A": "ITEM_7A_MARKET_RISK",
                "8": "ITEM_8_FINANCIAL_STATEMENTS",
                "9": "ITEM_9_DISAGREEMENTS",
                "9A": "ITEM_9A_CONTROLS",
                "9B": "ITEM_9B_OTHER_INFORMATION",
                "9C": "ITEM_9C_FOREIGN_JURISDICTIONS",
                "10": "ITEM_10_DIRECTORS",
                "11": "ITEM_11_EXECUTIVE_COMPENSATION",
                "12": "ITEM_12_SECURITY_OWNERSHIP",
                "13": "ITEM_13_RELATIONSHIPS",
                "14": "ITEM_14_ACCOUNTANT_FEES",
                "15": "ITEM_15_EXHIBITS",
                "16": "ITEM_16_SUMMARY"
            }
        # 10-Q mappings
        elif filing_type == "10-Q":
            mapping = {
                "1": "ITEM_1_FINANCIAL_STATEMENTS",
                "2": "ITEM_2_MD_AND_A",
                "3": "ITEM_3_MARKET_RISK",
                "4": "ITEM_4_CONTROLS",
                "1A": "ITEM_1A_RISK_FACTORS",
                "5": "ITEM_5_OTHER_INFORMATION",
                "6": "ITEM_6_EXHIBITS"
            }
        else:
            return None
        
        return mapping.get(item_num)
    
    def get_text_with_section_markers(self, content, document_sections=None):
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
        base_text = self.clean_text(content_copy.get_text())
        
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
    
    def filter_xbrl_identifiers(self, text):
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
    
    def clean_text(self, text):
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
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        
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
        
        # Replace newlines and tabs with spaces - only if they appear in the middle of sentences
        text = re.sub(r'([^\n])\r?\n([^\n])', r'\1 \2', text)
        text = re.sub(r'\t+', ' ', text)
        
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def extract_sections(self, soup):
        """
        Extract sections from a BeautifulSoup object
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dict with section IDs and content
        """
        sections = {}
        
        # Extract document title
        title = soup.find('title')
        if title:
            sections['title'] = title.get_text(strip=True)
        
        # Extract main content
        main_content = soup.find('body')
        if not main_content:
            main_content = soup
        
        # Look for standard section identifiers
        for section_tag in main_content.find_all(['h1', 'h2', 'h3', 'div'], class_=lambda c: c and ('title' in c.lower() or 'header' in c.lower() or 'section' in c.lower())):
            section_id = section_tag.get_text(strip=True)
            if section_id and len(section_id) > 3:
                # Find content for this section (all siblings until next section)
                section_content = []
                current = section_tag.next_sibling
                
                while current and not (current.name in ['h1', 'h2', 'h3'] and current.get('class') and ('title' in current['class'] or 'header' in current['class'] or 'section' in current['class'])):
                    if hasattr(current, 'get_text'):
                        text = current.get_text(strip=True)
                        if text:
                            section_content.append(text)
                    current = current.next_sibling
                
                sections[section_id] = ' '.join(section_content)
        
        # If no sections found, fallback to simple text extraction
        if len(sections) <= 1:  # Only title or nothing
            text_blocks = []
            
            # Extract all paragraphs
            for p in main_content.find_all(['p', 'div', 'td']):
                text = p.get_text(strip=True)
                if text and len(text) > 10:  # Avoid tiny fragments
                    text_blocks.append(text)
            
            # Group blocks into logical sections
            current_section = None
            current_section_text = []
            
            for block in text_blocks:
                # Check if this block looks like a section header
                if len(block) < 100 and (block.isupper() or re.match(r'^[A-Z][a-z]+(\s+[A-Z][a-z]+){1,5}$', block)):
                    # Save previous section
                    if current_section and current_section_text:
                        sections[current_section] = ' '.join(current_section_text)
                    
                    # Start new section
                    current_section = block
                    current_section_text = []
                else:
                    # Add to current section
                    if current_section:
                        current_section_text.append(block)
                    else:
                        # No section header yet, use generic name
                        if 'content' not in sections:
                            sections['content'] = []
                        if isinstance(sections['content'], list):
                            sections['content'].append(block)
                        else:
                            sections['content'] = [sections['content'], block]
            
            # Save last section
            if current_section and current_section_text:
                sections[current_section] = ' '.join(current_section_text)
            
            # Convert content list to string if needed
            if 'content' in sections and isinstance(sections['content'], list):
                sections['content'] = ' '.join(sections['content'])
        
        return sections
    
    def save_text_file(self, text_data, output_path, filing_metadata=None):
        """
        Save extracted text to a file with enhanced metadata and section markers
        
        Args:
            text_data: Text data from extract_text_from_filing()
            output_path: Path to save the text file
            filing_metadata: Optional filing metadata to include in the file header
            
        Returns:
            Dict with save result
        """
        try:
            # Get the text content - either direct text or full_text from structured output
            text_content = text_data.get('text', text_data.get('full_text', ''))
            sections = text_data.get('document_sections', {})
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save text with metadata header
            with open(output_path, 'w', encoding='utf-8') as f:
                # Add metadata if provided
                if filing_metadata:
                    f.write(f"@DOCUMENT: {filing_metadata.get('ticker', 'unknown')}-")
                    f.write(f"{filing_metadata.get('filing_type', 'unknown')}-")
                    f.write(f"{filing_metadata.get('period_end_date', 'unknown')}\n")
                    f.write(f"@FILING_DATE: {filing_metadata.get('filing_date', 'unknown')}\n")
                    f.write(f"@COMPANY: {filing_metadata.get('company_name', 'unknown')}\n")
                    f.write(f"@CIK: {filing_metadata.get('cik', 'unknown')}\n")
                    f.write(f"@CONTENT_TYPE: Full Text with Section Markers\n\n")
                    
                    # Add section information if available
                    if sections:
                        f.write(f"@SECTIONS: {', '.join(sections.keys())}\n\n")
                
                # Add table of contents if available
                if 'toc' in text_data and text_data['toc']:
                    f.write(text_data['toc'] + "\n\n")
                
                # Write the main text content
                f.write(text_content)
            
            return {
                "success": True,
                "path": output_path,
                "size": os.path.getsize(output_path)
            }
        except Exception as e:
            logging.error(f"Error saving text file: {str(e)}")
            return {"error": f"Error saving text file: {str(e)}"}

# Create a singleton instance
html_processor = HTMLProcessor()