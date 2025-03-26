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