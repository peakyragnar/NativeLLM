#!/usr/bin/env python3
"""
SEC Filing Text Extractor

This module extracts and processes text from rendered SEC iXBRL documents,
with special handling for common filing sections and formatting.
"""

import os
import sys
import logging
import json
import re
import time
from pathlib import Path
from bs4 import BeautifulSoup

class SECExtractor:
    """
    SEC filing text extractor for rendered iXBRL documents.

    This class provides methods to extract structured text content
    from SEC filings, including section identification and formatting.
    """

    def __init__(self, output_dir=None):
        """
        Initialize the SEC filing extractor.

        Args:
            output_dir: Directory to save extracted text files
        """
        # Set up output directory
        if output_dir:
            self.output_dir = Path(output_dir)
            os.makedirs(self.output_dir, exist_ok=True)
        else:
            self.output_dir = None

        logging.info(f"Initialized SEC extractor with output dir: {self.output_dir}")

    def extract_document_sections(self, html_content):
        """
        Extract document sections from HTML content.

        Args:
            html_content: HTML content of rendered document

        Returns:
            Dictionary of document sections
        """
        sections = {}

        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()

            # Extract document title
            title = soup.find('title')
            if title:
                sections['title'] = title.get_text().strip()

            # Look for common 10-K/10-Q section patterns
            item_patterns = [
                # 10-K items
                {'pattern': r'Item\s+1[A]?\.?\s*Business', 'key': 'ITEM_1_BUSINESS'},
                {'pattern': r'Item\s+1A\.?\s*Risk\s+Factors', 'key': 'ITEM_1A_RISK_FACTORS'},
                {'pattern': r'Item\s+1B\.?\s*Unresolved\s+Staff\s+Comments', 'key': 'ITEM_1B_UNRESOLVED_COMMENTS'},
                {'pattern': r'Item\s+2\.?\s*Properties', 'key': 'ITEM_2_PROPERTIES'},
                {'pattern': r'Item\s+3\.?\s*Legal\s+Proceedings', 'key': 'ITEM_3_LEGAL_PROCEEDINGS'},
                {'pattern': r'Item\s+4\.?\s*Mine\s+Safety\s+Disclosures', 'key': 'ITEM_4_MINE_SAFETY'},
                {'pattern': r'Item\s+5\.?\s*Market\s+for\s+Registrant', 'key': 'ITEM_5_MARKET'},
                {'pattern': r'Item\s+6\.?\s*Selected\s+Financial\s+Data', 'key': 'ITEM_6_FINANCIAL_DATA'},
                {'pattern': r'Item\s+7\.?\s*Management.*Discussion', 'key': 'ITEM_7_MD_AND_A'},
                {'pattern': r'Item\s+7A\.?\s*Quantitative\s+and\s+Qualitative', 'key': 'ITEM_7A_MARKET_RISK'},
                {'pattern': r'Item\s+8\.?\s*Financial\s+Statements', 'key': 'ITEM_8_FINANCIAL_STATEMENTS'},
                {'pattern': r'Item\s+9\.?\s*Changes\s+in\s+and\s+Disagreements', 'key': 'ITEM_9_DISAGREEMENTS'},
                {'pattern': r'Item\s+9A\.?\s*Controls\s+and\s+Procedures', 'key': 'ITEM_9A_CONTROLS'},
                {'pattern': r'Item\s+9B\.?\s*Other\s+Information', 'key': 'ITEM_9B_OTHER_INFORMATION'},
                {'pattern': r'Item\s+10\.?\s*Directors', 'key': 'ITEM_10_DIRECTORS'},
                {'pattern': r'Item\s+11\.?\s*Executive\s+Compensation', 'key': 'ITEM_11_EXECUTIVE_COMPENSATION'},
                {'pattern': r'Item\s+12\.?\s*Security\s+Ownership', 'key': 'ITEM_12_SECURITY_OWNERSHIP'},
                {'pattern': r'Item\s+13\.?\s*Certain\s+Relationships', 'key': 'ITEM_13_RELATIONSHIPS'},
                {'pattern': r'Item\s+14\.?\s*Principal\s+Accountant\s+Fees', 'key': 'ITEM_14_ACCOUNTANT_FEES'},
                {'pattern': r'Item\s+15\.?\s*Exhibits', 'key': 'ITEM_15_EXHIBITS'},

                # 10-Q items
                {'pattern': r'Item\s+1\.?\s*Financial\s+Statements', 'key': 'ITEM_1_FINANCIAL_STATEMENTS'},
                {'pattern': r'Item\s+2\.?\s*Management.*Discussion', 'key': 'ITEM_2_MD_AND_A'},
                {'pattern': r'Item\s+3\.?\s*Quantitative\s+and\s+Qualitative', 'key': 'ITEM_3_MARKET_RISK'},
                {'pattern': r'Item\s+4\.?\s*Controls\s+and\s+Procedures', 'key': 'ITEM_4_CONTROLS'},

                # Parts
                {'pattern': r'Part\s+I', 'key': 'PART_I'},
                {'pattern': r'Part\s+II', 'key': 'PART_II'},
                {'pattern': r'Part\s+III', 'key': 'PART_III'},
                {'pattern': r'Part\s+IV', 'key': 'PART_IV'},

                # Financial statement sections
                {'pattern': r'Consolidated\s+Balance\s+Sheets?', 'key': 'BALANCE_SHEETS'},
                {'pattern': r'Consolidated\s+Statements?\s+of\s+Operations', 'key': 'INCOME_STATEMENTS'},
                {'pattern': r'Consolidated\s+Statements?\s+of\s+Cash\s+Flows', 'key': 'CASH_FLOW_STATEMENTS'},
                {'pattern': r'Consolidated\s+Statements?\s+of\s+Stockholders\'\s+Equity', 'key': 'EQUITY_STATEMENTS'},
                {'pattern': r'Notes\s+to\s+.*Financial\s+Statements', 'key': 'NOTES_TO_FINANCIAL_STATEMENTS'}
            ]

            # Find all headings
            headings = []
            for tag in ['h1', 'h2', 'h3', 'h4', 'strong', 'b', 'p', 'div']:
                elements = soup.find_all(tag)
                for element in elements:
                    text = element.get_text().strip()
                    if text and len(text) > 5 and len(text) < 100:  # Filter out too short or too long
                        headings.append({
                            'text': text,
                            'element': element
                        })

            # Match headings to section patterns
            for heading in headings:
                heading_text = heading['text']
                for pattern in item_patterns:
                    if re.search(pattern['pattern'], heading_text, re.IGNORECASE):
                        sections[pattern['key']] = {
                            'heading': heading_text,
                            'element': heading['element']
                        }
                        break

            # Sort sections by their appearance in the document
            sorted_sections = {}
            for key, info in sections.items():
                if key != 'title' and 'element' in info:
                    # Get position in document (line number or index)
                    position = info['element'].sourceline if hasattr(info['element'], 'sourceline') else 0
                    sorted_sections[key] = {
                        'heading': info['heading'],
                        'position': position
                    }

            # Add document statistics
            sections['stats'] = {
                'word_count': len(soup.get_text().split()),
                'section_count': len(sorted_sections),
                'html_size': len(html_content)
            }

            logging.info(f"Extracted {len(sorted_sections)} document sections")
            return sections

        except Exception as e:
            logging.error(f"Error extracting document sections: {str(e)}")
            return {'error': str(e)}

    def extract_text_with_sections(self, html_content):
        """
        Extract text with section markers from HTML content.

        Args:
            html_content: HTML content of rendered document

        Returns:
            Extracted text with section markers
        """
        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.extract()

            # Extract document sections
            sections = self.extract_document_sections(html_content)

            # Get basic text
            text = soup.get_text()

            # Clean up whitespace
            text = re.sub(r'\n+', '\n', text)  # Remove multiple newlines
            text = re.sub(r'\s+', ' ', text)   # Replace multiple spaces with single space

            # Format with section markers
            formatted_text = []

            # Add document title
            if 'title' in sections:
                formatted_text.append(f"@DOCUMENT: {sections['title']}")
                formatted_text.append("")

            # Extract text by section using element positions
            sorted_sections = {}
            for key, info in sections.items():
                if key != 'title' and key != 'stats' and 'element' in info:
                    # Get position in document (line number or index)
                    position = info['element'].sourceline if hasattr(info['element'], 'sourceline') else 0
                    sorted_sections[key] = {
                        'heading': info['heading'],
                        'position': position
                    }

            # Sort sections by position
            sorted_sections = dict(sorted(sorted_sections.items(), key=lambda x: x[1]['position']))

            # Add section guide
            formatted_text.append("@SECTION_GUIDE")
            for key, info in sorted_sections.items():
                formatted_text.append(f"  {key}: {info['heading']}")
            formatted_text.append("")

            # Get full text (cleaned)
            full_text = soup.get_text()
            lines = (line.strip() for line in full_text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            full_text = '\n'.join(chunk for chunk in chunks if chunk)

            # Add full text
            formatted_text.append(full_text)

            return '\n'.join(formatted_text)

        except Exception as e:
            logging.error(f"Error extracting text with sections: {str(e)}")
            return f"ERROR: {str(e)}"

    def process_filing(self, html_path, metadata=None, return_content=True):
        """
        Process an SEC filing and extract text with sections.

        Args:
            html_path: Path to HTML file
            metadata: Optional filing metadata
            return_content: Always True to return content (parameter kept for backward compatibility)

        Returns:
            Dictionary with processing results
        """
        try:
            # Read HTML file
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Extract sections for use by the LLM formatter
            document_sections = self.extract_document_sections(html_content)

            # Extract text with sections
            extracted_text = self.extract_text_with_sections(html_content)

            # Add metadata header if provided
            if metadata:
                metadata_header = []
                metadata_header.append(f"@FILING_TYPE: {metadata.get('filing_type', 'UNKNOWN')}")
                metadata_header.append(f"@COMPANY: {metadata.get('company_name', 'UNKNOWN')}")
                metadata_header.append(f"@CIK: {metadata.get('cik', 'UNKNOWN')}")
                metadata_header.append(f"@FILING_DATE: {metadata.get('filing_date', 'UNKNOWN')}")
                metadata_header.append(f"@PERIOD_END_DATE: {metadata.get('period_end_date', 'UNKNOWN')}")
                metadata_header.append(f"@EXTRACTION_DATE: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                metadata_header.append(f"@SOURCE_URL: {metadata.get('source_url', 'UNKNOWN')}")
                metadata_header.append("")

                extracted_text = '\n'.join(metadata_header) + '\n' + extracted_text

            # Estimate the content size without writing to file
            file_size = len(extracted_text.encode('utf-8'))
            logging.info(f"Extracted content size: {file_size} bytes")

            # Prepare narrative sections for LLM formatter
            processed_sections = {}

            # Extract text for each section
            soup = BeautifulSoup(html_content, 'html.parser')
            raw_text = soup.get_text()

            # Process sections that have elements with text content
            for key, info in document_sections.items():
                if key not in ('title', 'stats') and 'element' in info:
                    # Get the element for this section
                    section_element = info['element']

                    # Extract text from this element and siblings until next section
                    section_text = ""

                    try:
                        # Get the section text
                        next_sibling = section_element.next_sibling
                        while next_sibling:
                            # Add text from this sibling
                            if hasattr(next_sibling, 'get_text'):
                                sibling_text = next_sibling.get_text().strip()
                                if sibling_text:
                                    section_text += sibling_text + "\n\n"
                            # Move to next sibling
                            next_sibling = next_sibling.next_sibling

                            # Stop at next section header
                            if hasattr(next_sibling, 'name') and next_sibling.name in ['h1', 'h2', 'h3', 'h4'] and len(next_sibling.get_text().strip()) > 5:
                                break

                        # Add section to processed sections
                        if section_text:
                            processed_sections[key] = {
                                "heading": info['heading'],
                                "text": section_text.strip()
                            }
                            logging.info(f"Extracted section text for {key} ({len(section_text)} chars)")
                    except Exception as e:
                        logging.warning(f"Error extracting text for section {key}: {str(e)}")

            # If we couldn't extract sections properly, try to get some key sections from the raw text
            if len(processed_sections) < 2:
                logging.info("Using fallback method to extract narrative sections from raw text")

                # Define commonly used section titles and patterns
                section_patterns = {
                    "ITEM_1_BUSINESS": r"(?:ITEM|Item)\s+1\.?\s*Business(.*?)(?:ITEM|Item)\s+1A",
                    "ITEM_1A_RISK_FACTORS": r"(?:ITEM|Item)\s+1A\.?\s*Risk\s+Factors(.*?)(?:ITEM|Item)\s+1B",
                    "ITEM_7_MD_AND_A": r"(?:ITEM|Item)\s+7\.?\s*Management.*Discussion(.*?)(?:ITEM|Item)\s+7A",
                    "ITEM_2_MD_AND_A": r"(?:ITEM|Item)\s+2\.?\s*Management.*Discussion(.*?)(?:ITEM|Item)\s+3"
                }

                for section_id, pattern in section_patterns.items():
                    match = re.search(pattern, raw_text, re.DOTALL | re.IGNORECASE)
                    if match and match.group(1):
                        section_text = match.group(1).strip()
                        if len(section_text) > 500:  # Only keep substantial sections
                            readable_name = section_id.replace("_", " ").title()
                            processed_sections[section_id] = {
                                "heading": readable_name,
                                "text": section_text
                            }
                            logging.info(f"Extracted fallback section {section_id} ({len(section_text)} chars)")

            # Log sections found
            logging.info(f"Extracted {len(processed_sections)} document sections with text content")
            for section_id in processed_sections:
                logging.info(f"  - {section_id}: {len(processed_sections[section_id]['text'])} chars")

            return {
                'success': True,
                'file_size': file_size,
                'file_size_mb': file_size / (1024 * 1024),
                'word_count': len(extracted_text.split()),
                'document_sections': processed_sections  # Add sections for LLM formatter
            }

        except Exception as e:
            logging.error(f"Error processing filing: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def extract_inline_xbrl(self, html_path):
        """
        Extract inline XBRL data from an HTML document.

        Args:
            html_path: Path to the HTML file

        Returns:
            List of XBRL facts (dictionaries)
        """
        xbrl_facts = []
        try:
            logging.info(f"Extracting inline XBRL from main document: {html_path}")
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Use lxml-xml parser for XML documents to avoid warnings
            soup = BeautifulSoup(html_content, 'lxml-xml', features='xml')

            # Find all relevant iXBRL tags
            ix_tags = soup.find_all(['ix:nonnumeric', 'ix:nonfraction'])

            if not ix_tags:
                logging.warning("No standard ix:nonNumeric or ix:nonFraction tags found.")
                # Attempt to find tags with namespaces explicitly (might be needed sometimes)
                try:
                    ix_tags = soup.find_all(re.compile(r'.*:nonnumeric$|.*:nonfraction$'))
                    if ix_tags:
                         logging.info(f"Found {len(ix_tags)} tags using namespace wildcard search.")
                    else:
                         logging.warning("Still no iXBRL tags found even with namespace wildcard.")
                except Exception as ns_err:
                    logging.error(f"Error searching with namespace wildcard: {ns_err}")


            for tag in ix_tags:
                fact = {
                    'name': tag.get('name'),
                    'contextRef': tag.get('contextref'),
                    'unitRef': tag.get('unitref'),
                    'scale': tag.get('scale'),
                    'format': tag.get('format'),
                    'value': tag.get_text(strip=True),
                    'tag_name': tag.name # e.g., 'ix:nonfraction'
                }

                # Clean up concept name (remove namespace prefix like 'us-gaap:')
                if fact['name'] and ':' in fact['name']:
                    fact['concept'] = fact['name'].split(':')[-1]
                else:
                     fact['concept'] = fact['name']

                xbrl_facts.append(fact)

            logging.info(f"Found {len(xbrl_facts)} inline XBRL tags")

            # ---- START ADDED/MODIFIED CODE ----
            if xbrl_facts:
                try:
                    # Determine the output path for the raw XBRL JSON
                    html_file_path = Path(html_path)
                    output_json_path = html_file_path.parent / "_xbrl_raw.json"

                    # Save the extracted facts to the JSON file
                    with open(output_json_path, 'w', encoding='utf-8') as f_json:
                        json.dump(xbrl_facts, f_json, indent=2)
                    logging.info(f"Saved raw XBRL data to: {output_json_path}")
                except Exception as json_err:
                    logging.error(f"Failed to save raw XBRL JSON: {str(json_err)}")
            # ---- END ADDED/MODIFIED CODE ----

            return xbrl_facts

        except FileNotFoundError:
            logging.error(f"HTML file not found for XBRL extraction: {html_path}")
            return []
        except Exception as e:
            logging.error(f"Error extracting inline XBRL: {str(e)}", exc_info=True)
            return []


# Example usage
if __name__ == "__main__":
    import argparse

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Parse arguments
    parser = argparse.ArgumentParser(description="Extract text from SEC filings")
    parser.add_argument("input_file", help="Path to HTML file to process")
    parser.add_argument("--output", help="Output text file path")
    parser.add_argument("--metadata", help="Path to JSON file with filing metadata")

    args = parser.parse_args()

    # Load metadata if provided
    metadata = None
    if args.metadata and os.path.exists(args.metadata):
        with open(args.metadata, 'r') as f:
            metadata = json.load(f)

    # Create extractor
    extractor = SECExtractor()

    # Process filing
    result = extractor.process_filing(args.input_file, args.output, metadata)

    # Print result
    if result['success']:
        print(f"\nProcessing complete!")
        print(f"File size: {result['file_size_mb']:.2f} MB")
        print(f"Word count: {result['word_count']}")
    else:
        print(f"\nError processing filing: {result['error']}")