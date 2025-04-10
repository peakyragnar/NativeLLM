#!/usr/bin/env python3
"""
JSON Output Formatter Module

Provides a JSON output formatter that extracts both narrative sections and financial data
directly from HTML/XBRL, similar to the .txt implementation.
"""

import os
import logging
import json
import re
from typing import Dict, List, Any
from bs4 import BeautifulSoup

# Import the SECExtractor
from src2.sec.extractor import SECExtractor

# Define a simple function to extract XBRL facts
def extract_facts_from_html(html_path):
    """
    Extract XBRL facts from an HTML file with inline XBRL.

    Args:
        html_path: Path to the HTML file

    Returns:
        List of XBRL facts (dictionaries)
    """
    logging.info(f"Extracting facts from {html_path}")

    # Load HTML file
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all XBRL facts
    facts = []

    # Find all elements with contextRef attribute (numeric and non-numeric facts)
    fact_elements = soup.find_all(attrs={'contextref': True})

    for element in fact_elements:
        # Extract fact attributes
        name = element.name
        context_ref = element.get('contextref')
        unit_ref = element.get('unitref')
        decimals = element.get('decimals')
        scale = element.get('scale')
        format = element.get('format')

        # For inline XBRL, get the concept name from the name attribute
        concept = element.get('name')

        # Extract fact value
        value = element.get_text(strip=True)

        # Create fact object
        fact = {
            'name': concept if concept else name,  # Use concept name if available
            'element': name,  # Store the element name separately
            'contextRef': context_ref,
            'unitRef': unit_ref,
            'decimals': decimals,
            'scale': scale,
            'format': format,
            'value': value
        }

        # Remove None values
        fact = {k: v for k, v in fact.items() if v is not None}

        facts.append(fact)

    logging.info(f"Extracted {len(facts)} facts")
    return facts


class JSONOutputFormatter:
    """
    JSON output formatter that extracts both narrative sections and financial data
    directly from HTML/XBRL, similar to the .txt implementation.
    """

    def __init__(self):
        """Initialize the JSON output formatter"""
        self.sec_extractor = SECExtractor()
        self.data_integrity = {
            "html_size": 0,
            "narrative_sections_extracted": 0,
            "xbrl_facts_extracted": 0,
            "financial_statements_created": 0
        }

    def generate_json_format(self, html_path: str, filing_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate JSON format from HTML/XBRL document

        Args:
            html_path: Path to HTML/XBRL document
            filing_metadata: Filing metadata

        Returns:
            JSON-formatted content as a dictionary
        """
        # Initialize the JSON structure
        json_data = {
            "metadata": filing_metadata,
            "content": {
                "document_sections": {}
            },
            "xbrl_data": {
                "facts": []
            }
        }

        try:
            # Process the filing using the SEC extractor
            extract_result = self.sec_extractor.process_filing(html_path)

            if extract_result.get("success", False):
                # Get document sections
                document_sections = extract_result.get("document_sections", {})

                # Filter out financial statement sections
                filtered_sections = {}
                for section_id, section_data in document_sections.items():
                    if not self._is_financial_section(section_id):
                        filtered_sections[section_id] = section_data

                # Store filtered document sections
                json_data["content"]["document_sections"] = filtered_sections
                self.data_integrity["narrative_sections_extracted"] = len(filtered_sections)

                # Extract XBRL facts
                xbrl_facts = extract_facts_from_html(html_path)

                # Store XBRL facts
                json_data["xbrl_data"]["facts"] = xbrl_facts
                self.data_integrity["xbrl_facts_extracted"] = len(xbrl_facts)

                # Add data integrity metrics
                json_data["data_integrity"] = self.data_integrity
            else:
                # Handle extraction error
                error_message = extract_result.get("error", "Unknown error")
                json_data["error"] = f"Error extracting document sections: {error_message}"
        except Exception as e:
            # Handle general error
            json_data["error"] = f"Error generating JSON format: {str(e)}"

        return json_data

    def _is_financial_section(self, section_id):
        """
        Check if a section is a financial statement section

        Args:
            section_id: Section ID

        Returns:
            True if the section is a financial statement section, False otherwise
        """
        # List of section IDs that are financial statement sections
        financial_section_ids = [
            "BALANCE_SHEETS", "INCOME_STATEMENTS", "CASH_FLOW_STATEMENTS", "EQUITY_STATEMENTS",
            "STATEMENT_OF_FINANCIAL_POSITION", "STATEMENT_OF_OPERATIONS", "STATEMENT_OF_CASH_FLOWS",
            "STATEMENT_OF_STOCKHOLDERS_EQUITY", "STATEMENT_OF_SHAREHOLDERS_EQUITY",
            "CONSOLIDATED_BALANCE_SHEETS", "CONSOLIDATED_STATEMENTS_OF_INCOME",
            "CONSOLIDATED_STATEMENTS_OF_CASH_FLOWS", "CONSOLIDATED_STATEMENTS_OF_EQUITY",
            "NOTES_TO_FINANCIAL_STATEMENTS", "FINANCIAL_STATEMENTS", "FINANCIAL_STATEMENTS_SECTION",
            "ITEM_8_FINANCIAL_STATEMENTS"
        ]

        # Check if the section ID is in the list of financial section IDs
        if section_id in financial_section_ids:
            return True

        # Check if the section ID contains financial statement keywords
        financial_keywords = [
            "balance sheet", "income statement", "cash flow", "equity", "financial statement",
            "consolidated", "statement of", "notes to"
        ]

        section_id_lower = section_id.lower()
        for keyword in financial_keywords:
            if keyword in section_id_lower:
                return True

        return False

    def save_json_format(self, json_data: Dict[str, Any], output_path: str) -> Dict[str, Any]:
        """
        Save JSON format to a file

        Args:
            json_data: JSON-formatted content as a dictionary
            output_path: Path to save the file

        Returns:
            Dict with save result
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Save file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2)

            return {
                "success": True,
                "path": output_path,
                "size": os.path.getsize(output_path)
            }
        except Exception as e:
            logging.error(f"Error saving JSON format: {str(e)}")
            return {"error": f"Error saving JSON format: {str(e)}"}


# Create a singleton instance
json_output_formatter = JSONOutputFormatter()
