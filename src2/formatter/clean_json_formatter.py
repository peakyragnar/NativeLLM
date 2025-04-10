#!/usr/bin/env python3
"""
Clean JSON Formatter Module

Provides a clean JSON formatter that properly separates narrative content and financial data,
similar to the .txt implementation.
"""

import os
import logging
import json
from typing import Dict, Any
from bs4 import BeautifulSoup

from src2.sec.extractor import SECExtractor


class CleanJSONFormatter:
    """
    Clean JSON formatter that properly separates narrative content and financial data,
    similar to the .txt implementation.
    """

    def __init__(self):
        """Initialize the clean JSON formatter"""
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
            "document": {
                "sections": []
            },
            "financial_data": {
                "statements": []
            },
            "xbrl_data": {
                "facts": []
            }
        }

        try:
            # Read HTML file
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Track HTML size
            self.data_integrity["html_size"] = len(html_content)

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

                # Convert filtered sections to list format
                sections_list = []
                for section_id, section_data in filtered_sections.items():
                    # Create a clean section object
                    section = {
                        "id": section_id,
                        "title": section_data.get("heading", section_id)
                    }

                    # Add content if available
                    if "text" in section_data:
                        section["content"] = section_data["text"]

                    sections_list.append(section)

                # Store filtered document sections
                json_data["document"]["sections"] = sections_list
                self.data_integrity["narrative_sections_extracted"] = len(sections_list)

                # Extract XBRL facts
                xbrl_facts = self._extract_facts_from_html(html_path)

                # Store XBRL facts
                json_data["xbrl_data"]["facts"] = xbrl_facts
                self.data_integrity["xbrl_facts_extracted"] = len(xbrl_facts)

                # Organize financial statements
                financial_statements = self._organize_financial_statements(xbrl_facts)

                # Store financial statements
                json_data["financial_data"]["statements"] = financial_statements
                self.data_integrity["financial_statements_created"] = len(financial_statements)
            else:
                # Handle extraction error
                error_message = extract_result.get("error", "Unknown error")
                json_data["error"] = f"Error extracting document sections: {error_message}"
        except Exception as e:
            # Handle general error
            json_data["error"] = f"Error generating JSON format: {str(e)}"

        # Add data integrity metrics
        json_data["data_integrity"] = self.data_integrity

        return json_data

    def _extract_facts_from_html(self, html_path):
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

    def _organize_financial_statements(self, facts):
        """
        Organize XBRL facts into financial statements.

        Args:
            facts: List of XBRL facts

        Returns:
            List of financial statements
        """
        # Define statement types
        BALANCE_SHEET = "BALANCE_SHEET"
        INCOME_STATEMENT = "INCOME_STATEMENT"
        CASH_FLOW_STATEMENT = "CASH_FLOW_STATEMENT"
        EQUITY_STATEMENT = "EQUITY_STATEMENT"
        OTHER = "OTHER"

        # Define section types
        ASSETS = "ASSETS"
        LIABILITIES = "LIABILITIES"
        EQUITY = "EQUITY"
        REVENUE = "REVENUE"
        EXPENSES = "EXPENSES"
        INCOME = "INCOME"
        OPERATING = "OPERATING"
        INVESTING = "INVESTING"
        FINANCING = "FINANCING"
        OTHER_SECTION = "OTHER"

        # Initialize facts by statement type
        facts_by_statement = {
            BALANCE_SHEET: [],
            INCOME_STATEMENT: [],
            CASH_FLOW_STATEMENT: [],
            EQUITY_STATEMENT: [],
            OTHER: []
        }

        # Initialize facts by section
        facts_by_section = {
            BALANCE_SHEET: {
                ASSETS: [],
                LIABILITIES: [],
                EQUITY: [],
                OTHER_SECTION: []
            },
            INCOME_STATEMENT: {
                REVENUE: [],
                EXPENSES: [],
                INCOME: [],
                OTHER_SECTION: []
            },
            CASH_FLOW_STATEMENT: {
                OPERATING: [],
                INVESTING: [],
                FINANCING: [],
                OTHER_SECTION: []
            },
            EQUITY_STATEMENT: {
                OTHER_SECTION: []
            },
            OTHER: {
                OTHER_SECTION: []
            }
        }

        # Organize facts by statement type and section
        for fact in facts:
            concept = fact.get("name", "").lower()

            # Determine statement type and section
            statement_type = OTHER
            section_type = OTHER_SECTION

            # Balance sheet concepts
            if any(term in concept for term in ["asset", "liability", "equity", "debt", "loan", "deposit", "inventory", "receivable", "payable", "cash and cash equivalent"]):
                statement_type = BALANCE_SHEET
                if any(term in concept for term in ["asset", "inventory", "receivable", "cash and cash equivalent"]):
                    section_type = ASSETS
                elif any(term in concept for term in ["liability", "debt", "loan", "payable"]):
                    section_type = LIABILITIES
                elif "equity" in concept:
                    section_type = EQUITY

            # Income statement concepts
            elif any(term in concept for term in ["revenue", "income", "expense", "cost", "tax", "profit", "loss", "earning", "sale"]):
                statement_type = INCOME_STATEMENT
                if any(term in concept for term in ["revenue", "sale"]):
                    section_type = REVENUE
                elif any(term in concept for term in ["expense", "cost"]):
                    section_type = EXPENSES
                elif any(term in concept for term in ["income", "profit", "loss", "earning", "tax"]):
                    section_type = INCOME

            # Cash flow statement concepts
            elif any(term in concept for term in ["cash flow", "cash and cash equivalent", "operating", "investing", "financing"]):
                statement_type = CASH_FLOW_STATEMENT
                if "operating" in concept:
                    section_type = OPERATING
                elif "investing" in concept:
                    section_type = INVESTING
                elif "financing" in concept:
                    section_type = FINANCING

            # Equity statement concepts
            elif any(term in concept for term in ["stockholder", "shareholder", "stock", "share", "dividend", "capital"]):
                statement_type = EQUITY_STATEMENT

            # Add fact to statement type and section
            facts_by_statement[statement_type].append(fact)
            facts_by_section[statement_type][section_type].append(fact)

        # Log the facts by statement type
        for statement_type, statement_facts in facts_by_statement.items():
            logging.info(f"Statement type {statement_type}: {len(statement_facts)} facts")

        # Create each statement type
        statements = []

        # Process each statement type
        for statement_type in [
            BALANCE_SHEET,
            INCOME_STATEMENT,
            CASH_FLOW_STATEMENT,
            EQUITY_STATEMENT
        ]:
            statement_facts = facts_by_statement.get(statement_type, [])
            if statement_facts:
                # Create statement structure
                statement = {
                    "type": statement_type,
                    "title": self._get_statement_title(statement_type),
                    "contexts": [],
                    "facts": []
                }

                # Get facts by section
                sections_for_statement = facts_by_section.get(statement_type, {})

                # Add sections to statement
                sections = []
                for section_name, section_facts in sections_for_statement.items():
                    if section_facts:
                        # Create section
                        section = {
                            "name": section_name,
                            "facts": []
                        }

                        # Add facts to section
                        for fact in section_facts:
                            # Create a clean fact object
                            clean_fact = {
                                "concept": fact.get("name", ""),
                                "value": fact.get("value", ""),
                                "context_ref": fact.get("contextRef", ""),
                                "unit_ref": fact.get("unitRef", "")
                            }

                            # Add to section facts
                            section["facts"].append(clean_fact)

                        # Add section to sections
                        sections.append(section)

                # Add sections to statement
                statement["sections"] = sections

                # Add all facts to statement
                for fact in statement_facts:
                    # Create a clean fact object
                    clean_fact = {
                        "concept": fact.get("name", ""),
                        "value": fact.get("value", ""),
                        "context_ref": fact.get("contextRef", ""),
                        "unit_ref": fact.get("unitRef", "")
                    }

                    # Add to statement facts
                    statement["facts"].append(clean_fact)

                # Add statement to statements
                statements.append(statement)

        return statements

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

    def _get_statement_title(self, statement_type):
        """
        Get a human-readable title for a statement type

        Args:
            statement_type: Statement type

        Returns:
            Human-readable title
        """
        titles = {
            "BALANCE_SHEET": "Balance Sheet",
            "INCOME_STATEMENT": "Income Statement",
            "CASH_FLOW_STATEMENT": "Cash Flow Statement",
            "EQUITY_STATEMENT": "Statement of Shareholders' Equity",
            "OTHER": "Other Financial Information"
        }

        return titles.get(statement_type, statement_type)

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
clean_json_formatter = CleanJSONFormatter()
