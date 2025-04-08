#!/usr/bin/env python3
"""
Filtered JSON Formatter Module

Provides a JSON formatter that properly separates narrative content and financial data,
similar to the .txt implementation, with specific filtering of financial tables from narrative sections.
"""

import os
import logging
import json
import re
from typing import Dict, Any, List
from bs4 import BeautifulSoup

from src2.sec.extractor import SECExtractor


class FilteredJSONFormatter:
    """
    Filtered JSON formatter that properly separates narrative content and financial data,
    with specific filtering of financial tables from narrative sections.
    """

    def __init__(self):
        """Initialize the filtered JSON formatter"""
        self.sec_extractor = SECExtractor()
        self.data_integrity = {
            "html_size": 0,
            "narrative_sections_extracted": 0,
            "narrative_paragraphs": 0,
            "financial_tables_filtered": 0,
            "xbrl_facts_extracted": 0,
            "financial_statements_created": 0
        }
        self.section_to_readable_name = {
            # Form 10-K Items
            "ITEM_1_BUSINESS": "Business",
            "ITEM_1A_RISK_FACTORS": "Risk Factors",
            "ITEM_1B_UNRESOLVED_STAFF_COMMENTS": "Unresolved Staff Comments",
            "ITEM_1C_CYBERSECURITY": "Cybersecurity",
            "ITEM_2_PROPERTIES": "Properties",
            "ITEM_3_LEGAL_PROCEEDINGS": "Legal Proceedings",
            "ITEM_4_MINE_SAFETY": "Mine Safety Disclosures",
            "ITEM_5_MARKET": "Market for Registrant's Common Equity and Related Stockholder Matters",
            "ITEM_6_SELECTED_FINANCIAL": "Selected Financial Data",
            "ITEM_7_MD_AND_A": "Management's Discussion and Analysis",
            "ITEM_7A_MARKET_RISK": "Quantitative and Qualitative Disclosures About Market Risk",
            "ITEM_8_FINANCIAL_STATEMENTS": "Financial Statements and Supplementary Data",
            "ITEM_9_DISAGREEMENTS": "Changes in and Disagreements with Accountants",
            "ITEM_9A_CONTROLS": "Controls and Procedures",
            "ITEM_9B_OTHER_INFORMATION": "Other Information",
            "ITEM_9C_FOREIGN_JURISDICTIONS": "Disclosure Regarding Foreign Jurisdictions",
            "ITEM_10_DIRECTORS": "Directors, Executive Officers and Corporate Governance",
            "ITEM_11_EXECUTIVE_COMPENSATION": "Executive Compensation",
            "ITEM_12_SECURITY_OWNERSHIP": "Security Ownership of Certain Beneficial Owners",
            "ITEM_13_RELATIONSHIPS": "Certain Relationships and Related Transactions",
            "ITEM_14_ACCOUNTANT_FEES": "Principal Accountant Fees and Services",
            "ITEM_15_EXHIBITS": "Exhibits, Financial Statement Schedules",
            "ITEM_16_SUMMARY": "Form 10-K Summary",
            # Form 10-Q Items
            "PART_I": "Part I - Financial Information",
            "PART_II": "Part II - Other Information",
            "ITEM_1_FINANCIAL_STATEMENTS": "Financial Statements",
            "ITEM_2_MD_AND_A": "Management's Discussion and Analysis",
            "ITEM_3_MARKET_RISK": "Quantitative and Qualitative Disclosures About Market Risk",
            "ITEM_4_CONTROLS": "Controls and Procedures",
            "ITEM_1_LEGAL": "Legal Proceedings",
            "ITEM_1A_RISK": "Risk Factors",
            "ITEM_2_UNREGISTERED_SALES": "Unregistered Sales of Equity Securities",
            "ITEM_3_DEFAULTS": "Defaults Upon Senior Securities",
            "ITEM_4_MINE_SAFETY": "Mine Safety Disclosures",
            "ITEM_5_OTHER": "Other Information",
            "ITEM_6_EXHIBITS": "Exhibits",
            # Financial statement sections
            "BALANCE_SHEETS": "Balance Sheets",
            "INCOME_STATEMENTS": "Income Statements",
            "CASH_FLOW_STATEMENTS": "Cash Flow Statements",
            "EQUITY_STATEMENTS": "Statements of Shareholders' Equity",
            "NOTES_TO_FINANCIAL_STATEMENTS": "Notes to Financial Statements"
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
                    # Skip financial statement sections entirely
                    if self._is_financial_section(section_id):
                        continue

                    # For PART_I section, which often contains financial statements at the end,
                    # we need to extract only the actual Part I content
                    if section_id == "PART_I" and "text" in section_data:
                        # Look for the start of Part I
                        part_i_match = re.search(r'PART\s+I', section_data["text"], re.IGNORECASE)
                        if part_i_match:
                            # Extract only the content from the start of Part I to the start of Item 15
                            item_15_match = re.search(r'ITEM\s+15\.', section_data["text"], re.IGNORECASE)
                            if item_15_match:
                                # Keep only the content before Item 15
                                section_data["text"] = section_data["text"][:item_15_match.start()]

                    # Add the section to filtered sections
                    filtered_sections[section_id] = section_data

                # Convert filtered sections to list format with filtered content
                sections_list = []
                for section_id, section_data in filtered_sections.items():
                    # Create a clean section object
                    section = {
                        "id": section_id,
                        "title": section_data.get("heading", self.section_to_readable_name.get(section_id, section_id))
                    }

                    # Add filtered content if available
                    if "text" in section_data:
                        # Filter out financial tables from the content
                        filtered_content = self._filter_financial_tables(section_data["text"], section_id)
                        section["content"] = filtered_content

                    sections_list.append(section)

                # Store filtered document sections
                json_data["document"]["sections"] = sections_list
                self.data_integrity["narrative_sections_extracted"] = len(sections_list)

                # Extract XBRL facts and contexts
                xbrl_facts, xbrl_contexts = self._extract_facts_from_html(html_path)

                # Store XBRL facts and contexts
                json_data["xbrl_data"]["facts"] = xbrl_facts
                json_data["xbrl_data"]["contexts"] = xbrl_contexts
                self.data_integrity["xbrl_facts_extracted"] = len(xbrl_facts)
                self.data_integrity["xbrl_contexts_extracted"] = len(xbrl_contexts)

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

    def _filter_financial_tables(self, text: str, section_id: str) -> str:
        """
        Filter out financial tables and financial statement sections from text content

        Args:
            text: Text content to filter
            section_id: Section ID for logging

        Returns:
            Filtered text content
        """
        # If this is already a financial statement section, remove it entirely
        if self._is_financial_section(section_id):
            logging.info(f"Removing entire financial section: {section_id}")
            self.data_integrity["financial_sections_removed"] = self.data_integrity.get("financial_sections_removed", 0) + 1
            return ""  # Remove the entire section

        # Define financial statement markers to identify financial sections within narrative content
        financial_statement_markers = [
            "CONSOLIDATED STATEMENTS OF INCOME",
            "CONSOLIDATED BALANCE SHEETS",
            "CONSOLIDATED STATEMENTS OF CASH FLOWS",
            "CONSOLIDATED STATEMENTS OF SHAREHOLDERS",
            "CONSOLIDATED STATEMENTS OF STOCKHOLDERS",
            "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME",
            "NOTES TO THE CONSOLIDATED FINANCIAL STATEMENTS",
            "Report of Independent Registered Public Accounting Firm"
        ]

        # Split text into paragraphs
        paragraphs = re.split(r'\n\s*\n', text)

        # Separate tables and narrative text
        narrative_paragraphs = []
        financial_tables_filtered = 0

        # Detect tables in paragraphs
        for paragraph in paragraphs:
            # Skip paragraphs that look like financial statements
            if any(marker in paragraph for marker in financial_statement_markers):
                financial_tables_filtered += 1
                continue

            # Check for financial statement headers
            if re.search(r'(CONSOLIDATED|STATEMENT OF|BALANCE SHEET|INCOME STATEMENT|CASH FLOW|NOTES TO)', paragraph, re.IGNORECASE):
                financial_tables_filtered += 1
                continue

            # Table detection logic
            is_table = False

            # Check for table markers
            if "|" in paragraph and len(paragraph) > 50:
                is_table = True
            elif paragraph.count("\t") > 3:
                is_table = True
            elif paragraph.count(",") > 5 and any(str(year) in paragraph for year in range(2018, 2025)):
                is_table = True
            elif "$" in paragraph and any(str(year) in paragraph for year in range(2018, 2025)):
                is_table = True

            # Check for financial terms in tables or paragraphs
            financial_terms = [
                "balance sheet", "income statement", "cash flow", "statement of operations",
                "revenue", "expense", "asset", "liability", "equity", "earnings",
                "profit", "loss", "margin", "ratio", "fiscal year", "quarter",
                "consolidated", "financial position", "cash and cash equivalent",
                "in millions", "in thousands", "stockholders", "shareholders",
                "net income", "total assets", "total liabilities"
            ]

            if is_table and any(financial_term in paragraph.lower() for financial_term in financial_terms):
                financial_tables_filtered += 1
                continue

            # Also filter out paragraphs that contain dollar amounts and look like financial data
            if "$" in paragraph and any(financial_term in paragraph.lower() for financial_term in financial_terms):
                financial_tables_filtered += 1
                continue

            # Keep non-table paragraphs or non-financial tables
            if len(paragraph) >= 10:  # Only keep substantive paragraphs
                narrative_paragraphs.append(paragraph)
                self.data_integrity["narrative_paragraphs"] += 1

        # Update data integrity metrics
        self.data_integrity["financial_tables_filtered"] += financial_tables_filtered

        # Join filtered paragraphs
        filtered_text = "\n\n".join(narrative_paragraphs)

        return filtered_text

    def _extract_facts_from_html(self, html_path):
        """
        Extract XBRL facts and contexts from an HTML file with inline XBRL.

        Args:
            html_path: Path to the HTML file

        Returns:
            Tuple of (facts, contexts) where facts is a list of XBRL facts (dictionaries)
            and contexts is a dictionary of context IDs to context data
        """
        logging.info(f"Extracting facts from {html_path}")

        # Check if there's a raw XBRL file
        xbrl_raw_path = os.path.join(os.path.dirname(html_path), '_xbrl_raw.json')
        if os.path.exists(xbrl_raw_path):
            # Load raw XBRL data
            with open(xbrl_raw_path, 'r', encoding='utf-8') as f:
                xbrl_data = json.load(f)

            # If it's a list, it's a list of facts
            if isinstance(xbrl_data, list):
                facts = xbrl_data
                logging.info(f"Loaded {len(facts)} facts from raw XBRL file")
            else:
                # If it's a dictionary, it might have facts and contexts
                facts = xbrl_data.get('facts', [])
                logging.info(f"Loaded {len(facts)} facts from raw XBRL file")
        else:
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

        # Extract contexts from fact context references
        contexts = {}

        # Collect all unique context references
        context_refs = set()
        for fact in facts:
            context_ref = fact.get('contextRef')
            if context_ref:
                context_refs.add(context_ref)

        # Parse context references to extract period information
        for context_ref in context_refs:
            # Try to extract period information from context reference
            context_data = self._parse_context_ref(context_ref)
            if context_data:
                contexts[context_ref] = context_data

        logging.info(f"Extracted {len(facts)} facts and {len(contexts)} contexts")
        return facts, contexts

    def _parse_context_ref(self, context_ref):
        """
        Parse a context reference to extract period information.

        Args:
            context_ref: Context reference string

        Returns:
            Dictionary with period information, or None if parsing fails
        """
        # Common patterns in context references
        instant_pattern = r'_I(\d{8})$'  # e.g., _I20230129
        duration_pattern = r'_D(\d{8})-(\d{8})$'  # e.g., _D20220131-20230129

        # Try to match instant pattern
        instant_match = re.search(instant_pattern, context_ref)
        if instant_match:
            date_str = instant_match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            return {
                'period': {
                    'instant': formatted_date
                },
                'entity': {
                    'identifier': 'NVDA'
                }
            }

        # Try to match duration pattern
        duration_match = re.search(duration_pattern, context_ref)
        if duration_match:
            start_date_str = duration_match.group(1)
            end_date_str = duration_match.group(2)
            formatted_start_date = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
            formatted_end_date = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
            return {
                'period': {
                    'startDate': formatted_start_date,
                    'endDate': formatted_end_date
                },
                'entity': {
                    'identifier': 'NVDA'
                }
            }

        # If no pattern matches, return a basic context
        return {
            'period': {},
            'entity': {
                'identifier': 'NVDA'
            }
        }

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
filtered_json_formatter = FilteredJSONFormatter()
