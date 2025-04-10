#!/usr/bin/env python3
"""
Improved JSON Formatter Module

Provides enhanced JSON formatting with better separation of XBRL facts and narrative content.
"""

import os
import logging
import json
import re
import datetime
from typing import Dict, List, Any, Optional, Union
from collections import defaultdict

from .normalize_value import normalize_value, safe_parse_decimals
from .context_extractor import extract_contexts_from_html, map_contexts_to_periods
from .context_format_handler import extract_period_info
from .financial_statement_organizer import organize_financial_statements, FinancialStatementOrganizer


class ImprovedJSONFormatter:
    """
    Enhanced JSON formatter that properly separates XBRL facts and narrative content
    """

    # Statement type definitions
    BALANCE_SHEET = "BALANCE_SHEET"
    INCOME_STATEMENT = "INCOME_STATEMENT"
    CASH_FLOW_STATEMENT = "CASH_FLOW_STATEMENT"
    EQUITY_STATEMENT = "EQUITY_STATEMENT"

    # Statement titles
    STATEMENT_TITLES = {
        BALANCE_SHEET: "Balance Sheet",
        INCOME_STATEMENT: "Income Statement",
        CASH_FLOW_STATEMENT: "Cash Flow Statement",
        EQUITY_STATEMENT: "Statement of Shareholders' Equity"
    }

    # Key metrics to extract for each statement type
    KEY_METRICS = {
        BALANCE_SHEET: [
            {"name": "total_assets", "concepts": ["Assets", "TotalAssets"]},
            {"name": "total_liabilities", "concepts": ["Liabilities", "TotalLiabilities"]},
            {"name": "total_equity", "concepts": ["StockholdersEquity", "TotalEquity", "TotalShareholdersEquity"]}
        ],
        INCOME_STATEMENT: [
            {"name": "net_income", "concepts": ["NetIncome", "ProfitLoss", "NetEarnings"]},
            {"name": "eps_basic", "concepts": ["EarningsPerShare", "EarningsPerShareBasic"]},
            {"name": "eps_diluted", "concepts": ["EarningsPerShareDiluted"]}
        ],
        CASH_FLOW_STATEMENT: [
            {"name": "cash_and_equivalents", "concepts": ["CashAndCashEquivalents", "CashAndCashEquivalentsAtCarryingValue"]}
        ],
        EQUITY_STATEMENT: [
            {"name": "total_liabilities", "concepts": ["Liabilities", "TotalLiabilities"]},
            {"name": "total_equity", "concepts": ["StockholdersEquity", "TotalEquity", "TotalShareholdersEquity"]},
            {"name": "eps_basic", "concepts": ["EarningsPerShare", "EarningsPerShareBasic"]},
            {"name": "eps_diluted", "concepts": ["EarningsPerShareDiluted"]}
        ]
    }

    def __init__(self):
        """Initialize the improved JSON formatter"""
        self.data_integrity = {
            "xbrl_facts_extracted": 0,
            "xbrl_facts_processed": 0,
            "narrative_sections_extracted": 0,
            "financial_statements_created": 0
        }
        self.section_to_readable_name = {
            "PART_I": "Part I",
            "PART_II": "Part II",
            "PART_III": "Part III",
            "PART_IV": "Part IV",
            "ITEM_1_BUSINESS": "Item 1. Business",
            "ITEM_1A_RISK_FACTORS": "Item 1A. Risk Factors",
            "ITEM_1B_UNRESOLVED_COMMENTS": "Item 1B. Unresolved Staff Comments",
            "ITEM_2_PROPERTIES": "Item 2. Properties",
            "ITEM_3_LEGAL_PROCEEDINGS": "Item 3. Legal Proceedings",
            "ITEM_4_MINE_SAFETY": "Item 4. Mine Safety Disclosures",
            "ITEM_5_MARKET": "Item 5. Market for Registrant's Common Equity",
            "ITEM_6_SELECTED_FINANCIAL_DATA": "Item 6. Selected Financial Data",
            "ITEM_7_MD_AND_A": "Item 7. Management's Discussion and Analysis",
            "ITEM_7A_MARKET_RISK": "Item 7A. Quantitative and Qualitative Disclosures About Market Risk",
            "ITEM_8_FINANCIAL_STATEMENTS": "Item 8. Financial Statements and Supplementary Data",
            "ITEM_9_DISAGREEMENTS": "Item 9. Changes in and Disagreements with Accountants",
            "ITEM_9A_CONTROLS": "Item 9A. Controls and Procedures",
            "ITEM_9B_OTHER_INFORMATION": "Item 9B. Other Information",
            "ITEM_10_DIRECTORS": "Item 10. Directors, Executive Officers and Corporate Governance",
            "ITEM_11_EXECUTIVE_COMPENSATION": "Item 11. Executive Compensation",
            "ITEM_12_SECURITY_OWNERSHIP": "Item 12. Security Ownership of Certain Beneficial Owners",
            "ITEM_13_RELATIONSHIPS": "Item 13. Certain Relationships and Related Transactions",
            "ITEM_14_ACCOUNTANT_FEES": "Item 14. Principal Accountant Fees and Services",
            "ITEM_15_EXHIBITS": "Item 15. Exhibits, Financial Statement Schedules",
            "BALANCE_SHEETS": "Balance Sheets",
            "INCOME_STATEMENTS": "Income Statements",
            "CASH_FLOW_STATEMENTS": "Cash Flow Statements",
            "EQUITY_STATEMENTS": "Statements of Shareholders' Equity",
            "NOTES_TO_FINANCIAL_STATEMENTS": "Notes to Financial Statements"
        }
        self.context_mapping_from_html = {}

    def generate_json_format(self, parsed_xbrl, filing_metadata) -> Dict[str, Any]:
        """
        Generate enhanced JSON format from parsed XBRL and narrative text

        Args:
            parsed_xbrl: Parsed XBRL data
            filing_metadata: Filing metadata including any narrative content

        Returns:
            JSON-formatted content as a dictionary
        """
        # Debug input types
        logging.info(f"ImprovedJSONFormatter.generate_json_format: parsed_xbrl type={type(parsed_xbrl)}, filing_metadata type={type(filing_metadata)}")

        # Handle string input (error message)
        if isinstance(parsed_xbrl, str):
            logging.info(f"ImprovedJSONFormatter: parsed_xbrl is a string: {parsed_xbrl[:100]}...")
            return {"error": parsed_xbrl}

        # Handle dictionary with error
        if isinstance(parsed_xbrl, dict) and "error" in parsed_xbrl:
            return {"error": parsed_xbrl["error"]}

        # Initialize the JSON structure with clear separation of financial data and narrative content
        json_data = {
            "metadata": {},
            "narrative_sections": [],
            "financial_data": {
                "statements": []
            },
            "xbrl_data": {
                "contexts": {},
                "facts": []
            }
        }

        # Process metadata
        if isinstance(filing_metadata, dict):
            # Extract basic metadata
            metadata_fields = [
                "ticker", "company_name", "filing_type", "filing_date",
                "period_end_date", "fiscal_year", "fiscal_period", "source_url", "cik"
            ]
            for field in metadata_fields:
                if field in filing_metadata:
                    json_data["metadata"][field] = filing_metadata[field]

        # Process narrative sections
        document_sections = []
        if isinstance(filing_metadata, dict) and "html_content" in filing_metadata:
            html_content = filing_metadata["html_content"]
            if isinstance(html_content, dict) and "document_sections" in html_content:
                # Handle document_sections as dict or list
                if isinstance(html_content["document_sections"], dict):
                    for section_id, section_data in html_content["document_sections"].items():
                        if isinstance(section_data, dict):
                            # Skip financial statement sections
                            if self._is_financial_section(section_id):
                                continue

                            # Create a clean section object
                            section = {
                                "id": section_id,
                                "title": section_data.get("title", self.section_to_readable_name.get(section_id, section_id))
                            }

                            # Add content if available
                            content = ""
                            if "text" in section_data:
                                content = section_data["text"]
                            elif "content" in section_data:
                                content = section_data["content"]

                            # Clean financial tables from content
                            if content:
                                content = self._clean_financial_tables_from_content(content, section_id)

                            document_sections.append(section)
                elif isinstance(html_content["document_sections"], list):
                    for section_data in html_content["document_sections"]:
                        if isinstance(section_data, dict):
                            section_id = section_data.get("id", "")

                            # Skip financial statement sections
                            if self._is_financial_section(section_id):
                                continue

                            # Create a clean section object
                            section = {
                                "id": section_id,
                                "title": section_data.get("title", self.section_to_readable_name.get(section_id, section_id))
                            }

                            # Add content if available
                            if "text" in section_data:
                                section["content"] = section_data["text"]
                            elif "content" in section_data:
                                section["content"] = section_data["content"]

                            document_sections.append(section)

        # Store narrative sections
        json_data["narrative_sections"] = document_sections
        self.data_integrity["narrative_sections_extracted"] = len(document_sections)

        # Process XBRL data
        try:
            # Extract facts and contexts
            facts = []
            contexts = {}

            if isinstance(parsed_xbrl, dict):
                facts = parsed_xbrl.get("facts", [])
                contexts = parsed_xbrl.get("contexts", {})
            elif isinstance(parsed_xbrl, list):
                facts = parsed_xbrl

            self.data_integrity["xbrl_facts_extracted"] = len(facts)

            # Try to extract contexts from HTML if not available in parsed_xbrl
            if not contexts and "doc_path" in filing_metadata:
                doc_path = filing_metadata["doc_path"]
                logging.info(f"Attempting to extract contexts from HTML: {doc_path}")
                try:
                    contexts = extract_contexts_from_html(doc_path)
                    self.context_mapping_from_html = map_contexts_to_periods(contexts)
                except Exception as e:
                    logging.error(f"Error extracting contexts from HTML: {str(e)}")

            # Store contexts in JSON data
            json_data["xbrl_data"]["contexts"] = contexts

            # Process facts
            processed_facts = []
            fact_index = {}  # Map concept names to fact indices

            for i, fact in enumerate(facts):
                # Extract basic fact information
                concept = fact.get("concept", fact.get("name", ""))
                value = fact.get("value", "")
                context_ref = fact.get("contextRef", fact.get("context_ref", ""))
                unit_ref = fact.get("unitRef", fact.get("unit_ref", ""))

                # Create processed fact
                processed_fact = {
                    "concept": concept,
                    "value": value,
                    "context_ref": context_ref,
                    "unit_ref": unit_ref
                }

                # Add to processed facts
                processed_facts.append(processed_fact)

                # Add to fact index
                if concept:
                    if concept not in fact_index:
                        fact_index[concept] = []
                    fact_index[concept].append(i)

            # Store processed facts
            json_data["xbrl_data"]["facts"] = processed_facts
            self.data_integrity["xbrl_facts_processed"] = len(processed_facts)

            # Create financial statements
            financial_statements = self._create_financial_statements(processed_facts, contexts, fact_index)
            json_data["financial_data"]["statements"] = financial_statements
            self.data_integrity["financial_statements_created"] = len(financial_statements)

        except Exception as e:
            logging.error(f"Error processing XBRL data: {str(e)}")
            json_data["xbrl_data"]["contexts"] = {}
            json_data["xbrl_data"]["facts"] = []
            json_data["financial_data"]["statements"] = []

        # Add data integrity metrics
        json_data["data_integrity"] = self.data_integrity

        return json_data

    def _create_financial_statements(self, facts, contexts, fact_index) -> List[Dict[str, Any]]:
        """
        Create structured financial statements from XBRL facts

        Args:
            facts: List of processed XBRL facts
            contexts: Dictionary of XBRL contexts
            fact_index: Dictionary mapping concept names to fact indices

        Returns:
            List of financial statements with structured data
        """
        # Initialize financial statements
        financial_statements = []

        # Create a financial statement organizer
        organizer = FinancialStatementOrganizer()

        # Categorize facts by statement type
        facts_by_statement = organizer.organize_facts(facts)

        # Create each statement type
        for statement_type in [self.BALANCE_SHEET, self.INCOME_STATEMENT, self.CASH_FLOW_STATEMENT, self.EQUITY_STATEMENT]:
            statement_facts = facts_by_statement.get(statement_type, [])
            if statement_facts:
                # Create statement structure
                statement = {
                    "type": statement_type,
                    "title": self.STATEMENT_TITLES.get(statement_type, statement_type),
                    "key_metrics": {},
                    "structured_data": {
                        "periods": [],
                        "rows": []
                    },
                    "sections": [],
                    "fact_refs": []
                }

                # Extract fact references
                fact_refs = []
                for fact in statement_facts:
                    concept = fact.get("concept", "")
                    if concept in fact_index:
                        fact_refs.extend(fact_index[concept])

                # Remove duplicates and sort
                fact_refs = sorted(list(set(fact_refs)))
                statement["fact_refs"] = fact_refs

                # Group facts by context
                context_groups = defaultdict(list)
                for fact_ref in fact_refs:
                    if fact_ref < len(facts):
                        fact = facts[fact_ref]
                        context_ref = fact.get("context_ref", "")
                        context_groups[context_ref].append(fact)

                # Extract periods from contexts
                periods = sorted(context_groups.keys())

                # If no periods found, try to extract from fact values
                if not periods or (len(periods) == 1 and periods[0] == ""):
                    # Look for facts with dates in their values
                    date_facts = []
                    for ref in fact_refs:
                        if ref < len(facts):
                            fact = facts[ref]
                            concept = fact.get("concept", "").lower()
                            if "date" in concept or "period" in concept:
                                date_facts.append(fact)

                    # Extract dates from values
                    import re
                    date_pattern = r'([A-Za-z]+\s+\d+,\s*\d{4}|\d{4}[-/]\d{2}[-/]\d{2})'
                    for fact in date_facts:
                        value = fact.get("value", "")
                        dates = re.findall(date_pattern, value)
                        if dates:
                            periods = dates
                            break

                    # If still no periods, use period end date from metadata
                    if not periods or (len(periods) == 1 and periods[0] == ""):
                        # Try to find period end date in facts
                        for ref in fact_refs:
                            if ref < len(facts):
                                fact = facts[ref]
                                concept = fact.get("concept", "")
                                if concept == "dei:DocumentPeriodEndDate":
                                    periods = [fact.get("value", "")]
                                    break

                # Store periods in statement
                statement["structured_data"]["periods"] = periods

                # Create rows from facts
                rows = []
                for ref in fact_refs:
                    if ref < len(facts):
                        fact = facts[ref]
                        concept = fact.get("concept", "")
                        value = fact.get("value", "")
                        context_ref = fact.get("context_ref", "")

                        # Create a readable label from the concept
                        label = self._create_readable_label(concept)

                        # Create a row with the label and values for each period
                        row = {"label": label}

                        # Add value for the appropriate period
                        for period in periods:
                            if period == context_ref:
                                row[period] = value
                            else:
                                # Check if this fact exists in another context
                                for other_ref in fact_refs:
                                    if other_ref < len(facts):
                                        other_fact = facts[other_ref]
                                        if other_fact.get("concept") == concept and other_fact.get("context_ref") == period:
                                            row[period] = other_fact.get("value", "")
                                            break

                        # Add row if it has values
                        if len(row) > 1:  # More than just the label
                            rows.append(row)

                # Store rows in statement
                statement["structured_data"]["rows"] = rows

                # Extract key metrics
                key_metrics = {}
                for metric in self.KEY_METRICS.get(statement_type, []):
                    metric_name = metric["name"]
                    metric_concepts = metric["concepts"]

                    # Look for matching concepts
                    for concept_pattern in metric_concepts:
                        for ref in fact_refs:
                            if ref < len(facts):
                                fact = facts[ref]
                                concept = fact.get("concept", "")

                                # Check if concept matches pattern
                                if concept_pattern.lower() in concept.lower():
                                    value = fact.get("value", "")
                                    context_ref = fact.get("context_ref", "")

                                    # Add metric
                                    key_metrics[metric_name] = {
                                        "value": value,
                                        "period": context_ref
                                    }
                                    break

                        # Stop if metric found
                        if metric_name in key_metrics:
                            break

                # Store key metrics in statement
                statement["key_metrics"] = key_metrics

                # Add statement to financial statements
                financial_statements.append(statement)

        return financial_statements

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

    def _create_readable_label(self, concept):
        """
        Create a readable label from an XBRL concept name

        Args:
            concept: XBRL concept name

        Returns:
            Readable label
        """
        # Remove namespace prefix
        if ":" in concept:
            concept = concept.split(":", 1)[1]

        # Split camelCase
        label = re.sub(r'([a-z])([A-Z])', r'\1 \2', concept)

        # Replace underscores with spaces
        label = label.replace("_", " ")

        # Capitalize first letter of each word
        label = " ".join(word.capitalize() for word in label.split())

        return label

    def save_json_format(self, json_data: Dict[str, Any], filing_metadata: Dict[str, Any], output_path: str) -> Dict[str, Any]:
        """
        Save JSON format to a file

        Args:
            json_data: JSON-formatted content as a dictionary
            filing_metadata: Filing metadata
            output_path: Path to save the file

        Returns:
            Dict with save result
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Get original size for comparison
            json_str = json.dumps(json_data, indent=2)
            original_size = len(json_str.encode('utf-8'))
            logging.info(f"Original JSON size: {original_size / 1024:.2f} KB")

            # Check if this is a 10-K filing (which tends to be larger)
            filing_type = filing_metadata.get('filing_type', '')
            is_10k = filing_type == '10-K'

            # For very large 10-K filings, consider minifying the JSON
            if is_10k and original_size > 10 * 1024 * 1024:  # If larger than 10MB
                logging.info("Large 10-K filing detected, minifying JSON")
                json_str = json.dumps(json_data, separators=(',', ':'))
                minified_size = len(json_str.encode('utf-8'))
                size_reduction = (original_size - minified_size) / original_size * 100
                logging.info(f"Minified JSON size: {minified_size / 1024:.2f} KB (reduced by {size_reduction:.2f}%)")
            else:
                minified_size = original_size
                size_reduction = 0

            # Save file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(json_str)

            return {
                "success": True,
                "path": output_path,
                "size": os.path.getsize(output_path),
                "original_size": original_size,
                "minified_size": minified_size,
                "size_reduction_percent": size_reduction
            }
        except Exception as e:
            logging.error(f"Error saving JSON format: {str(e)}")
            return {"error": f"Error saving JSON format: {str(e)}"}


# Create a singleton instance
improved_json_formatter = ImprovedJSONFormatter()
