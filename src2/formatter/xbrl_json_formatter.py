#!/usr/bin/env python3
"""
XBRL JSON Formatter Module

Responsible for converting parsed XBRL data and narrative content to JSON format,
with a clean separation between financial data and narrative content.
"""

import os
import logging
import json
import re
from typing import Dict, List, Any

from .context_extractor import extract_contexts_from_html, map_contexts_to_periods
from .financial_statement_organizer import organize_financial_statements, FinancialStatementOrganizer


class XBRLJSONFormatter:
    """
    Format parsed XBRL data and narrative content as JSON with clean separation
    """

    def __init__(self):
        """
        Initialize XBRL JSON formatter
        """
        # Map of section IDs to human-readable names
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
        
        # Data integrity tracking
        self.data_integrity = {
            "xbrl_facts_extracted": 0,
            "xbrl_facts_processed": 0,
            "narrative_sections_extracted": 0,
            "financial_statements_created": 0
        }

    def generate_json_format(self, parsed_xbrl, filing_metadata) -> Dict[str, Any]:
        """
        Generate JSON format from parsed XBRL and narrative text
        
        Args:
            parsed_xbrl: Parsed XBRL data
            filing_metadata: Filing metadata including any narrative content
            
        Returns:
            JSON-formatted content as a dictionary
        """
        # Debug input types
        logging.info(f"XBRLJSONFormatter.generate_json_format: parsed_xbrl type={type(parsed_xbrl)}, filing_metadata type={type(filing_metadata)}")
        
        # Handle string input (error message)
        if isinstance(parsed_xbrl, str):
            logging.info(f"XBRLJSONFormatter: parsed_xbrl is a string: {parsed_xbrl[:100]}...")
            return {"error": parsed_xbrl}
        
        # Handle dictionary with error
        if isinstance(parsed_xbrl, dict) and "error" in parsed_xbrl:
            return {"error": parsed_xbrl["error"]}
        
        # Initialize the JSON structure with clear separation of financial data and narrative content
        json_data = {
            "metadata": {},
            "document": {
                "sections": []
            },
            "financial_data": {
                "statements": []
            },
            "xbrl_data": {
                "contexts": {},
                "facts": []
            },
            "data_integrity": {}
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
                            # Skip financial statement sections - we'll get this data from XBRL
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
                elif isinstance(html_content["document_sections"], list):
                    for section_data in html_content["document_sections"]:
                        if isinstance(section_data, dict):
                            section_id = section_data.get("id", "")
                            
                            # Skip financial statement sections - we'll get this data from XBRL
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
        json_data["document"]["sections"] = document_sections
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
                except Exception as e:
                    logging.error(f"Error extracting contexts from HTML: {str(e)}")
            
            # Store contexts in JSON data
            json_data["xbrl_data"]["contexts"] = contexts
            
            # Store raw facts in JSON data
            json_data["xbrl_data"]["facts"] = facts
            self.data_integrity["xbrl_facts_processed"] = len(facts)
            
            # Create financial statements using the FinancialStatementOrganizer
            # This is the same approach used in the .txt implementation
            organizer = FinancialStatementOrganizer()
            
            # Organize facts by statement type
            facts_by_statement = organizer.organize_facts(facts)
            
            # Create each statement type
            statements = []
            
            # Process each statement type
            for statement_type in [
                FinancialStatementOrganizer.BALANCE_SHEET,
                FinancialStatementOrganizer.INCOME_STATEMENT,
                FinancialStatementOrganizer.CASH_FLOW_STATEMENT,
                FinancialStatementOrganizer.EQUITY_STATEMENT
            ]:
                statement_facts = facts_by_statement.get(statement_type, [])
                if statement_facts:
                    # Get contexts for this statement
                    statement_contexts = list(organizer.get_statement_contexts(statement_type))
                    
                    # Create statement structure
                    statement = {
                        "type": statement_type,
                        "title": self._get_statement_title(statement_type),
                        "contexts": statement_contexts,
                        "facts": []
                    }
                    
                    # Get facts by section
                    facts_by_section = organizer.facts_by_section.get(statement_type, {})
                    
                    # Add sections to statement
                    sections = []
                    for section_name, section_facts in facts_by_section.items():
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
                                    "concept": fact.get("concept", ""),
                                    "value": fact.get("value", ""),
                                    "context_ref": fact.get("contextRef", fact.get("context_ref", "")),
                                    "unit_ref": fact.get("unitRef", fact.get("unit_ref", ""))
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
                            "concept": fact.get("concept", ""),
                            "value": fact.get("value", ""),
                            "context_ref": fact.get("contextRef", fact.get("context_ref", "")),
                            "unit_ref": fact.get("unitRef", fact.get("unit_ref", ""))
                        }
                        
                        # Add to statement facts
                        statement["facts"].append(clean_fact)
                    
                    # Add statement to statements
                    statements.append(statement)
            
            # Store statements in JSON data
            json_data["financial_data"]["statements"] = statements
            self.data_integrity["financial_statements_created"] = len(statements)
            
        except Exception as e:
            logging.error(f"Error processing XBRL data: {str(e)}")
            json_data["xbrl_data"]["contexts"] = {}
            json_data["xbrl_data"]["facts"] = []
            json_data["financial_data"]["statements"] = []
        
        # Add data integrity metrics
        json_data["data_integrity"] = self.data_integrity
        
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

    def _get_statement_title(self, statement_type):
        """
        Get a human-readable title for a statement type
        
        Args:
            statement_type: Statement type
            
        Returns:
            Human-readable title
        """
        titles = {
            FinancialStatementOrganizer.BALANCE_SHEET: "Balance Sheet",
            FinancialStatementOrganizer.INCOME_STATEMENT: "Income Statement",
            FinancialStatementOrganizer.CASH_FLOW_STATEMENT: "Cash Flow Statement",
            FinancialStatementOrganizer.EQUITY_STATEMENT: "Statement of Shareholders' Equity",
            FinancialStatementOrganizer.OTHER: "Other Financial Information"
        }
        
        return titles.get(statement_type, statement_type)

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
xbrl_json_formatter = XBRLJSONFormatter()
