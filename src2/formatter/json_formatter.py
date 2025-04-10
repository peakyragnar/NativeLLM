#!/usr/bin/env python3
"""
JSON Formatter Module

Responsible for converting parsed XBRL data and narrative content to JSON format.
"""

import os
import logging
import json
import re
import datetime
from typing import Dict, List, Any, Optional, Union

from .normalize_value import normalize_value, safe_parse_decimals
from .context_extractor import extract_contexts_from_html, map_contexts_to_periods
from .context_format_handler import extract_period_info
from .financial_statement_organizer import organize_financial_statements
from .normalized_financial_mapper import NormalizedFinancialMapper
from .xbrl_mapping_integration import xbrl_mapping_integration


class JSONFormatter:
    """
    Format parsed XBRL data and narrative content as JSON
    """

    def __init__(self):
        """
        Initialize JSON formatter
        """
        # Map of section IDs to human-readable names (same as LLMFormatter)
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
            "ITEM_8_FINANCIAL": "Financial Statements and Supplementary Data",
            "ITEM_9_CHANGES": "Changes in and Disagreements with Accountants",
            "ITEM_9A_CONTROLS": "Controls and Procedures",
            "ITEM_9B_OTHER": "Other Information",
            "ITEM_9C_DISCLOSURE": "Disclosure Regarding Foreign Jurisdictions",
            "ITEM_10_DIRECTORS": "Directors, Executive Officers and Corporate Governance",
            "ITEM_11_COMPENSATION": "Executive Compensation",
            "ITEM_12_SECURITY_OWNERSHIP": "Security Ownership of Certain Beneficial Owners and Management",
            "ITEM_13_RELATED_TRANSACTIONS": "Certain Relationships and Related Transactions",
            "ITEM_14_FEES": "Principal Accountant Fees and Services",
            "ITEM_15_EXHIBITS": "Exhibits, Financial Statement Schedules",
            "ITEM_16_FORM_SUMMARY": "Form 10-K Summary",

            # Form 10-Q Items
            "PART1_ITEM1_FINANCIAL": "Financial Statements",
            "PART1_ITEM2_MD_AND_A": "Management's Discussion and Analysis",
            "PART1_ITEM3_MARKET_RISK": "Quantitative and Qualitative Disclosures About Market Risk",
            "PART1_ITEM4_CONTROLS": "Controls and Procedures",
            "PART2_ITEM1_LEGAL": "Legal Proceedings",
            "PART2_ITEM1A_RISK_FACTORS": "Risk Factors",
            "PART2_ITEM2_UNREGISTERED_SALES": "Unregistered Sales of Equity Securities",
            "PART2_ITEM3_DEFAULTS": "Defaults Upon Senior Securities",
            "PART2_ITEM4_MINE_SAFETY": "Mine Safety Disclosures",
            "PART2_ITEM5_OTHER": "Other Information",
            "PART2_ITEM6_EXHIBITS": "Exhibits"
        }

        # Initialize data integrity tracking
        self.data_integrity = {
            "tables_detected": 0,
            "tables_included": 0,
            "total_table_rows": 0,
            "narrative_paragraphs": 0,
            "included_paragraphs": 0,
            "section_tables": {},
            "sections_detected": 0,
            "sections_included": 0,
            "alternative_sections_detected": 0,
            "content_coverage": 0.0
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
        logging.info(f"JSONFormatter.generate_json_format: parsed_xbrl type={type(parsed_xbrl)}, filing_metadata type={type(filing_metadata)}")

        # Handle string input (error message)
        if isinstance(parsed_xbrl, str):
            logging.info(f"JSONFormatter: parsed_xbrl is a string: {parsed_xbrl[:100]}...")
            return {"error": parsed_xbrl}

        # Handle dictionary with error
        if isinstance(parsed_xbrl, dict) and "error" in parsed_xbrl:
            return {"error": parsed_xbrl["error"]}

        # Initialize context mapping from HTML as an instance variable
        self.context_mapping_from_html = {}

        # Initialize the JSON structure
        self.json_data = {
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
            }
        }
        json_data = self.json_data

        # Handle filing_metadata
        if not isinstance(filing_metadata, dict):
            filing_metadata = {}

        # Add document metadata
        ticker = filing_metadata.get("ticker", "unknown")
        filing_type = filing_metadata.get("filing_type", "unknown")
        company_name = filing_metadata.get("company_name", "unknown")
        cik = filing_metadata.get("cik", "unknown")
        filing_date = filing_metadata.get("filing_date", "unknown")
        period_end = filing_metadata.get("period_end_date", "unknown")
        fiscal_year = filing_metadata.get("fiscal_year", "")
        fiscal_period = filing_metadata.get("fiscal_period", "")
        source_url = filing_metadata.get("source_url", "")

        json_data["metadata"] = {
            "ticker": ticker,
            "company_name": company_name,
            "filing_type": filing_type,
            "filing_date": filing_date,
            "period_end_date": period_end,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "source_url": source_url,
            "cik": cik
        }

        # Add document structure if available
        try:
            # Debug the structure of filing_metadata
            logging.info(f"Filing metadata keys: {list(filing_metadata.keys())}")
            if "html_content" in filing_metadata:
                logging.info(f"html_content type: {type(filing_metadata['html_content'])}")
                if isinstance(filing_metadata["html_content"], dict):
                    logging.info(f"html_content dict keys: {list(filing_metadata['html_content'].keys())}")
                    if "document_sections" in filing_metadata["html_content"]:
                        document_sections = filing_metadata["html_content"]["document_sections"]
                        logging.info(f"document_sections type: {type(document_sections)}")

                        # Convert dict to list if needed
                        if isinstance(document_sections, dict):
                            logging.info(f"Converting document_sections dict to list")
                            sections_list = []
                            for section_id, section_data in document_sections.items():
                                if isinstance(section_data, dict):
                                    # Add the id to the section data
                                    section_data["id"] = section_id

                                    # Make sure content is included but avoid duplication
                                    if "text" in section_data and not section_data.get("content"):
                                        # For all sections, store a summary instead of full content
                                        # This dramatically reduces file size while maintaining searchability
                                        text = section_data["text"]
                                        text_len = len(text)

                                        # Preserve full content for 100% data integrity
                                        logging.info(f"Processing section {section_id} with text length {text_len}")
                                        # Keep full content for all sections to maintain 100% data integrity
                                        section_data["content"] = text
                                        logging.info(f"Kept full content for section {section_id}: {text_len} chars")

                                    sections_list.append(section_data)
                                else:
                                    logging.warning(f"Unexpected section data type: {type(section_data)}")
                            json_data["document"]["sections"] = self._process_document_sections(sections_list)
                        else:
                            json_data["document"]["sections"] = self._process_document_sections(document_sections)
                elif isinstance(filing_metadata["html_content"], list):
                    # Handle case where html_content is a list of sections
                    logging.info(f"html_content list length: {len(filing_metadata['html_content'])}")
                    json_data["document"]["sections"] = self._process_document_sections(filing_metadata["html_content"])
                else:
                    # Handle other types
                    logging.info(f"html_content is neither dict nor list")
                    json_data["document"]["sections"] = []
            else:
                # Check for document_sections directly
                if "document_sections" in filing_metadata:
                    logging.info(f"document_sections found directly in metadata")
                    document_sections = filing_metadata["document_sections"]

                    # Convert dict to list if needed
                    if isinstance(document_sections, dict):
                        logging.info(f"Converting document_sections dict to list")
                        sections_list = []
                        for section_id, section_data in document_sections.items():
                            if isinstance(section_data, dict):
                                # Add the id to the section data
                                section_data["id"] = section_id
                                sections_list.append(section_data)
                            else:
                                logging.warning(f"Unexpected section data type: {type(section_data)}")
                        json_data["document"]["sections"] = self._process_document_sections(sections_list)
                    else:
                        json_data["document"]["sections"] = self._process_document_sections(document_sections)
                else:
                    logging.info(f"No document sections found in metadata")
                    json_data["document"]["sections"] = []
        except Exception as e:
            logging.error(f"Error processing document sections: {str(e)}")
            json_data["document"]["sections"] = []

        # Process XBRL data
        try:
            logging.info(f"Parsed XBRL keys: {list(parsed_xbrl.keys())}")

            if "facts" in parsed_xbrl:
                # Extract contexts
                contexts = parsed_xbrl.get("contexts", {})
                logging.info(f"Contexts type: {type(contexts)}, length: {len(contexts) if isinstance(contexts, dict) else 'N/A'}")

                # If contexts are empty but we have doc_path in metadata, try to extract contexts from HTML
                if not contexts and "doc_path" in filing_metadata and os.path.exists(filing_metadata["doc_path"]):
                    try:
                        logging.info(f"Attempting to extract contexts from HTML: {filing_metadata['doc_path']}")
                        from .context_extractor import extract_contexts_from_html
                        html_contexts = extract_contexts_from_html(filing_metadata["doc_path"])
                        if html_contexts:
                            logging.info(f"Extracted {len(html_contexts)} contexts from HTML")
                            contexts = html_contexts
                    except Exception as ctx_err:
                        logging.error(f"Error extracting contexts from HTML: {str(ctx_err)}")

                json_data["xbrl_data"]["contexts"] = self._process_contexts(contexts)

                # Extract facts
                facts = parsed_xbrl.get("facts", [])
                logging.info(f"Facts type: {type(facts)}, length: {len(facts) if isinstance(facts, list) else 'N/A'}")

                # Process facts and create a fact index for reference
                processed_facts = self._process_facts(facts)

                # Create a fact index by concept for faster lookups
                fact_index = {}
                for i, fact in enumerate(processed_facts):
                    concept = fact.get("concept", "")
                    if concept not in fact_index:
                        fact_index[concept] = []
                    fact_index[concept].append(i)  # Store the index of the fact

                # Store the processed facts
                json_data["xbrl_data"]["facts"] = processed_facts

                # Organize financial statements using fact references instead of duplicating facts
                financial_statements = self._organize_financial_statements_optimized(processed_facts, contexts, fact_index)
                json_data["financial_data"]["statements"] = financial_statements
        except Exception as e:
            logging.error(f"Error processing XBRL data: {str(e)}")
            json_data["xbrl_data"]["contexts"] = {}
            json_data["xbrl_data"]["facts"] = []
            json_data["financial_data"]["statements"] = []

        # Add data integrity metrics
        json_data["data_integrity"] = self.data_integrity

        return json_data

    def _process_document_sections(self, document_sections) -> List[Dict[str, Any]]:
        """
        Process document sections into a hierarchical structure

        Args:
            document_sections: List of document sections

        Returns:
            List of processed document sections with optimized content
        """
        processed_sections = []

        # Handle non-list input
        if not isinstance(document_sections, list):
            logging.error(f"document_sections is not a list: {type(document_sections)}")
            return processed_sections

        # Track sections for data integrity
        self.data_integrity["sections_detected"] = len(document_sections)
        self.data_integrity["sections_included"] = 0

        # First pass: collect all section IDs and their content
        section_content_map = {}
        for section in document_sections:
            if not isinstance(section, dict):
                continue

            section_id = section.get("id", "")
            if section_id:
                section_content_map[section_id] = section.get("content", "")

        # Second pass: process sections and optimize content
        for section in document_sections:
            # Handle non-dict section
            if not isinstance(section, dict):
                logging.error(f"section is not a dict: {type(section)}")
                continue

            section_id = section.get("id", "")
            section_title = section.get("title", "")
            section_content = section.get("content", "")
            section_level = section.get("level", 1)

            # Use readable name if available
            if section_id in self.section_to_readable_name:
                section_title = self.section_to_readable_name[section_id]

            # Optimize content to avoid duplication
            # If this section has subsections, only include the content that's unique to this section
            subsections = section.get("subsections", [])
            if subsections and section_content:
                # Extract subsection IDs
                subsection_ids = []
                for subsection in subsections:
                    if isinstance(subsection, dict):
                        subsection_id = subsection.get("id", "")
                        if subsection_id:
                            subsection_ids.append(subsection_id)

                # Remove subsection content from this section's content
                optimized_content = section_content
                for subsection_id in subsection_ids:
                    subsection_content = section_content_map.get(subsection_id, "")
                    if subsection_content and subsection_content in optimized_content:
                        # Replace subsection content with a reference
                        optimized_content = optimized_content.replace(subsection_content, f"[See section {subsection_id}]")

                # If the optimized content is too short, keep a summary
                if len(optimized_content) < 100 and len(section_content) > 1000:
                    # Keep the first 500 characters as a summary
                    optimized_content = section_content[:500] + "..."

                section_content = optimized_content

            # Process tables in the section
            tables = section.get("tables", [])
            processed_tables = []

            if isinstance(tables, list):
                for table in tables:
                    if not isinstance(table, dict):
                        continue

                    processed_table = {
                        "title": table.get("title", ""),
                        "headers": table.get("headers", []),
                        "rows": table.get("rows", [])
                    }
                    processed_tables.append(processed_table)

                    # Track table data for integrity metrics
                    self.data_integrity["tables_detected"] += 1
                    self.data_integrity["tables_included"] += 1
                    self.data_integrity["total_table_rows"] += len(table.get("rows", []))

            # Process subsections
            processed_subsections = self._process_document_sections(subsections)

            # Create the processed section
            processed_section = {
                "id": section_id,
                "title": section_title,
                "level": section_level,
                "content": section_content,
                "tables": processed_tables,
                "sections": processed_subsections
            }

            processed_sections.append(processed_section)
            self.data_integrity["sections_included"] += 1

            # Track paragraphs for integrity metrics
            if section_content and isinstance(section_content, str):
                paragraphs = section_content.count('\n\n') + 1
                self.data_integrity["narrative_paragraphs"] += paragraphs
                self.data_integrity["included_paragraphs"] += paragraphs

        return processed_sections

    def _process_contexts(self, contexts) -> Dict[str, Any]:
        """
        Process XBRL contexts

        Args:
            contexts: Dictionary of XBRL contexts

        Returns:
            Processed contexts
        """
        processed_contexts = {}

        # Handle non-dictionary or empty contexts
        if not isinstance(contexts, dict):
            return processed_contexts

        for context_id, context_data in contexts.items():
            # Extract period information
            period = context_data.get("period", {})
            start_date = period.get("startDate")
            end_date = period.get("endDate")
            instant = period.get("instant")

            # Create processed context
            processed_context = {
                "id": context_id,
                "period_type": "duration" if start_date and end_date else "instant",
                "period": {}
            }

            if start_date and end_date:
                processed_context["period"] = {
                    "start_date": start_date,
                    "end_date": end_date
                }
            elif instant:
                processed_context["period"] = {
                    "instant": instant
                }

            # Add entity information if available
            if "entity" in context_data:
                entity = context_data["entity"]
                processed_context["entity"] = {
                    "identifier": entity.get("identifier", ""),
                    "scheme": entity.get("scheme", "")
                }

                # Add segment information if available
                if "segment" in entity:
                    processed_context["entity"]["segment"] = entity["segment"]

            processed_contexts[context_id] = processed_context

        return processed_contexts

    def _process_facts(self, facts) -> List[Dict[str, Any]]:
        """
        Process XBRL facts with optimized field names and number representation

        Args:
            facts: List of XBRL facts

        Returns:
            Processed facts with optimized field names
        """
        processed_facts = []

        # Handle non-list or empty facts
        if not isinstance(facts, list):
            return processed_facts

        # Create a context reference map to avoid duplication
        context_ref_map = {}
        context_ref_counter = 0

        for fact in facts:
            # Extract basic fact information
            concept = fact.get("concept", "")
            value = fact.get("value", "")
            context_ref = fact.get("contextRef", "")
            unit_ref = fact.get("unitRef", "")
            decimals = fact.get("decimals")

            # Optimize context references using a map
            if context_ref:
                if context_ref not in context_ref_map:
                    context_ref_map[context_ref] = f"c{context_ref_counter}"
                    context_ref_counter += 1
                optimized_context_ref = context_ref_map[context_ref]
            else:
                optimized_context_ref = ""

            # Create processed fact with full field names to maintain data integrity
            # We're not shortening field names to ensure compatibility with existing code
            processed_fact = {
                "concept": concept,
                "value": value,
                "context_ref": optimized_context_ref
            }

            # Only include non-empty fields to save space
            if unit_ref:
                processed_fact["unit_ref"] = unit_ref

            # Add decimals if available
            if decimals is not None:
                processed_fact["decimals"] = decimals

            # Add normalized value if possible
            try:
                normalized = normalize_value(value, decimals)
                if normalized is not None and normalized != value:
                    processed_fact["normalized_value"] = normalized
            except:
                pass

            processed_facts.append(processed_fact)

        # Store the context reference map for later use
        self.context_ref_map = context_ref_map

        return processed_facts

    def _organize_financial_statements(self, facts, contexts) -> List[Dict[str, Any]]:
        """
        Organize facts into financial statements

        Args:
            facts: List of XBRL facts
            contexts: Dictionary of XBRL contexts

        Returns:
            List of financial statements
        """
        # Handle invalid inputs
        if not isinstance(facts, list) or not isinstance(contexts, dict):
            return []

        # Use the financial statement organizer to group facts
        try:
            # Organize facts manually since the organizer function may not be compatible

            # Group facts by statement type
            balance_sheet_facts = []
            income_statement_facts = []
            cash_flow_facts = []
            equity_statement_facts = []

            # Keywords for classification
            balance_sheet_keywords = [
                "asset", "liability", "equity", "balance", "inventory", "receivable", "payable",
                "debt", "property", "equipment", "goodwill", "intangible", "investment", "deposit",
                "capital", "stock", "share", "treasury", "retained", "accumulated"
            ]

            income_statement_keywords = [
                "revenue", "income", "expense", "earnings", "profit", "loss", "sale", "cost",
                "tax", "interest", "depreciation", "amortization", "compensation", "salary",
                "wage", "benefit", "research", "development", "marketing", "administrative",
                "operating", "gross", "net", "ebitda", "ebit"
            ]

            cash_flow_keywords = [
                "cash", "flow", "financing", "investing", "operating", "dividend", "payment",
                "receipt", "proceed", "acquisition", "disposal", "purchase", "repayment",
                "borrowing", "issuance", "redemption"
            ]

            equity_statement_keywords = [
                "equity", "capital", "stock", "share", "treasury", "retained", "accumulated",
                "comprehensive", "dividend", "shareholder", "stockholder", "issuance", "repurchase"
            ]

            # Simple classification based on concept names
            for fact in facts:
                concept = fact.get("concept", "").lower()

                # Classify based on concept name patterns
                if any(keyword in concept for keyword in balance_sheet_keywords):
                    balance_sheet_facts.append(fact)
                if any(keyword in concept for keyword in income_statement_keywords):
                    income_statement_facts.append(fact)
                if any(keyword in concept for keyword in cash_flow_keywords):
                    cash_flow_facts.append(fact)
                if any(keyword in concept for keyword in equity_statement_keywords):
                    equity_statement_facts.append(fact)

            # Facts can belong to multiple statements if they match multiple keyword sets

            financial_statements = []

            # Process balance sheet
            if balance_sheet_facts:
                balance_sheet = {
                    "type": "BALANCE_SHEET",
                    "facts": []
                }

                # Add all balance sheet facts directly
                for fact in balance_sheet_facts:
                    # Add the fact to the balance sheet
                    balance_sheet["facts"].append(fact)

                financial_statements.append(balance_sheet)

            # Process income statement
            if income_statement_facts:
                income_statement = {
                    "type": "INCOME_STATEMENT",
                    "facts": []
                }

                # Add all income statement facts directly
                for fact in income_statement_facts:
                    # Add the fact to the income statement
                    income_statement["facts"].append(fact)

                financial_statements.append(income_statement)

            # Process cash flow statement
            if cash_flow_facts:
                cash_flow = {
                    "type": "CASH_FLOW_STATEMENT",
                    "facts": []
                }

                # Add all cash flow facts directly
                for fact in cash_flow_facts:
                    # Add the fact to the cash flow statement
                    cash_flow["facts"].append(fact)

                financial_statements.append(cash_flow)

            # Process equity statement
            if equity_statement_facts:
                equity_statement = {
                    "type": "EQUITY_STATEMENT",
                    "facts": []
                }

                # Add all equity statement facts directly
                for fact in equity_statement_facts:
                    # Add the fact to the equity statement
                    equity_statement["facts"].append(fact)

                financial_statements.append(equity_statement)

            return financial_statements
        except Exception as e:
            logging.warning(f"Error organizing financial statements: {str(e)}")
            return []

    def _organize_financial_statements_optimized(self, facts, contexts, fact_index) -> List[Dict[str, Any]]:
        """
        Organize facts into financial statements with proper table structure and fact references
        to maintain 100% data integrity while providing structured access to financial data.

        Args:
            facts: List of XBRL facts
            contexts: Dictionary of XBRL contexts
            fact_index: Dictionary mapping concept names to fact indices

        Returns:
            List of financial statements with structured tables and fact references
        """
        # Get document sections from the JSON data
        self.document_sections = []
        if hasattr(self, 'json_data') and self.json_data and 'document' in self.json_data:
            self.document_sections = self.json_data['document'].get('sections', [])
        # Handle invalid inputs
        if not isinstance(facts, list) or not isinstance(contexts, dict):
            return []

        try:
            # Keywords for classification
            balance_sheet_keywords = [
                "asset", "liability", "equity", "balance", "inventory", "receivable", "payable",
                "debt", "property", "equipment", "goodwill", "intangible", "investment", "deposit",
                "capital", "stock", "share", "treasury", "retained", "accumulated"
            ]

            income_statement_keywords = [
                "revenue", "income", "expense", "earnings", "profit", "loss", "sale", "cost",
                "tax", "interest", "depreciation", "amortization", "compensation", "salary",
                "wage", "benefit", "research", "development", "marketing", "administrative",
                "operating", "gross", "net", "ebitda", "ebit"
            ]

            cash_flow_keywords = [
                "cash", "flow", "financing", "investing", "operating", "dividend", "payment",
                "receipt", "proceed", "acquisition", "disposal", "purchase", "repayment",
                "borrowing", "issuance", "redemption"
            ]

            equity_statement_keywords = [
                "equity", "capital", "stock", "share", "treasury", "retained", "accumulated",
                "comprehensive", "dividend", "shareholder", "stockholder", "issuance", "repurchase"
            ]

            # Create statement fact references
            balance_sheet_refs = []
            income_statement_refs = []
            cash_flow_refs = []
            equity_statement_refs = []

            # Classify facts directly by concept name
            for i, fact in enumerate(facts):
                concept = fact.get("concept", "")
                concept_lower = concept.lower()

                # Add fact references to appropriate statements
                if any(keyword in concept_lower for keyword in balance_sheet_keywords):
                    balance_sheet_refs.append(i)
                if any(keyword in concept_lower for keyword in income_statement_keywords):
                    income_statement_refs.append(i)
                if any(keyword in concept_lower for keyword in cash_flow_keywords):
                    cash_flow_refs.append(i)
                if any(keyword in concept_lower for keyword in equity_statement_keywords):
                    equity_statement_refs.append(i)

            # Remove duplicates
            balance_sheet_refs = list(set(balance_sheet_refs))
            income_statement_refs = list(set(income_statement_refs))
            cash_flow_refs = list(set(cash_flow_refs))
            equity_statement_refs = list(set(equity_statement_refs))

            # Create financial statements with structured tables
            financial_statements = []

            # Helper function to organize facts into structured sections
            def organize_facts_into_sections(fact_refs, fact_list):
                # Group facts by context (period)
                context_groups = {}
                for ref in fact_refs:
                    if ref < len(fact_list):
                        fact = fact_list[ref]
                        context = fact.get("context_ref", "")
                        if context not in context_groups:
                            context_groups[context] = []
                        context_groups[context].append(ref)

                # Create sections based on concept prefixes
                sections = []

                # Common section categories
                section_categories = {
                    "assets": ["asset", "cash", "inventory", "receivable", "property", "equipment"],
                    "liabilities": ["liability", "payable", "debt", "lease", "accrued"],
                    "equity": ["equity", "capital", "stock", "retained", "treasury"],
                    "revenue": ["revenue", "sales"],
                    "expenses": ["expense", "cost", "tax", "research", "development"],
                    "cash_flows": ["cash", "flow", "financing", "investing", "operating"]
                }

                # Create sections for each category
                for section_name, keywords in section_categories.items():
                    section_refs = []
                    for ref in fact_refs:
                        if ref < len(fact_list):
                            fact = fact_list[ref]
                            concept = fact.get("concept", "").lower()
                            if any(keyword in concept for keyword in keywords):
                                section_refs.append(ref)

                    if section_refs:
                        sections.append({
                            "id": section_name.upper(),
                            "title": section_name.replace("_", " ").title(),
                            "fact_refs": section_refs
                        })

                return sections

            # Helper function to create table structure from facts
            def create_table_from_facts(fact_refs, fact_list):
                if not fact_refs:
                    return []

                # Group facts by context (period)
                context_groups = {}
                for ref in fact_refs:
                    if ref < len(fact_list):
                        fact = fact_list[ref]
                        context = fact.get("context_ref", "")
                        if context not in context_groups:
                            context_groups[context] = []
                        context_groups[context].append(ref)

                # Create tables for each context group
                tables = []

                # Sort contexts by date if possible
                sorted_contexts = sorted(context_groups.keys())

                if len(sorted_contexts) > 1:
                    # Create a table with multiple periods
                    headers = ["Item"] + sorted_contexts

                    # Group facts by concept
                    concept_groups = {}
                    for context in sorted_contexts:
                        for ref in context_groups[context]:
                            if ref < len(fact_list):
                                fact = fact_list[ref]
                                concept = fact.get("concept", "")
                                if concept not in concept_groups:
                                    concept_groups[concept] = {}
                                concept_groups[concept][context] = ref

                    # Create rows
                    rows = []
                    for concept, context_refs in concept_groups.items():
                        row = [concept]
                        for context in sorted_contexts:
                            if context in context_refs:
                                row.append({"fact_ref": context_refs[context]})
                            else:
                                row.append("")
                        rows.append(row)

                    tables.append({
                        "title": "Financial Data",
                        "headers": headers,
                        "rows": rows
                    })
                else:
                    # Create a simple table for a single period
                    headers = ["Item", "Value"]
                    rows = []

                    for context in sorted_contexts:
                        for ref in context_groups[context]:
                            if ref < len(fact_list):
                                fact = fact_list[ref]
                                concept = fact.get("concept", "")
                                rows.append([concept, {"fact_ref": ref}])

                    tables.append({
                        "title": "Financial Data",
                        "headers": headers,
                        "rows": rows
                    })

                return tables

            # Helper function to extract key metrics from facts
            def extract_key_metrics(fact_refs, fact_list):
                key_metrics = {}

                # Define important financial metrics to extract
                key_metric_concepts = {
                    "total_assets": ["us-gaap:Assets"],
                    "total_liabilities": ["us-gaap:Liabilities", "us-gaap:LiabilitiesAndStockholdersEquity"],
                    "total_equity": ["us-gaap:StockholdersEquity"],
                    "cash_and_equivalents": ["us-gaap:CashAndCashEquivalentsAtCarryingValue"],
                    "revenue": ["us-gaap:Revenue", "us-gaap:SalesRevenueNet"],
                    "net_income": ["us-gaap:NetIncomeLoss", "us-gaap:ProfitLoss"],
                    "eps_basic": ["us-gaap:EarningsPerShareBasic"],
                    "eps_diluted": ["us-gaap:EarningsPerShareDiluted"]
                }

                # Find the most recent period for each concept
                for metric_name, concept_names in key_metric_concepts.items():
                    for ref in fact_refs:
                        if ref < len(fact_list):
                            fact = fact_list[ref]
                            concept = fact.get("concept", "")
                            if concept in concept_names:
                                value = fact.get("value", "")
                                context_ref = fact.get("context_ref", "")

                                # Only add if we don't have this metric yet or if this is more recent
                                if metric_name not in key_metrics or context_ref > key_metrics[metric_name]["period"]:
                                    key_metrics[metric_name] = {
                                        "value": value,
                                        "period": context_ref
                                    }

                return key_metrics

            # Helper function to create structured tables from XBRL facts
            def create_structured_table_from_facts(fact_refs, facts, statement_type):
                if not fact_refs or not facts:
                    return {}

                # Group facts by context (period)
                context_groups = {}
                for ref in fact_refs:
                    if ref < len(facts):
                        fact = facts[ref]
                        context = fact.get("context_ref", "")
                        if context not in context_groups:
                            context_groups[context] = []
                        context_groups[context].append(ref)

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

                # Create a mapping of concepts to rows
                concept_rows = {}

                # Process each fact to build the table
                for period in periods:
                    for ref in context_groups[period]:
                        if ref < len(facts):
                            fact = facts[ref]
                            concept = fact.get("concept", "")
                            value = fact.get("value", "")

                            # Create a readable label from the concept
                            label_parts = concept.split(":")
                            if len(label_parts) > 1:
                                label = label_parts[1]
                                # Convert camelCase to Title Case With Spaces
                                import re
                                label = re.sub(r'([a-z])([A-Z])', r'\1 \2', label)
                                label = label.title()
                            else:
                                label = concept

                            # Add to concept rows
                            if label not in concept_rows:
                                concept_rows[label] = {"label": label}

                            # Add value for this period
                            concept_rows[label][period] = value

                # Convert to list of rows
                rows = list(concept_rows.values())

                # Sort rows based on statement type
                if statement_type == "BALANCE_SHEET":
                    # For balance sheet, sort by assets, liabilities, equity
                    asset_rows = [row for row in rows if "asset" in row["label"].lower()]
                    liability_rows = [row for row in rows if "liability" in row["label"].lower() or "payable" in row["label"].lower()]
                    equity_rows = [row for row in rows if "equity" in row["label"].lower() or "capital" in row["label"].lower()]
                    other_rows = [row for row in rows if row not in asset_rows and row not in liability_rows and row not in equity_rows]
                    rows = asset_rows + liability_rows + equity_rows + other_rows
                elif statement_type == "INCOME_STATEMENT":
                    # For income statement, sort by revenue, expenses, income
                    revenue_rows = [row for row in rows if "revenue" in row["label"].lower() or "sales" in row["label"].lower()]
                    expense_rows = [row for row in rows if "expense" in row["label"].lower() or "cost" in row["label"].lower()]
                    income_rows = [row for row in rows if "income" in row["label"].lower() or "earnings" in row["label"].lower()]
                    other_rows = [row for row in rows if row not in revenue_rows and row not in expense_rows and row not in income_rows]
                    rows = revenue_rows + expense_rows + income_rows + other_rows
                elif statement_type == "CASH_FLOW_STATEMENT":
                    # For cash flow, sort by operating, investing, financing
                    operating_rows = [row for row in rows if "operating" in row["label"].lower()]
                    investing_rows = [row for row in rows if "investing" in row["label"].lower()]
                    financing_rows = [row for row in rows if "financing" in row["label"].lower()]
                    other_rows = [row for row in rows if row not in operating_rows and row not in investing_rows and row not in financing_rows]
                    rows = operating_rows + investing_rows + financing_rows + other_rows

                # Create the structured table
                table = {
                    "periods": periods,
                    "rows": rows
                }

                return table

            # Find document sections with financial statements
            financial_sections = {}
            for section in self.document_sections:
                section_id = section.get("id", "").upper()
                if "BALANCE" in section_id:
                    financial_sections["BALANCE_SHEET"] = section
                elif "INCOME" in section_id or "OPERATIONS" in section_id or "EARNINGS" in section_id:
                    financial_sections["INCOME_STATEMENT"] = section
                elif "CASH_FLOW" in section_id:
                    financial_sections["CASH_FLOW_STATEMENT"] = section
                elif "EQUITY" in section_id or "STOCKHOLDERS" in section_id or "SHAREHOLDERS" in section_id:
                    financial_sections["EQUITY_STATEMENT"] = section

            # Add balance sheet
            if balance_sheet_refs:
                balance_sheet_table = create_structured_table_from_facts(balance_sheet_refs, facts, "BALANCE_SHEET")
                balance_sheet_metrics = extract_key_metrics(balance_sheet_refs, facts)
                balance_sheet_sections = organize_facts_into_sections(balance_sheet_refs, facts)

                title = "Balance Sheet"
                if "BALANCE_SHEET" in financial_sections:
                    title = financial_sections["BALANCE_SHEET"].get("title", "Balance Sheet")

                financial_statements.append({
                    "type": "BALANCE_SHEET",
                    "title": title,
                    "key_metrics": balance_sheet_metrics,
                    "structured_data": balance_sheet_table,
                    "sections": balance_sheet_sections,
                    "fact_refs": balance_sheet_refs
                })

            # Add income statement
            if income_statement_refs:
                income_statement_table = create_structured_table_from_facts(income_statement_refs, facts, "INCOME_STATEMENT")
                income_statement_metrics = extract_key_metrics(income_statement_refs, facts)
                income_statement_sections = organize_facts_into_sections(income_statement_refs, facts)

                title = "Income Statement"
                if "INCOME_STATEMENT" in financial_sections:
                    title = financial_sections["INCOME_STATEMENT"].get("title", "Income Statement")

                financial_statements.append({
                    "type": "INCOME_STATEMENT",
                    "title": title,
                    "key_metrics": income_statement_metrics,
                    "structured_data": income_statement_table,
                    "sections": income_statement_sections,
                    "fact_refs": income_statement_refs
                })

            # Add cash flow statement
            if cash_flow_refs:
                cash_flow_table = create_structured_table_from_facts(cash_flow_refs, facts, "CASH_FLOW_STATEMENT")
                cash_flow_metrics = extract_key_metrics(cash_flow_refs, facts)
                cash_flow_sections = organize_facts_into_sections(cash_flow_refs, facts)

                title = "Cash Flow Statement"
                if "CASH_FLOW_STATEMENT" in financial_sections:
                    title = financial_sections["CASH_FLOW_STATEMENT"].get("title", "Cash Flow Statement")

                financial_statements.append({
                    "type": "CASH_FLOW_STATEMENT",
                    "title": title,
                    "key_metrics": cash_flow_metrics,
                    "structured_data": cash_flow_table,
                    "sections": cash_flow_sections,
                    "fact_refs": cash_flow_refs
                })

            # Add equity statement
            if equity_statement_refs:
                equity_statement_table = create_structured_table_from_facts(equity_statement_refs, facts, "EQUITY_STATEMENT")
                equity_statement_metrics = extract_key_metrics(equity_statement_refs, facts)
                equity_statement_sections = organize_facts_into_sections(equity_statement_refs, facts)

                title = "Equity Statement"
                if "EQUITY_STATEMENT" in financial_sections:
                    title = financial_sections["EQUITY_STATEMENT"].get("title", "Equity Statement")

                financial_statements.append({
                    "type": "EQUITY_STATEMENT",
                    "title": title,
                    "key_metrics": equity_statement_metrics,
                    "structured_data": equity_statement_table,
                    "sections": equity_statement_sections,
                    "fact_refs": equity_statement_refs
                })

            return financial_statements
        except Exception as e:
            logging.warning(f"Error organizing financial statements (optimized): {str(e)}")
            return []

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
json_formatter = JSONFormatter()
