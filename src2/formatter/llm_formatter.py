"""
LLM Formatter Module

Responsible for converting parsed XBRL data and narrative content to LLM-friendly format.
"""

import os
import logging
import json
import re
import datetime
from .normalize_value import normalize_value, safe_parse_decimals
from .context_extractor import extract_contexts_from_html, map_contexts_to_periods
from .context_format_handler import extract_period_info
from .financial_statement_organizer import organize_financial_statements
from .normalized_financial_mapper import NormalizedFinancialMapper
from .file_size_optimizer import FileSizeOptimizer
from .xbrl_mapping_integration import xbrl_mapping_integration

def safe_parse_decimals(decimals):
    '''Safely parse decimals value, handling 'INF' special case'''
    if not decimals:
        return None
    if str(decimals).strip().upper() == 'INF':
        return float('inf')  # Return Python's infinity
    try:
        return int(decimals)
    except (ValueError, TypeError):
        return None  # Return None for unparseable values

class LLMFormatter:
    """
    Format parsed XBRL data and narrative content for optimal LLM input
    """

    def __init__(self):
        """
        Initialize LLM formatter
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
            "ITEM_9_CHANGES_ACCOUNTANTS": "Changes in and Disagreements with Accountants",
            "ITEM_9A_CONTROLS": "Controls and Procedures",
            "ITEM_9B_OTHER_INFORMATION": "Other Information",
            "ITEM_9C_FOREIGN_JURISDICTIONS": "Disclosure Regarding Foreign Jurisdictions",
            "ITEM_10_DIRECTORS": "Directors, Executive Officers and Corporate Governance",
            "ITEM_11_EXECUTIVE_COMPENSATION": "Executive Compensation",
            "ITEM_12_SECURITY_OWNERSHIP": "Security Ownership of Certain Beneficial Owners and Management",
            "ITEM_13_RELATED_TRANSACTIONS": "Certain Relationships and Related Transactions",
            "ITEM_14_PRINCIPAL_ACCOUNTANT": "Principal Accountant Fees and Services",
            "ITEM_15_EXHIBITS": "Exhibits, Financial Statement Schedules",
            "ITEM_16_FORM_10K_SUMMARY": "Form 10-K Summary",

            # Form 10-Q Items
            "ITEM_1_FINANCIAL_STATEMENTS": "Financial Statements",
            "ITEM_2_MD_AND_A": "Management's Discussion and Analysis",
            "ITEM_3_MARKET_RISK": "Quantitative and Qualitative Disclosures About Market Risk",
            "ITEM_4_CONTROLS": "Controls and Procedures",
            "ITEM_1_LEGAL_PROCEEDINGS": "Legal Proceedings",
            "ITEM_1A_RISK_FACTORS_Q": "Risk Factors (10-Q)",
            "ITEM_2_UNREGISTERED_SALES": "Unregistered Sales of Equity Securities",
            "ITEM_3_DEFAULTS": "Defaults Upon Senior Securities",
            "ITEM_4_MINE_SAFETY_Q": "Mine Safety Disclosures (10-Q)",
            "ITEM_5_OTHER_INFORMATION_Q": "Other Information (10-Q)",
            "ITEM_6_EXHIBITS_Q": "Exhibits (10-Q)",

            # Common statement sections
            "MANAGEMENT_DISCUSSION": "Management's Discussion and Analysis",
            "CONSOLIDATED_BALANCE_SHEET": "Consolidated Balance Sheet",
            "CONSOLIDATED_INCOME_STATEMENT": "Consolidated Income Statement",
            "CONSOLIDATED_CASH_FLOW": "Consolidated Cash Flow Statement",
            "RESULTS_OF_OPERATIONS": "Results of Operations",
            "LIQUIDITY_AND_CAPITAL": "Liquidity and Capital Resources",
            "CRITICAL_ACCOUNTING": "Critical Accounting Policies"
        }

    def generate_llm_format(self, parsed_xbrl, filing_metadata):
        """
        Generate LLM-native format from parsed XBRL and narrative text

        Args:
            parsed_xbrl: Parsed XBRL data
            filing_metadata: Filing metadata including any narrative content

        Returns:
            LLM-formatted content as string
        """
        if "error" in parsed_xbrl:
            return f"ERROR: {parsed_xbrl['error']}"

        # Integrate XBRL mapping
        try:
            # Use our XBRL mapping integration to enhance the parsed XBRL data
            parsed_xbrl = xbrl_mapping_integration.integrate_xbrl_mapping(parsed_xbrl, filing_metadata)

            # If we have LLM-friendly output from our XBRL mapping integration, use it
            if "llm_friendly_output" in parsed_xbrl:
                logging.info("Using LLM-friendly output from XBRL mapping integration")
                return parsed_xbrl["llm_friendly_output"]
        except Exception as e:
            logging.error(f"Error integrating XBRL mapping: {str(e)}")
            # Continue with standard processing if XBRL mapping integration fails

        # Define priority sections at the very beginning to ensure it's available throughout the method
        # Include all possible 10-K and 10-Q sections
        priority_sections = [
            # Form 10-K main Items (in order)
            "ITEM_1_BUSINESS",
            "ITEM_1A_RISK_FACTORS",
            "ITEM_1B_UNRESOLVED_STAFF_COMMENTS",
            "ITEM_1C_CYBERSECURITY",
            "ITEM_2_PROPERTIES",
            "ITEM_3_LEGAL_PROCEEDINGS",
            "ITEM_4_MINE_SAFETY_DISCLOSURES",
            "ITEM_5_MARKET",
            "ITEM_6_SELECTED_FINANCIAL_DATA",
            "ITEM_7_MD_AND_A",
            "ITEM_7A_MARKET_RISK",
            "ITEM_8_FINANCIAL_STATEMENTS",
            "ITEM_9_DISAGREEMENTS",
            "ITEM_9A_CONTROLS",
            "ITEM_9B_OTHER_INFORMATION",
            "ITEM_9C_FOREIGN_JURISDICTIONS",
            "ITEM_10_DIRECTORS",
            "ITEM_11_EXECUTIVE_COMPENSATION",
            "ITEM_12_SECURITY_OWNERSHIP",
            "ITEM_13_RELATIONSHIPS",
            "ITEM_14_ACCOUNTANT_FEES",
            "ITEM_15_EXHIBITS",
            "ITEM_16_SUMMARY",

            # Form 10-Q main Items (in order)
            "ITEM_1_FINANCIAL_STATEMENTS",
            "ITEM_2_MD_AND_A",
            "ITEM_3_MARKET_RISK",
            "ITEM_4_CONTROLS",
            "ITEM_1_LEGAL_PROCEEDINGS",
            "ITEM_1A_RISK_FACTORS_Q",
            "ITEM_2_UNREGISTERED_SALES",
            "ITEM_3_DEFAULTS",
            "ITEM_4_MINE_SAFETY_Q",
            "ITEM_5_OTHER_INFORMATION_Q",
            "ITEM_6_EXHIBITS_Q",

            # Common MD&A subsections
            "MANAGEMENT_DISCUSSION",
            "RESULTS_OF_OPERATIONS",
            "LIQUIDITY_AND_CAPITAL",
            "CRITICAL_ACCOUNTING"
        ]

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

        # Initialize context mapping from HTML as an instance variable so it's accessible throughout the method
        self.context_mapping_from_html = {}

        output = []

        # Add document metadata
        ticker = filing_metadata.get("ticker", "unknown")
        filing_type = filing_metadata.get("filing_type", "unknown")
        company_name = filing_metadata.get("company_name", "unknown")
        cik = filing_metadata.get("cik", "unknown")
        filing_date = filing_metadata.get("filing_date", "unknown")
        period_end = filing_metadata.get("period_end_date", "unknown")
        fiscal_year = filing_metadata.get("fiscal_year", "")
        fiscal_period = filing_metadata.get("fiscal_period", "")

        # Enhanced document structure with metadata headers
        output.append(f"@DOCUMENT: {ticker}-{filing_type}-{period_end}")
        output.append(f"@FILING_DATE: {filing_date}")
        output.append(f"@COMPANY: {company_name}")
        output.append(f"@CIK: {cik}")
        if fiscal_year:
            output.append(f"@FISCAL_YEAR: {fiscal_year}")
        if fiscal_period:
            output.append(f"@FISCAL_PERIOD: {fiscal_period}")

        # Add structure metadata headers
        output.append("")
        output.append("@STRUCTURE: Financial_Statement")
        output.append("@MAIN_CATEGORIES: Revenues, Cost_of_Revenues, Gross_Profit, Operating_Expenses, Operating_Income, Net_Income")

        # Expanded document structure information
        if filing_type == "10-K":
            output.append("@STATEMENT_TYPES: Income_Statement, Balance_Sheet, Cash_Flow_Statement, Statement_Of_Equity")
            output.append("@DOCUMENT_PARTS: Part_I (Items_1-4), Part_II (Items_5-9), Part_III (Items_10-14), Part_IV (Items_15-16)")
            output.append("@ALL_SECTIONS: Item_1_Business, Item_1A_Risk_Factors, Item_1B_Unresolved_Comments, Item_1C_Cybersecurity, Item_2_Properties, Item_3_Legal, Item_4_Mine_Safety, Item_5_Market, Item_6_Selected_Financial, Item_7_MD&A, Item_7A_Market_Risk, Item_8_Financial_Statements, Item_9_Accountant_Changes, Item_9A_Controls, Item_9B_Other, Item_9C_Foreign_Jurisdictions, Item_10_Directors, Item_11_Compensation, Item_12_Security_Ownership, Item_13_Related_Transactions, Item_14_Accountant_Fees, Item_15_Exhibits, Item_16_Summary")
        else:
            output.append("@STATEMENT_TYPES: Income_Statement, Balance_Sheet, Cash_Flow_Statement, Statement_Of_Equity")
            output.append("@DOCUMENT_PARTS: Part_I (Items_1-2), Part_II (Items_3-6)")
            output.append("@ALL_SECTIONS: Item_1_Financial_Statements, Item_2_MD&A, Item_3_Market_Risk, Item_4_Controls, Item_1_Legal_Proceedings, Item_1A_Risk_Factors, Item_2_Unregistered_Sales, Item_3_Defaults, Item_4_Mine_Safety, Item_5_Other, Item_6_Exhibits")

        output.append("@SUBCATEGORIES_REVENUES: Product_Revenue, Service_Revenue, Total_Revenue")
        output.append("@SUBCATEGORIES_EXPENSES: Cost_of_Revenue, Research_Development, Sales_Marketing, General_Administrative")
        output.append("")

        # Create enhanced context data dictionary
        context_map = {}
        context_reference_guide = {}  # Store context details for the reference guide
        context_code_map = {}  # For explicitly labeled context references

        # Start context section with data dictionary format
        output.append("@DATA_DICTIONARY: CONTEXTS")

        # Check if we need to extract contexts from HTML content
        if (not parsed_xbrl.get("contexts") or len(parsed_xbrl.get("contexts", {})) == 0):
            # First try to extract from HTML if available
            html_content = None

            # Log metadata keys for debugging
            logging.info(f"Metadata keys: {', '.join(filing_metadata.keys())}")

            # Check multiple sources for HTML content
            if "html_content" in filing_metadata and isinstance(filing_metadata["html_content"], str):
                # Direct HTML content in metadata
                html_content = filing_metadata["html_content"]
                logging.info(f"Using HTML content from metadata ({len(html_content)} bytes)")
            elif "html_file" in filing_metadata:
                # HTML file path in metadata
                html_path = filing_metadata["html_file"]
                logging.info(f"Found html_file in metadata: {html_path}")
                if os.path.exists(html_path):
                    try:
                        with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                            html_content = f.read()
                            logging.info(f"Loaded HTML content from html_file: {html_path} ({len(html_content)} bytes)")
                    except Exception as e:
                        logging.error(f"Error loading HTML from html_file: {str(e)}")
                else:
                    logging.error(f"HTML file path does not exist: {html_path}")
            elif "doc_path" in filing_metadata:
                # Document path in metadata
                html_path = filing_metadata.get("doc_path", "")
                logging.info(f"Found doc_path in metadata: {html_path}")
                if html_path and os.path.exists(html_path):
                    try:
                        with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                            html_content = f.read()
                            logging.info(f"Loaded HTML content from doc_path: {html_path} ({len(html_content)} bytes)")
                    except Exception as e:
                        logging.error(f"Error loading HTML from doc_path: {str(e)}")
                else:
                    logging.error(f"Document path does not exist or is empty: {html_path}")
            else:
                logging.warning("No HTML content source found in metadata. Available keys: " + ", ".join(filing_metadata.keys()))

            # Process HTML content if we have it
            if html_content:
                try:
                    # Use the context extractor module to extract contexts
                    extracted_contexts = extract_contexts_from_html(html_content, filing_metadata)

                    if extracted_contexts:
                        # Update the contexts in parsed_xbrl
                        parsed_xbrl["contexts"] = extracted_contexts
                        num_contexts = len(extracted_contexts)
                        logging.info(f"Successfully extracted {num_contexts} contexts from HTML file")

                        # Create context mapping for simple context IDs
                        for context_id, context_data in extracted_contexts.items():
                            period_info = context_data.get("period", {})
                            if period_info:
                                # Store the period info directly
                                self.context_mapping_from_html[context_id] = period_info

                                # Log details for debugging
                                if re.match(r'^c-\d+$', context_id):
                                    if "startDate" in period_info and "endDate" in period_info:
                                        logging.info(f"Saved duration context {context_id}: {period_info['startDate']} to {period_info['endDate']}")
                                    elif "instant" in period_info:
                                        logging.info(f"Saved instant context {context_id}: {period_info['instant']}")

                        # Verify that context mapping has been populated
                        simple_contexts = [c_id for c_id in self.context_mapping_from_html.keys() if re.match(r'^c-\d+$', c_id)]
                        logging.info(f"Populated context_mapping_from_html with {len(simple_contexts)} simple contexts (c-1, c-2, etc.)")

                        # Create human-readable labels
                        context_labels = map_contexts_to_periods(extracted_contexts, filing_metadata)
                        for context_id, label in context_labels.items():
                            context_map[context_id] = label

                        # Add a note to the output
                        output.append(f"@NOTE: Found {num_contexts} contexts via direct HTML extraction")
                except Exception as e:
                    logging.error(f"Error extracting contexts from HTML: {str(e)}")
                    logging.error(f"HTML content length: {len(html_content) if html_content else 0}")
                    logging.error(f"HTML content type: {type(html_content)}")
                    logging.error(f"HTML content first 100 chars: {html_content[:100] if html_content else ''}")

            # If we still don't have contexts, try to extract from context_refs in facts
            if not parsed_xbrl.get("contexts") or len(parsed_xbrl.get("contexts", {})) == 0:
                # Try to extract context information from facts
                context_refs = set()
                extracted_contexts = {}

                for fact in parsed_xbrl.get("facts", []):
                    context_ref = fact.get("context_ref", fact.get("contextRef", ""))
                    if context_ref and context_ref not in context_refs:
                        context_refs.add(context_ref)
                        context_data = {"id": context_ref, "period": {}}

                        # Try to extract date info from context_ref patterns
                        # Format 1: C_0000789019_20200701_20210630 (duration with CIK)
                        c_duration_match = re.search(r'C_\d+_(\d{8})_(\d{8})', context_ref)

                        # Format 2: C_0000789019_20200701 (instant with CIK)
                        c_instant_match = re.search(r'C_\d+_(\d{8})$', context_ref)

                        # Format 3: _D20200701-20210630 (standard duration)
                        d_match = re.search(r'_D(\d{8})-(\d{8})', context_ref)

                        # Format 4: _I20200701 (standard instant)
                        i_match = re.search(r'_I(\d{8})', context_ref)

                        # Format 5: Look for dates in context IDs with _I or _D prefixes
                        id_match = re.search(r'_[DI](\d{8})', context_ref)

                        # Format 6: Look for context IDs with "I" or "D" followed by date(s)
                        date_match = re.search(r'[\s_]([DI])(\d{8})', context_ref)

                        # Format 7: NVDA format with embedded dates (e.g., i2c5e111a942340e08ad1e8d2e3b0fb71_D20210201-20220130)
                        nvda_duration_match = re.search(r'i[a-z0-9]+_D(\d{8})-(\d{8})', context_ref)

                        # Format 8: NVDA format with embedded instant date (e.g., i2c5e111a942340e08ad1e8d2e3b0fb71_I20210201)
                        nvda_instant_match = re.search(r'i[a-z0-9]+_I(\d{8})', context_ref)

                        # Check each format
                        if c_duration_match:
                            start_date_str = c_duration_match.group(1)
                            end_date_str = c_duration_match.group(2)
                            formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                            formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                            context_data["period"] = {"startDate": formatted_start, "endDate": formatted_end}
                        elif c_instant_match:
                            date_str = c_instant_match.group(1)
                            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                            context_data["period"] = {"instant": formatted_date}
                        elif d_match:
                            start_date_str = d_match.group(1)
                            end_date_str = d_match.group(2)
                            formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                            formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                            context_data["period"] = {"startDate": formatted_start, "endDate": formatted_end}
                        elif i_match:
                            date_str = i_match.group(1)
                            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                            context_data["period"] = {"instant": formatted_date}
                        elif nvda_duration_match:
                            start_date_str = nvda_duration_match.group(1)
                            end_date_str = nvda_duration_match.group(2)
                            formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                            formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                            context_data["period"] = {"startDate": formatted_start, "endDate": formatted_end}
                        elif nvda_instant_match:
                            date_str = nvda_instant_match.group(1)
                            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                            context_data["period"] = {"instant": formatted_date}

                        # Store the context data
                        if context_data["period"]:
                            extracted_contexts[context_ref] = context_data

                # Update parsed_xbrl with the extracted contexts
                if extracted_contexts:
                    parsed_xbrl["contexts"] = extracted_contexts
                    num_contexts = len(extracted_contexts)
                    logging.info(f"Successfully extracted {num_contexts} contexts from fact context_refs")
                    output.append(f"@NOTE: Found {num_contexts} contexts from context ID patterns")

        # Collect all unique context periods for the reference guide
        period_contexts = []  # Duration contexts (with start and end dates)
        instant_contexts = []  # Instant contexts (with a single date)

        # If there are no contexts but we have facts with context_refs, create implicit contexts from them
        if (not parsed_xbrl.get("contexts") or len(parsed_xbrl.get("contexts", {})) == 0) and parsed_xbrl.get("facts"):
            implicit_contexts = {}
            for fact in parsed_xbrl.get("facts", []):
                context_ref = fact.get("context_ref", fact.get("contextRef", ""))
                if not context_ref:
                    continue

                # Only process each context_ref once
                if context_ref in implicit_contexts:
                    continue

                # Create a minimal context entry
                context_data = {"id": context_ref, "period": {}}

                # Try to extract date info from context_ref patterns
                # Format 1: C_0000789019_20200701_20210630 (duration with CIK)
                c_duration_match = re.search(r'C_\d+_(\d{8})_(\d{8})', context_ref)

                # Format 2: C_0000789019_20200701 (instant with CIK)
                c_instant_match = re.search(r'C_\d+_(\d{8})$', context_ref)

                # Format 3: _D20200701-20210630 (standard duration)
                d_match = re.search(r'_D(\d{8})-(\d{8})', context_ref)

                # Format 4: _I20200701 (standard instant)
                i_match = re.search(r'_I(\d{8})', context_ref)

                if c_duration_match:
                    start_date_str = c_duration_match.group(1)
                    end_date_str = c_duration_match.group(2)
                    formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                    formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                    context_data["period"] = {"startDate": formatted_start, "endDate": formatted_end}

                    # Add to period contexts
                    period_contexts.append({
                        "context_id": context_ref,
                        "start_date": formatted_start,
                        "end_date": formatted_end,
                        "description": f"Period from {formatted_start} to {formatted_end}",
                        "segment": "Consolidated"  # Default value
                    })
                elif c_instant_match:
                    date_str = c_instant_match.group(1)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    context_data["period"] = {"instant": formatted_date}

                    # Add to instant contexts
                    instant_contexts.append({
                        "context_id": context_ref,
                        "date": formatted_date,
                        "description": f"As of {formatted_date}",
                        "segment": "Consolidated"  # Default value
                    })
                elif d_match:
                    start_date_str = d_match.group(1)
                    end_date_str = d_match.group(2)
                    formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                    formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                    context_data["period"] = {"startDate": formatted_start, "endDate": formatted_end}

                    # Add to period contexts
                    period_contexts.append({
                        "context_id": context_ref,
                        "start_date": formatted_start,
                        "end_date": formatted_end,
                        "description": f"Period from {formatted_start} to {formatted_end}",
                        "segment": "Consolidated"  # Default value
                    })
                elif i_match:
                    date_str = i_match.group(1)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    context_data["period"] = {"instant": formatted_date}

                    # Add to instant contexts
                    instant_contexts.append({
                        "context_id": context_ref,
                        "date": formatted_date,
                        "description": f"As of {formatted_date}",
                        "segment": "Consolidated"  # Default value
                    })

                # Store the context data
                if context_data["period"]:
                    implicit_contexts[context_ref] = context_data

            if implicit_contexts:
                logging.info(f"Created {len(implicit_contexts)} implicit contexts from context IDs")
                output.append(f"@NOTE: Created {len(implicit_contexts)} implicit contexts from context IDs")
                parsed_xbrl["contexts"] = implicit_contexts

        # Process all contexts for the reference guide
        for context_id, context in parsed_xbrl.get("contexts", {}).items():
            period_info = context.get("period", {})

            # First try to extract date information from context data
            if "startDate" in period_info and "endDate" in period_info:
                # Duration context
                period_contexts.append({
                    "context_id": context_id,
                    "start_date": period_info["startDate"],
                    "end_date": period_info["endDate"],
                    "description": f"Period from {period_info['startDate']} to {period_info['endDate']}",
                    "segment": self._get_segment_info(context)
                })
            elif "instant" in period_info:
                # Instant context
                instant_contexts.append({
                    "context_id": context_id,
                    "date": period_info["instant"],
                    "description": f"As of {period_info['instant']}",
                    "segment": self._get_segment_info(context)
                })
            else:
                # Try to extract from context ID if period info not available

                # Format 1: C_0000789019_20200701_20210630 (duration with CIK)
                c_duration_match = re.search(r'C_\d+_(\d{8})_(\d{8})', context_id)

                # Format 2: C_0000789019_20200701 (instant with CIK)
                c_instant_match = re.search(r'C_\d+_(\d{8})$', context_id)

                # Format 3: _D20200701-20210630 (standard duration)
                d_match = re.search(r'_D(\d{8})-(\d{8})', context_id)

                # Format 4: _I20200701 (standard instant)
                i_match = re.search(r'_I(\d{8})', context_id)

                # Format 7: NVDA format with embedded dates (e.g., i2c5e111a942340e08ad1e8d2e3b0fb71_D20210201-20220130)
                nvda_duration_match = re.search(r'i[a-z0-9]+_D(\d{8})-(\d{8})', context_id)

                # Format 8: NVDA format with embedded instant date (e.g., i2c5e111a942340e08ad1e8d2e3b0fb71_I20210201)
                nvda_instant_match = re.search(r'i[a-z0-9]+_I(\d{8})', context_id)

                if c_duration_match:
                    # Duration with CIK
                    start_date_str = c_duration_match.group(1)
                    end_date_str = c_duration_match.group(2)

                    formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                    formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"

                    period_contexts.append({
                        "context_id": context_id,
                        "start_date": formatted_start,
                        "end_date": formatted_end,
                        "description": f"Period from {formatted_start} to {formatted_end}",
                        "segment": self._get_segment_info(context)
                    })
                elif c_instant_match:
                    # Instant with CIK
                    date_str = c_instant_match.group(1)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                    instant_contexts.append({
                        "context_id": context_id,
                        "date": formatted_date,
                        "description": f"As of {formatted_date}",
                        "segment": self._get_segment_info(context)
                    })
                elif d_match:
                    # Standard duration
                    start_date_str = d_match.group(1)
                    end_date_str = d_match.group(2)

                    formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                    formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"

                    period_contexts.append({
                        "context_id": context_id,
                        "start_date": formatted_start,
                        "end_date": formatted_end,
                        "description": f"Period from {formatted_start} to {formatted_end}",
                        "segment": self._get_segment_info(context)
                    })
                elif i_match:
                    # Standard instant
                    date_str = i_match.group(1)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                    instant_contexts.append({
                        "context_id": context_id,
                        "date": formatted_date,
                        "description": f"As of {formatted_date}",
                        "segment": self._get_segment_info(context)
                    })
                elif nvda_duration_match:
                    # NVDA duration format
                    start_date_str = nvda_duration_match.group(1)
                    end_date_str = nvda_duration_match.group(2)

                    formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                    formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"

                    period_contexts.append({
                        "context_id": context_id,
                        "start_date": formatted_start,
                        "end_date": formatted_end,
                        "description": f"Period from {formatted_start} to {formatted_end}",
                        "segment": self._get_segment_info(context)
                    })
                elif nvda_instant_match:
                    # NVDA instant format
                    date_str = nvda_instant_match.group(1)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                    instant_contexts.append({
                        "context_id": context_id,
                        "date": formatted_date,
                        "description": f"As of {formatted_date}",
                        "segment": self._get_segment_info(context)
                    })

        # Assign short codes to contexts for easier reference (c-1, c-2, etc.)
        context_counter = 0

        for context_id, context in parsed_xbrl.get("contexts", {}).items():
            period_info = context.get("period", {})
            readable_label = ""
            context_counter += 1
            context_code = f"c-{context_counter}"

            # Generate human-readable period labels for contexts
            if "startDate" in period_info and "endDate" in period_info:
                try:
                    start_year = period_info['startDate'].split('-')[0]
                    end_year = period_info['endDate'].split('-')[0]
                    end_month = int(period_info['endDate'].split('-')[1])

                    # Check for segment/dimension information to add context detail
                    segment_info = ""
                    segment_text = ""
                    entity_info = context.get("entity", {})
                    segment_data = entity_info.get("segment", {})

                    # Extract segment/business unit information if available
                    if segment_data:
                        # Look for common segment dimensions like business unit or geography
                        for dimension_key, dimension_value in segment_data.items():
                            if isinstance(dimension_value, str):
                                # Clean up dimension names
                                dim_name = dimension_key.split(":")[-1] if ":" in dimension_key else dimension_key
                                dim_name = dim_name.replace("Segment", "").replace("Dimension", "")

                                # Clean up dimension values
                                dim_value = dimension_value.split(":")[-1] if ":" in dimension_value else dimension_value

                                if not segment_info:
                                    segment_info = f"{dim_name}_{dim_value}"
                                    segment_text = f"{dim_name}: {dim_value}"
                                else:
                                    segment_info += f"_{dim_value}"
                                    segment_text += f", {dim_name}: {dim_value}"

                    # Create a more descriptive meaningful code that shows what the context represents
                    if start_year == end_year:
                        # Same year period - could be quarterly or annual
                        if filing_type == "10-K":
                            readable_label = f"FY{end_year}"
                            category_label = f"Annual_{end_year}"

                            # Create more descriptive context code
                            semantic_code = f"FY{end_year}"
                            if segment_info:
                                semantic_code += f"_{segment_info}"
                                readable_label += f" ({segment_text})"
                        else:
                            # Map month to quarter (approximate)
                            quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                            # Get closest quarter
                            closest_month = min(quarter_map.keys(), key=lambda x: abs(x - end_month))
                            quarter_code = quarter_map[closest_month]

                            readable_label = f"{end_year}_{quarter_code}"
                            category_label = f"Quarter{quarter_code}_{end_year}"

                            # Create more descriptive context code
                            semantic_code = f"{end_year}_{quarter_code}"
                            if segment_info:
                                semantic_code += f"_{segment_info}"
                                readable_label += f" ({segment_text})"
                    else:
                        # Multi-year period - likely annual
                        readable_label = f"FY{end_year}"
                        category_label = f"Annual_{end_year}"

                        # Create more descriptive context code
                        semantic_code = f"FY{end_year}"
                        if segment_info:
                            semantic_code += f"_{segment_info}"
                            readable_label += f" ({segment_text})"

                    # Store mapping from context ID to readable label
                    context_map[context_id] = readable_label
                    context_code_map[context_id] = context_code

                    # Create a human-readable description for the context
                    consolidated_txt = "Consolidated" if not segment_info else f"{segment_text}"
                    if filing_type == "10-K":
                        period_description = f"{consolidated_txt} – Year ended {period_info['endDate']}"
                    else:
                        # Calculate period duration in days
                        try:
                            start_date = datetime.datetime.strptime(period_info['startDate'], "%Y-%m-%d")
                            end_date = datetime.datetime.strptime(period_info['endDate'], "%Y-%m-%d")
                            duration_days = (end_date - start_date).days

                            if 85 <= duration_days <= 95:
                                period_description = f"{consolidated_txt} – Quarter ended {period_info['endDate']}"
                            elif 175 <= duration_days <= 190:
                                period_description = f"{consolidated_txt} – Six months ended {period_info['endDate']}"
                            elif 265 <= duration_days <= 280:
                                period_description = f"{consolidated_txt} – Nine months ended {period_info['endDate']}"
                            else:
                                period_description = f"{consolidated_txt} – Period from {period_info['startDate']} to {period_info['endDate']}"
                        except:
                            period_description = f"{consolidated_txt} – Period from {period_info['startDate']} to {period_info['endDate']}"

                    # Store information for context reference guide
                    context_reference_guide[readable_label] = {
                        "type": "period",
                        "code": context_code,
                        "semantic_code": semantic_code,
                        "category": category_label,
                        "segment": segment_text if segment_text else "Consolidated",
                        "start_date": period_info['startDate'],
                        "end_date": period_info['endDate'],
                        "description": period_description
                    }

                    # Enhanced explicit context labeling using both shortcode and semantic code
                    output.append(f"{context_code} | @CODE: {semantic_code} | {category_label} | Period: {period_info['startDate']} to {period_info['endDate']}")
                    if segment_text:
                        output.append(f"  @SEGMENT: {segment_text} | @LABEL: {readable_label}")
                    else:
                        output.append(f"  @SEGMENT: Consolidated | @LABEL: {readable_label}")
                    output.append(f"  @DESCRIPTION: {period_description}")
                except Exception as e:
                    # Fallback if parsing fails
                    logging.warning(f"Context parsing error: {str(e)}")
                    output.append(f"{context_code}: Period: {period_info['startDate']} to {period_info['endDate']}")
            elif "instant" in period_info:
                # For instant dates (like balance sheet dates)
                try:
                    year = period_info['instant'].split('-')[0]
                    month = int(period_info['instant'].split('-')[1])

                    # Check for segment/dimension information to add context detail
                    segment_info = ""
                    segment_text = ""
                    entity_info = context.get("entity", {})
                    segment_data = entity_info.get("segment", {})

                    # Extract segment/business unit information if available
                    if segment_data:
                        # Look for common segment dimensions like business unit or geography
                        for dimension_key, dimension_value in segment_data.items():
                            if isinstance(dimension_value, str):
                                # Clean up dimension names
                                dim_name = dimension_key.split(":")[-1] if ":" in dimension_key else dimension_key
                                dim_name = dim_name.replace("Segment", "").replace("Dimension", "")

                                # Clean up dimension values
                                dim_value = dimension_value.split(":")[-1] if ":" in dimension_value else dimension_value

                                if not segment_info:
                                    segment_info = f"{dim_name}_{dim_value}"
                                    segment_text = f"{dim_name}: {dim_value}"
                                else:
                                    segment_info += f"_{dim_value}"
                                    segment_text += f", {dim_name}: {dim_value}"

                    # Determine if this is a balance sheet date or other type of instant
                    # Check for balance sheet date by examining the date itself
                    date_obj = datetime.datetime.strptime(period_info['instant'], "%Y-%m-%d")
                    is_balance_sheet = False
                    date_str = date_obj.strftime("%b %d, %Y")

                    # For 10-K filings, check if this is the fiscal year end date
                    if filing_type == "10-K":
                        readable_label = f"FY{year}_END"
                        category_label = f"Annual_{year}_End"
                        is_balance_sheet = True
                        statement_type = "Balance_Sheet"
                    else:
                        # Map month to quarter (approximate)
                        quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                        # Get closest quarter
                        closest_month = min(quarter_map.keys(), key=lambda x: abs(x - month))
                        quarter_code = quarter_map[closest_month]
                        readable_label = f"{year}_{quarter_code}_END"
                        category_label = f"Quarter{quarter_code}_{year}_End"
                        is_balance_sheet = True
                        statement_type = "Balance_Sheet"

                    # Create more descriptive context code that conveys meaning
                    if is_balance_sheet:
                        semantic_code = f"BS_{year}_{month:02d}_{date_obj.day:02d}"
                    else:
                        semantic_code = f"INST_{year}_{month:02d}_{date_obj.day:02d}"

                    # Add segment information if available
                    if segment_info:
                        semantic_code += f"_{segment_info}"
                        readable_label += f" ({segment_text})"

                    # Store mapping
                    context_map[context_id] = readable_label
                    context_code_map[context_id] = context_code

                    # Create a human-readable description for the context
                    consolidated_txt = "Consolidated" if not segment_info else f"{segment_text}"
                    if is_balance_sheet:
                        period_description = f"{consolidated_txt} – Balance Sheet as of {date_str}"
                    else:
                        period_description = f"{consolidated_txt} – Point in time at {date_str}"

                    # Store information for context reference guide
                    context_reference_guide[readable_label] = {
                        "type": "instant",
                        "code": context_code,
                        "semantic_code": semantic_code,
                        "category": category_label,
                        "segment": segment_text if segment_text else "Consolidated",
                        "statement_type": statement_type if is_balance_sheet else "Other",
                        "date": period_info['instant'],
                        "description": period_description
                    }

                    # Enhanced explicit context labeling with both shortcode and semantic code
                    output.append(f"{context_code} | @CODE: {semantic_code} | {category_label} | Instant: {period_info['instant']}")
                    if segment_text:
                        output.append(f"  @SEGMENT: {segment_text} | @LABEL: {readable_label}")
                    else:
                        output.append(f"  @SEGMENT: Consolidated | @LABEL: {readable_label}")
                    output.append(f"  @DESCRIPTION: {period_description}")
                    if is_balance_sheet:
                        output.append(f"  @STATEMENT_TYPE: Balance_Sheet")
                except Exception as e:
                    # Fallback if parsing fails
                    logging.warning(f"Instant context parsing error: {str(e)}")
                    output.append(f"{context_code}: Instant: {period_info['instant']}")
        output.append("")

        # Extract and analyze units and scales
        units_info = {}
        decimals_info = {}

        # First pass to gather unit and decimal information
        for fact in parsed_xbrl.get("facts", []):
            unit_ref = fact.get("unit_ref", "")
            decimals = fact.get("decimals", "")

            if unit_ref:
                if unit_ref not in units_info:
                    units_info[unit_ref] = 0
                units_info[unit_ref] += 1

            if decimals:
                try:
                    decimal_val = safe_parse_decimals(decimals)
                    if decimal_val not in decimals_info:
                        decimals_info[decimal_val] = 0
                    decimals_info[decimal_val] += 1
                except:
                    pass

        # Determine most common scale for monetary values
        most_common_decimal = None
        max_count = 0
        for decimal_val, count in decimals_info.items():
            if count > max_count:
                most_common_decimal = decimal_val
                max_count = count

        # Interpret scale
        scale_description = ""
        if most_common_decimal is not None:
            if most_common_decimal == 0:
                scale_description = "exact units"
            elif most_common_decimal == -3:
                scale_description = "thousands"
            elif most_common_decimal == -6:
                scale_description = "millions"
            elif most_common_decimal == -9:
                scale_description = "billions"

        # Add enhanced units section with scaling information
        output.append("@UNITS_AND_SCALING")

        # Explicitly state the scaling for monetary values
        if scale_description:
            output.append(f"@MONETARY_SCALE: {scale_description}")
            if most_common_decimal == -6:
                output.append("@SCALE_NOTE: All dollar amounts are in millions unless otherwise specified")
            elif most_common_decimal == -3:
                output.append("@SCALE_NOTE: All dollar amounts are in thousands unless otherwise specified")
            elif most_common_decimal == -9:
                output.append("@SCALE_NOTE: All dollar amounts are in billions unless otherwise specified")

        # Document unit codes
        for unit_id, unit_value in parsed_xbrl.get("units", {}).items():
            if unit_id.lower() == "usd":
                output.append(f"@UNIT_DEF: {unit_id} | United States Dollars (USD)")
            elif unit_id.lower() in ["eur", "gbp", "jpy", "cad"]:
                currency_map = {"eur": "Euros", "gbp": "British Pounds", "jpy": "Japanese Yen", "cad": "Canadian Dollars"}
                output.append(f"@UNIT_DEF: {unit_id} | {currency_map.get(unit_id.lower(), unit_value)}")
            elif unit_id.lower() == "shares":
                output.append(f"@UNIT_DEF: {unit_id} | Number of equity shares")
            else:
                output.append(f"@UNIT_DEF: {unit_id} | {unit_value}")

        # Document and explain decimals usage
        output.append("@DECIMALS_USAGE:")
        for decimal_val, count in sorted(decimals_info.items()):
            if decimal_val == 0:
                output.append(f"  {decimal_val}: Exact values")
            elif decimal_val == -3:
                output.append(f"  {decimal_val}: Values rounded to thousands")
            elif decimal_val == -6:
                output.append(f"  {decimal_val}: Values rounded to millions")
            elif decimal_val == -9:
                output.append(f"  {decimal_val}: Values rounded to billions")
            else:
                output.append(f"  {decimal_val}: Values rounded to {10**(-decimal_val)} decimal places")

        output.append("")

        # Check for narrative content
        extracted_sections = {}

        # Try to get narrative content from filing_metadata
        if "html_content" in filing_metadata and isinstance(filing_metadata["html_content"], dict) and "document_sections" in filing_metadata["html_content"]:
            html_content = filing_metadata["html_content"]
            document_sections = html_content.get("document_sections", {})
            for section_id, section_info in document_sections.items():
                if section_id in self.section_to_readable_name:
                    section_text = section_info.get("text", "")
                    if section_text:
                        extracted_sections[section_id] = {
                            "name": self.section_to_readable_name[section_id],
                            "text": section_text
                        }

        # Try to get narrative content from text_file_path if available
        if "text_file_path" in filing_metadata and os.path.exists(filing_metadata["text_file_path"]):
            try:
                with open(filing_metadata["text_file_path"], 'r', encoding='utf-8') as f:
                    text_content = f.read()

                # Extract sections from text file
                section_pattern = r'@SECTION_START: ([A-Z_]+)[\s\S]*?@SECTION_END: \1'
                for match in re.finditer(section_pattern, text_content):
                    section_id = match.group(1)
                    if section_id in self.section_to_readable_name:
                        section_text = match.group(0)
                        # Clean up text by removing section markers
                        section_text = re.sub(r'@SECTION_START: [A-Z_]+\n', '', section_text)
                        section_text = re.sub(r'@SECTION_END: [A-Z_]+', '', section_text)
                        extracted_sections[section_id] = {
                            "name": self.section_to_readable_name[section_id],
                            "text": section_text.strip()
                        }
            except Exception as e:
                logging.warning(f"Could not extract sections from text file: {str(e)}")

        # Add key financial facts with improved organization using the financial statement organizer

        # Track table data for integrity checks
        self.data_integrity["xbrl_facts"] = len(parsed_xbrl.get("facts", []))
        self.data_integrity["xbrl_tables_created"] = 0

        # Organize financial statements
        financial_statements = organize_financial_statements(parsed_xbrl)

        # Add financial statements to the output
        if financial_statements:
            output.append("")
            output.append("@FINANCIAL_STATEMENTS_SECTION")
            output.append("")

            # Add each financial statement
            for statement_type, statement_lines in financial_statements.items():
                output.extend(statement_lines)
                output.append("")
                output.append("-" * 80)  # Add a separator between statements
                output.append("")

            # Update data integrity metrics
            self.data_integrity["xbrl_tables_created"] = len(financial_statements)
            self.data_integrity["tables_detected"] += len(financial_statements)
            self.data_integrity["tables_included"] += len(financial_statements)

            # Count total rows in all statements
            total_rows = sum(len(statement_lines) for statement_lines in financial_statements.values())
            self.data_integrity["total_table_rows"] += total_rows

        # Track facts by context reference to build tables
        facts_by_context = {}
        for fact in parsed_xbrl.get("facts", []):
            context_ref = fact.get("context_ref", "")
            if context_ref not in facts_by_context:
                facts_by_context[context_ref] = []
            facts_by_context[context_ref].append(fact)

        # Add facts section
        output.append("")
        output.append("@FACTS_SECTION")
        output.append("")

        # Check if we have any facts to add
        if parsed_xbrl.get("facts", []):
            # Add facts organized by context
            for context_ref, facts_list in facts_by_context.items():
                if facts_list:
                    output.append(f"@CONTEXT: {context_ref}")

                    # Group facts by prefix
                    facts_by_prefix = {}
                    for fact in facts_list:
                        concept = fact.get("concept", "")
                        prefix = concept.split(":")[0] if ":" in concept else ""

                        if prefix not in facts_by_prefix:
                            facts_by_prefix[prefix] = []
                        facts_by_prefix[prefix].append(fact)

                    # Add facts for each prefix
                    for prefix, prefix_facts in facts_by_prefix.items():
                        if prefix:
                            output.append(f"@PREFIX: {prefix}")

                        # Add facts
                        for fact in prefix_facts:
                            concept = fact.get("concept", "")
                            value = fact.get("value", "")
                            unit = fact.get("unit_ref", "")

                            # Remove prefix from concept if it matches the current prefix
                            if prefix and concept.startswith(f"{prefix}:"):
                                concept = concept.split(":", 1)[1]

                            # Add fact
                            if unit:
                                output.append(f"{concept}|{value}|{unit}")
                            else:
                                output.append(f"{concept}|{value}")

            # Also add individual facts as concept blocks for hierarchy extraction
            output.append("")
            output.append("@CONCEPT_BLOCKS")
            output.append("")

            # Add concept blocks for each fact
            for fact in parsed_xbrl.get("facts", []):
                concept = fact.get("concept", "")
                value = fact.get("value", "")
                unit_ref = fact.get("unit_ref", "")
                context_ref = fact.get("context_ref", "")

                # Get context information
                context = parsed_xbrl.get("contexts", {}).get(context_ref, {})
                period = context.get("period", {})

                # Determine date type and dates
                if "instant" in period:
                    date_type = "INSTANT"
                    date = period.get("instant", "")
                    output.append(f"@CONCEPT: {concept}")
                    output.append(f"@VALUE: {value}")
                    output.append(f"@UNIT_REF: {unit_ref}")
                    output.append(f"@CONTEXT_REF: {context_ref}")
                    output.append(f"@DATE_TYPE: {date_type}")
                    output.append(f"@DATE: {date}")
                    output.append("")
                elif "startDate" in period and "endDate" in period:
                    date_type = "DURATION"
                    start_date = period.get("startDate", "")
                    end_date = period.get("endDate", "")
                    output.append(f"@CONCEPT: {concept}")
                    output.append(f"@VALUE: {value}")
                    output.append(f"@UNIT_REF: {unit_ref}")
                    output.append(f"@CONTEXT_REF: {context_ref}")
                    output.append(f"@DATE_TYPE: {date_type}")
                    output.append(f"@START_DATE: {start_date}")
                    output.append(f"@END_DATE: {end_date}")
                    output.append("")
        else:
            # No facts to add, just add a placeholder
            output.append("@CONTEXT_REFERENCE_GUIDE")
            output.append("This section provides a consolidated reference for all time periods used in this document.")
            output.append("")

        # Add individual facts organized by section
        output.append("")
        output.append("@INDIVIDUAL_FACTS_SECTION")
        output.append("")

        # Organize facts by section and store for sorting later
        financial_sections = {
            "INCOME_STATEMENT": [],
            "BALANCE_SHEET": [],
            "CASH_FLOW": [],
            "EQUITY_STATEMENT": [],
            "OTHER_FINANCIAL": []
        }

        # Helper function to determine section for a concept
        def determine_section(concept):
            concept_lower = concept.lower()
            if any(term in concept_lower for term in ["revenue", "sales", "income", "earnings", "eps", "expense", "cost"]):
                return "INCOME_STATEMENT"
            elif any(term in concept_lower for term in ["asset", "liability", "equity", "debt", "cash and", "inventory", "payable", "receivable"]):
                return "BALANCE_SHEET"
            elif any(term in concept_lower for term in ["cashflow", "cash flow", "financing", "investing", "operating"]):
                return "CASH_FLOW"
            elif any(term in concept_lower for term in ["stockholder", "shareholder", "comprehensive", "retained earnings"]):
                return "EQUITY_STATEMENT"
            else:
                return "OTHER_FINANCIAL"

        # Categorize facts by financial section
        for fact in parsed_xbrl.get("facts", []):
            concept = fact.get("concept", "")
            section = determine_section(concept)
            financial_sections[section].append(fact)

        # Add facts from each section
        for section_name, section_facts in financial_sections.items():
            if section_facts:
                output.append(f"@SECTION: {section_name}")
                output.append("")

                # Alternative approach for when context_map is empty or incomplete
                # Group facts by their concept (name) for table rows
                concepts_in_section = {}
                for fact in section_facts:
                    concept = fact.get("concept", "")
                    if ":" in concept:
                        # Get readable concept name
                        concept_name = concept.split(":")[-1]
                        # Make it more readable
                        readable_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', concept_name).title()
                    else:
                        readable_name = concept

                    if readable_name not in concepts_in_section:
                        concepts_in_section[readable_name] = []
                    concepts_in_section[readable_name].append(fact)

                # Find most common contexts for columns (often periods like quarters or years)
                context_counts = {}
                for fact in section_facts:
                    context_ref = fact.get("context_ref", "")
                    if context_ref not in context_counts:
                        context_counts[context_ref] = 0
                    context_counts[context_ref] += 1

                # Sort contexts by frequency (most common first)
                sorted_contexts = sorted(context_counts.items(), key=lambda x: x[1], reverse=True)

                # Get contexts with period information for columns
                contexts_with_period = []
                context_to_period_info = {}  # Store all context period info for reference

                for context_ref, count in sorted_contexts:
                    # Only include contexts that appear multiple times
                    if count >= 3:  # At least 3 facts with this context
                        # Try to get period info from the raw contexts
                        if context_ref in parsed_xbrl.get("contexts", {}):
                            context_info = parsed_xbrl["contexts"][context_ref]
                            period_data = None

                            if "period" in context_info:
                                period_info = context_info["period"]

                                # Store all period info for later reference
                                context_to_period_info[context_ref] = period_info

                                if "instant" in period_info:
                                    date_str = period_info['instant']
                                    try:
                                        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                                        period_type = "Instant"
                                        period_data = {
                                            "date": date_obj,
                                            "label": f"As of {date_obj.strftime('%b %d, %Y')}",
                                            "year": date_obj.year,
                                            "type": period_type,
                                            "context_ref": context_ref,
                                            "sort_key": f"{date_obj.year}{date_obj.month:02d}{date_obj.day:02d}"
                                        }
                                    except ValueError:
                                        # If date parsing fails, use the original string
                                        period_data = {
                                            "date": date_str,
                                            "label": f"As of {date_str}",
                                            "year": date_str[:4],  # Assume YYYY-MM-DD format
                                            "type": "Instant",
                                            "context_ref": context_ref,
                                            "sort_key": date_str.replace("-", "")
                                        }
                                elif "startDate" in period_info and "endDate" in period_info:
                                    start_str = period_info['startDate']
                                    end_str = period_info['endDate']
                                    try:
                                        start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d")
                                        end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d")
                                        delta = (end_date - start_date).days

                                        # Determine if it's a quarter, year, or other period
                                        if 88 <= delta <= 95:  # ~3 months (quarter)
                                            quarter = (end_date.month + 2) // 3  # Calculate quarter from end month
                                            period_type = "Quarter"
                                            period_label = f"Q{quarter} {end_date.year}"
                                        elif 360 <= delta <= 371:  # ~1 year
                                            period_type = "Annual"
                                            period_label = f"FY {end_date.year}"
                                        elif 175 <= delta <= 190:  # ~6 months
                                            period_type = "Semi-annual"
                                            period_label = f"H1 {end_date.year}"
                                        else:
                                            period_type = "Period"
                                            period_label = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"

                                        period_data = {
                                            "start_date": start_date,
                                            "end_date": end_date,
                                            "label": period_label,
                                            "year": end_date.year,
                                            "type": period_type,
                                            "context_ref": context_ref,
                                            "sort_key": f"{end_date.year}{end_date.month:02d}{end_date.day:02d}"
                                        }
                                    except ValueError:
                                        # If date parsing fails, use the original strings
                                        period_data = {
                                            "start_date": start_str,
                                            "end_date": end_str,
                                            "label": f"{start_str} to {end_str}",
                                            "year": end_str[:4],  # Assume YYYY-MM-DD format
                                            "type": "Period",
                                            "context_ref": context_ref,
                                            "sort_key": end_str.replace("-", "")
                                        }

                            if period_data:
                                contexts_with_period.append(period_data)
                        else:
                            # No context info, fallback to ID with count indicator
                            contexts_with_period.append({
                                "label": f"Context {context_ref}",
                                "context_ref": context_ref,
                                "type": "Unknown",
                                "sort_key": f"Z{context_ref}",  # 'Z' prefix to sort after known dates
                                "year": 9999  # High value to sort at the end
                            })

                # Sort contexts chronologically (earliest to latest)
                contexts_with_period.sort(key=lambda x: x.get("sort_key", ""))

                # Convert to the format needed for table creation
                top_contexts = [(ctx["context_ref"], ctx["label"]) for ctx in contexts_with_period]

                # Create a context reference mapping for the table header
                context_mapping = {}
                for ctx in contexts_with_period:
                    context_mapping[ctx["context_ref"]] = ctx["label"]

                # Only create a table if we have enough contexts and concepts
                if len(top_contexts) >= 2 and len(concepts_in_section) >= 3:
                    # Identify the years covered in this table
                    years_covered = sorted(set([ctx.get("year") for ctx in contexts_with_period if "year" in ctx]))
                    period_types = sorted(set([ctx.get("type") for ctx in contexts_with_period if "type" in ctx]))

                    # Create descriptive table header
                    if years_covered and len(years_covered) <= 5:  # Reasonable number to display
                        years_str = ", ".join([str(year) for year in years_covered])
                        header = f"{section_name.replace('_', ' ')} TABLE ({years_str})"
                    else:
                        header = f"{section_name.replace('_', ' ')} TABLE"

                    output.append(f"@TABLE_CONTENT: {header}")

                    # Add a period guide for this table if we have good period info
                    if len(contexts_with_period) > 0 and any("type" in ctx for ctx in contexts_with_period):
                        period_guide = []

                        # Group contexts by type
                        period_by_type = {}
                        for ctx in contexts_with_period:
                            if "type" in ctx and ctx["type"] != "Unknown":
                                if ctx["type"] not in period_by_type:
                                    period_by_type[ctx["type"]] = []
                                period_by_type[ctx["type"]].append(ctx)

                        # Add period reference information
                        for period_type in ["Annual", "Quarter", "Semi-annual", "Instant", "Period"]:
                            if period_type in period_by_type:
                                periods = period_by_type[period_type]
                                if period_type == "Annual":
                                    periods_str = ", ".join([f"{p['label']}" for p in periods])
                                    period_guide.append(f"Annual periods: {periods_str}")
                                elif period_type == "Quarter":
                                    periods_str = ", ".join([f"{p['label']}" for p in periods])
                                    period_guide.append(f"Quarterly periods: {periods_str}")
                                elif period_type == "Instant":
                                    if len(periods) <= 4:  # Keep it concise
                                        periods_str = ", ".join([f"{p['label']}" for p in periods])
                                        period_guide.append(f"As of dates: {periods_str}")

                        # Add the period guide if we have useful information
                        if period_guide:
                            output.append("Period reference: " + "; ".join(period_guide))

                    # Build enhanced table with explicit context references
                    table_rows = []

                    # Add table metadata
                    table_rows.append(f"@TABLE_TYPE: {section_name.replace('_', ' ')}")

                    # Add verification key if available
                    if "Revenues" in section_name or "Revenue" in section_name:
                        verification_contexts = []
                        for context_ref, _ in top_contexts:
                            if context_ref in context_code_map:
                                verification_contexts.append(context_code_map[context_ref])
                        if verification_contexts:
                            table_rows.append(f"@VERIFICATION: Total_Revenue = Sum_of_Revenue_Components for contexts: {', '.join(verification_contexts)}")

                    # Add standardized header structure with Key column
                    header_row = "Concept | Key"
                    for _, context_label in top_contexts:
                        header_row += f" | {context_label}"
                    table_rows.append(header_row)

                    # Add separator row
                    separator = "-" * len("Concept") + " | " + "-" * len("Key")
                    for context_label in [label for _, label in top_contexts]:
                        separator += f" | {'-' * len(context_label)}"
                    table_rows.append(separator)

                    # Analyze XBRL facts to identify hierarchical relationships
                    # Extract hierarchy information from facts and XBRL structure
                    concept_hierarchies = {}
                    subtotal_items = {}
                    parent_child_map = {}

                    # First pass: identify potential total/subtotal items
                    for concept_name, facts in concepts_in_section.items():
                        concept_lower = concept_name.lower()
                        # Identify likely subtotal or total items based on naming patterns
                        is_total = False
                        if "total" in concept_lower or "subtotal" in concept_lower:
                            is_total = True
                        elif any(concept_lower.startswith(prefix) for prefix in ["gross", "net", "operating", "consolidated"]):
                            is_total = True

                        # Mark items that are likely to be subtotals
                        if is_total:
                            subtotal_items[concept_name] = {
                                "level": 0,  # Will be adjusted in the second pass
                                "children": [],
                                "facts": facts
                            }

                    # Second pass: identify parent-child relationships
                    for subtotal_name in subtotal_items:
                        subtotal_lower = subtotal_name.lower()

                        # Find child items that belong to this subtotal
                        # Example: "Total Revenue" would match with items containing "revenue"
                        base_term = None
                        if "total" in subtotal_lower:
                            base_term = subtotal_lower.replace("total", "").strip()
                        elif "subtotal" in subtotal_lower:
                            base_term = subtotal_lower.replace("subtotal", "").strip()

                        if base_term:
                            for concept_name in concepts_in_section:
                                if concept_name != subtotal_name and base_term in concept_name.lower():
                                    # This is likely a child of the subtotal
                                    subtotal_items[subtotal_name]["children"].append(concept_name)
                                    # Track parent-child relationship
                                    parent_child_map[concept_name] = subtotal_name

                    # Sort concepts for hierarchical organization with levels based on relationships
                    # Group related concepts together
                    organized_concepts = {}
                    for concept_name, facts in concepts_in_section.items():
                        # Look for parent categories
                        concept_lower = concept_name.lower()
                        if "revenue" in concept_lower or "sales" in concept_lower:
                            category = "Revenue"
                        elif "cost" in concept_lower or "expense" in concept_lower:
                            category = "Expenses"
                        elif "profit" in concept_lower or "margin" in concept_lower:
                            category = "Profit"
                        elif "asset" in concept_lower:
                            category = "Assets"
                        elif "liabilit" in concept_lower:
                            category = "Liabilities"
                        elif "equity" in concept_lower:
                            category = "Equity"
                        elif "income" in concept_lower or "earnings" in concept_lower:
                            category = "Income"
                        elif "cash" in concept_lower or "flow" in concept_lower:
                            category = "Cash_Flow"
                        else:
                            category = "Other"

                        # Determine this concept's hierarchical level
                        # Level 0 is a root, Level 1 is a subtotal, Level 2+ are detail items
                        hierarchy_level = 0

                        # Check if this is a total/subtotal
                        if concept_name in subtotal_items:
                            hierarchy_level = 1
                        # Check if this is a child of a subtotal
                        elif concept_name in parent_child_map:
                            hierarchy_level = 2

                        # Store hierarchy information with the concept
                        concept_hierarchies[concept_name] = {
                            "level": hierarchy_level,
                            "parent": parent_child_map.get(concept_name, None),
                            "is_total": concept_name in subtotal_items,
                            "category": category
                        }

                        if category not in organized_concepts:
                            organized_concepts[category] = []
                        # Include hierarchy level with concept data
                        organized_concepts[category].append((concept_name, facts, hierarchy_level))

                    # Add data rows with hierarchical organization
                    for category in ["Revenue", "Expenses", "Profit", "Income", "Assets", "Liabilities", "Equity", "Cash_Flow", "Other"]:
                        if category in organized_concepts:
                            # Add category header
                            if len(organized_concepts[category]) > 1:
                                category_row = f"<{category}>"
                                for _ in range(len(top_contexts) + 1):  # +1 for Key column
                                    category_row += " | "
                                table_rows.append(category_row)

                            # Sort items to ensure parent items come before children
                            sorted_concepts = sorted(
                                organized_concepts[category],
                                key=lambda x: (concept_hierarchies[x[0]]["level"], 0 if concept_hierarchies[x[0]]["is_total"] else 1)
                            )

                            # Add items in this category
                            for concept_tuple in sorted_concepts:
                                concept_name = concept_tuple[0]
                                facts = concept_tuple[1]
                                hierarchy_level = concept_tuple[2] if len(concept_tuple) > 2 else 0

                                # Create a mapping from context to fact for this concept
                                context_to_fact = {fact.get("context_ref", ""): fact for fact in facts}

                                # Only add rows that have data in at least one of our top contexts
                                if any(context_ref in context_to_fact for context_ref, _ in top_contexts):
                                    # Get hierarchy info for this concept
                                    concept_info = concept_hierarchies.get(concept_name, {})
                                    is_total = concept_info.get("is_total", False)
                                    parent = concept_info.get("parent", None)

                                    # Apply formatting based on hierarchy
                                    if is_total:
                                        # Format total items (make them stand out)
                                        if hierarchy_level == 1:
                                            # Level 1 totals (main line totals)
                                            row = f"@TOTAL: {concept_name}"
                                        else:
                                            # Lower level subtotals
                                            row = f"@SUBTOTAL: {concept_name}"
                                    elif parent:
                                        # Child items with indentation based on hierarchy level
                                        indentation = "  " * hierarchy_level
                                        row = f"{indentation}↳ {concept_name} @CHILD_OF: {parent}"
                                    else:
                                        # Regular line items with basic indentation
                                        if len(organized_concepts[category]) > 1:
                                            row = f"  {concept_name}"
                                        else:
                                            row = concept_name

                                    # Add enhanced context key reference with semantic codes
                                    context_keys = []
                                    semantic_keys = []
                                    for context_ref, _ in top_contexts:
                                        if context_ref in context_to_fact:
                                            if context_ref in context_code_map:
                                                context_keys.append(context_code_map[context_ref])

                                            # Add semantic code if available
                                            code = context_code_map.get(context_ref, "")
                                            if code:
                                                for label, info in context_reference_guide.items():
                                                    if info.get("code") == code and "semantic_code" in info:
                                                        semantic_keys.append(info["semantic_code"])
                                                        break

                                    # First show the short code (c-1, c-2) for backward compatibility
                                    if context_keys:
                                        row += f" | {','.join(context_keys)}"
                                    else:
                                        row += " | -"

                                    # Then add a comment with the semantic codes that are more descriptive
                                    if semantic_keys:
                                        row += f" # {','.join(semantic_keys)}"

                                    # Add values for each context with consistent units and scaling
                                    for context_ref, _ in top_contexts:
                                        if context_ref in context_to_fact:
                                            fact = context_to_fact[context_ref]
                                            value = fact.get("value", "")
                                            unit_ref = fact.get("unit_ref", "")
                                            decimals = fact.get("decimals", "")

                                            # Format value with appropriate scale indicator
                                            try:
                                                # Add currency symbol if available
                                                if unit_ref and unit_ref.lower() == "usd":
                                                    # Only add $ if it's not already there
                                                    if not value.startswith("$"):
                                                        formatted_value = f"${value}"
                                                    else:
                                                        formatted_value = value

                                                    # Add scale indicator if specified
                                                    if decimals and safe_parse_decimals(decimals) == -6:
                                                        formatted_value += " [M]"  # Millions
                                                    elif decimals and safe_parse_decimals(decimals) == -3:
                                                        formatted_value += " [K]"  # Thousands
                                                    elif decimals and safe_parse_decimals(decimals) == -9:
                                                        formatted_value += " [B]"  # Billions
                                                elif unit_ref and unit_ref.lower() == "shares":
                                                    formatted_value = f"{value} shares"
                                                else:
                                                    formatted_value = value

                                                row += f" | {formatted_value}"
                                            except:
                                                # Fallback if formatting fails
                                                # Add scale indicators to values in fallback context-based tables
                                                # Format value with appropriate scaling
                                                unit_ref = fact.get("unit_ref", "")
                                                decimals = fact.get("decimals", "")

                                                if unit_ref and unit_ref.lower() == "usd":
                                                    # Add currency symbol if needed
                                                    if not value.startswith("$"):
                                                        formatted_value = f"${value}"
                                                    else:
                                                        formatted_value = value

                                                    # Add scale indicator
                                                    if decimals and safe_parse_decimals(decimals) == -6:
                                                        formatted_value += " [M]"  # Millions
                                                    elif decimals and safe_parse_decimals(decimals) == -3:
                                                        formatted_value += " [K]"  # Thousands
                                                    elif decimals and safe_parse_decimals(decimals) == -9:
                                                        formatted_value += " [B]"  # Billions
                                                elif unit_ref and unit_ref.lower() == "shares":
                                                    formatted_value = f"{value} shares"
                                                else:
                                                    formatted_value = value

                                                row += f" | {formatted_value}"
                                        else:
                                            row += " | -"
                                    table_rows.append(row)

                            # Close category with proper XML syntax if we added a category header
                            if len(organized_concepts[category]) > 1:
                                end_category = f"</{category}>"
                                for _ in range(len(top_contexts) + 1):  # +1 for Key column
                                    end_category += " | "
                                table_rows.append(end_category)

                    # Only add the table if it actually has data rows
                    if len(table_rows) > 2:  # Header + separator + at least one data row
                        output.append("\n".join(table_rows))
                        output.append("")

                        # Update data integrity metrics
                        self.data_integrity["xbrl_tables_created"] += 1
                        self.data_integrity["tables_detected"] += 1
                        self.data_integrity["tables_included"] += 1
                        self.data_integrity["total_table_rows"] += len(table_rows)

                # Keep the original context-based approach as a fallback
                if context_map and not self.data_integrity["xbrl_tables_created"]:
                    # Group related facts into tables by context
                    related_contexts = {}
                    context_period_mapping = {}

                    # Process context map for year and period extraction
                    for context_ref, context_label in context_map.items():
                        # Extract year from context label (e.g. "FY2023" -> "2023")
                        year_match = re.search(r'(\d{4})', context_label)
                        if year_match:
                            year = year_match.group(1)

                            # Try to determine period type
                            period_type = "Unknown"
                            if "FY" in context_label or "Annual" in context_label:
                                period_type = "Annual"
                            elif "Q1" in context_label or "Q2" in context_label or "Q3" in context_label or "Q4" in context_label:
                                period_type = "Quarter"
                                quarter_match = re.search(r'Q([1-4])', context_label)
                                quarter = quarter_match.group(1) if quarter_match else ""
                                # Enhance the context label to be more descriptive
                                if quarter:
                                    context_map[context_ref] = f"Q{quarter} {year}"
                            elif "As of" in context_label:
                                period_type = "Instant"

                            # Store in our mapping
                            context_period_mapping[context_ref] = {
                                "year": year,
                                "label": context_map[context_ref],
                                "type": period_type,
                                "sort_key": f"{year}{period_type}{context_ref}"  # For sorting
                            }

                            # Group by year for tables
                            key = f"{section_name}_{year}"
                            if key not in related_contexts:
                                related_contexts[key] = []
                            if context_ref not in related_contexts[key]:
                                related_contexts[key].append(context_ref)

                    # Generate tables for each group of related contexts (typically by year)
                    for key, contexts in related_contexts.items():
                        # For each table, collect relevant facts
                        table_facts = []
                        for context_ref in contexts:
                            for fact in section_facts:
                                if fact.get("context_ref") == context_ref:
                                    table_facts.append(fact)

                        # Check if we have enough facts to create a meaningful table
                        if len(table_facts) >= 3:
                            # Extract year from key for better header
                            year_match = re.search(r'_(\d{4})$', key)
                            year = year_match.group(1) if year_match else ""

                            # Create descriptive table header
                            if year:
                                header = f"{section_name.replace('_', ' ')} TABLE ({year})"
                            else:
                                header = f"{section_name.replace('_', ' ')} TABLE - {key}"

                            output.append(f"@TABLE_CONTENT: {header}")

                            # Add period reference for this table if we have good info
                            period_types_in_table = set()
                            for context_ref in contexts:
                                if context_ref in context_period_mapping:
                                    period_types_in_table.add(context_period_mapping[context_ref]["type"])

                            if period_types_in_table:
                                period_guide = []
                                for period_type in ["Annual", "Quarter", "Instant"]:
                                    if period_type in period_types_in_table:
                                        periods = [ctx for ctx_ref, ctx in context_period_mapping.items()
                                                if ctx_ref in contexts and ctx["type"] == period_type]
                                        if periods:
                                            periods_str = ", ".join([ctx["label"] for ctx in periods])
                                            period_guide.append(f"{period_type} periods: {periods_str}")

                                if period_guide:
                                    output.append("Period reference: " + "; ".join(period_guide))

                            # Extract relevant periods/contexts for columns and sort chronologically
                            column_contexts = []
                            for context_ref in contexts:
                                if context_ref in context_map:
                                    sort_key = context_period_mapping.get(context_ref, {}).get("sort_key", context_ref)
                                    column_contexts.append((context_ref, context_map[context_ref], sort_key))

                            # Sort column_contexts chronologically
                            column_contexts.sort(key=lambda x: x[2])

                            # Convert to the format expected by the rest of the code
                            column_contexts = [(ctx[0], ctx[1]) for ctx in column_contexts]

                            # Group facts by concept for rows
                            facts_by_concept = {}
                            for fact in table_facts:
                                concept = fact.get("concept", "")
                                if ":" in concept:
                                    # Get readable concept name
                                    concept_name = concept.split(":")[-1]
                                    # Make it more readable
                                    readable_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', concept_name).title()
                                else:
                                    readable_name = concept

                                if readable_name not in facts_by_concept:
                                    facts_by_concept[readable_name] = {}

                                context_ref = fact.get("context_ref", "")
                                facts_by_concept[readable_name][context_ref] = fact

                            # Build table
                            table_rows = []

                            # Add header row with column contexts
                            header_row = "Concept"
                            for _, context_label in column_contexts:
                                header_row += f" | {context_label}"
                            table_rows.append(header_row)

                            # Add separator row
                            separator = "-" * len("Concept")
                            for context_label in [label for _, label in column_contexts]:
                                separator += f" | {'-' * len(context_label)}"
                            table_rows.append(separator)

                            # Add data rows
                            for concept_name, context_facts in facts_by_concept.items():
                                row = concept_name
                                for context_ref, _ in column_contexts:
                                    if context_ref in context_facts:
                                        value = context_facts[context_ref].get("value", "")
                                        # Add currency symbol if available
                                        unit_ref = context_facts[context_ref].get("unit_ref", "")
                                        if unit_ref and unit_ref.lower() == "usd":
                                            # Only add $ if it's not already there
                                            if not value.startswith("$"):
                                                value = f"${value}"
                                        row += f" | {value}"
                                    else:
                                        row += " | -"
                                table_rows.append(row)

                            # Add the table
                            output.append("\n".join(table_rows))
                            output.append("")

                            # Update data integrity metrics
                            self.data_integrity["xbrl_tables_created"] += 1
                            self.data_integrity["tables_detected"] += 1
                            self.data_integrity["tables_included"] += 1
                            self.data_integrity["total_table_rows"] += len(table_rows)

                # Sort facts within section
                sorted_section_facts = sorted(section_facts, key=lambda x: x.get("concept", ""))

                # Add facts with improved readability
                for fact in sorted_section_facts:
                    concept = fact.get("concept", "")
                    output.append(f"@CONCEPT: {concept}")

                    # Add readable name for custom elements
                    if ":" in concept and not concept.startswith("us-gaap:"):
                        # This is likely a custom taxonomy element
                        concept_name = concept.split(":")[-1]
                        # Make it more readable by converting camelCase to words
                        readable_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', concept_name).title()
                        output.append(f"@COMMON_NAME: {readable_name}")

                    output.append(f"@VALUE: {fact.get('value', '')}")

                    # Include units and decimals
                    if fact.get("unit_ref"):
                        output.append(f"@UNIT_REF: {fact.get('unit_ref', '')}")
                    if fact.get("decimals"):
                        output.append(f"@DECIMALS: {fact.get('decimals', '')}")

                    # Use human-readable context labels
                    # Note: context references can be either "context_ref" or "contextRef" (camelCase)
                    context_ref = fact.get("context_ref", fact.get("contextRef", ""))
                    if context_ref in context_map:
                        output.append(f"@CONTEXT_REF: {context_ref} | @CONTEXT: {context_map[context_ref]}")
                    else:
                        output.append(f"@CONTEXT_REF: {context_ref}")

                    try:
                        # First priority: Check context data from parsed XBRL
                        context_data = parsed_xbrl.get("contexts", {}).get(context_ref, {})
                        period_info = context_data.get("period", {})

                        if "startDate" in period_info and "endDate" in period_info:
                            # This is a duration context from parsed data
                            output.append(f"@DATE_TYPE: Duration")
                            output.append(f"@START_DATE: {period_info['startDate']}")
                            output.append(f"@END_DATE: {period_info['endDate']}")
                        elif "instant" in period_info:
                            # This is an instant context from parsed data
                            output.append(f"@DATE_TYPE: Instant")
                            output.append(f"@DATE: {period_info['instant']}")
                        # Check for simple context IDs (c-1, c-2, etc.) used by NVDA 2025+
                        elif re.match(r'^c-\d+$', context_ref):
                            # Check if we have HTML-extracted context information
                            if context_ref in self.context_mapping_from_html:
                                html_period_info = self.context_mapping_from_html[context_ref]
                                logging.info(f"Found HTML context info for {context_ref}: {html_period_info}")

                                if "startDate" in html_period_info and "endDate" in html_period_info:
                                    # Duration context from HTML
                                    output.append(f"@DATE_TYPE: Duration")
                                    output.append(f"@START_DATE: {html_period_info['startDate']}")
                                    output.append(f"@END_DATE: {html_period_info['endDate']}")
                                elif "instant" in html_period_info:
                                    # Instant context from HTML
                                    output.append(f"@DATE_TYPE: Instant")
                                    output.append(f"@DATE: {html_period_info['instant']}")
                                else:
                                    logging.warning(f"Context {context_ref} has HTML mapping but no dates: {html_period_info}")
                            else:
                                # Try to find the context in parsed_xbrl.contexts
                                if context_ref in parsed_xbrl.get("contexts", {}):
                                    context_data = parsed_xbrl["contexts"][context_ref]
                                    period_info = context_data.get("period", {})

                                    if "startDate" in period_info and "endDate" in period_info:
                                        # Duration context from parsed_xbrl
                                        output.append(f"@DATE_TYPE: Duration")
                                        output.append(f"@START_DATE: {period_info['startDate']}")
                                        output.append(f"@END_DATE: {period_info['endDate']}")
                                        logging.info(f"Used period info from parsed_xbrl for {context_ref}")
                                    elif "instant" in period_info:
                                        # Instant context from parsed_xbrl
                                        output.append(f"@DATE_TYPE: Instant")
                                        output.append(f"@DATE: {period_info['instant']}")
                                        logging.info(f"Used period info from parsed_xbrl for {context_ref}")
                                    else:
                                        logging.warning(f"Context {context_ref} found in parsed_xbrl but has no dates: {period_info}")
                                else:
                                    logging.warning(f"Context {context_ref} not found in HTML context mapping or parsed_xbrl. Available contexts in HTML mapping: {len(self.context_mapping_from_html)}")
                        else:
                            # Second priority: Check for embedded dates in context_ref ID

                            # Use the context format handler to extract period info from the context ID
                            period_info = extract_period_info(context_ref)

                            if period_info:
                                if "startDate" in period_info and "endDate" in period_info:
                                    # Duration context
                                    output.append(f"@DATE_TYPE: Duration")
                                    output.append(f"@START_DATE: {period_info['startDate']}")
                                    output.append(f"@END_DATE: {period_info['endDate']}")
                                elif "instant" in period_info:
                                    # Instant context
                                    output.append(f"@DATE_TYPE: Instant")
                                    output.append(f"@DATE: {period_info['instant']}")
                    except Exception as e:
                        # Catch any errors to prevent breaking the existing pipeline
                        logging.warning(f"Error extracting date information from context: {context_ref}. Error: {str(e)}")
                        # Continue processing without date information

                    output.append("")

        # Add the @CONTEXT_REFERENCE_GUIDE section
        output.append("@CONTEXT_REFERENCE_GUIDE")
        output.append("This section provides a consolidated reference for all time periods used in this document.")
        output.append("")

        # Add Data Dictionary Header
        output.append("@DATA_DICTIONARY:")
        output.append("Context_Code | Semantic_Code | Category | Type | Segment | Period | Description")
        output.append("------------|---------------|----------|------|---------|--------|------------")

        # Sort period contexts by date for better readability
        period_contexts.sort(key=lambda x: x["start_date"])
        instant_contexts.sort(key=lambda x: x["date"])

        # Add all contexts to the guide

        # Extract context information directly from context IDs if we have facts but no contexts
        if not period_contexts and not instant_contexts and parsed_xbrl.get("facts"):
            # Extract unique context IDs from facts
            context_refs = set()
            for fact in parsed_xbrl.get("facts", []):
                context_ref = fact.get("context_ref", fact.get("contextRef", ""))
                if context_ref:
                    context_refs.add(context_ref)

            # Try to extract dates from context IDs
            for i, context_ref in enumerate(sorted(context_refs)):
                # Use the context format handler to extract period info
                period_info = extract_period_info(context_ref)

                if period_info:
                    if "startDate" in period_info and "endDate" in period_info:
                        # Duration context
                        code = f"P{i+1}"
                        context_code_map[context_ref] = code

                        formatted_start = period_info["startDate"]
                        formatted_end = period_info["endDate"]

                        semantic_code = f"Duration_{formatted_start}_{formatted_end}"
                        category = self._get_fiscal_category(formatted_start, formatted_end, filing_type)

                        # Add to output
                        output.append(f"{code} | {semantic_code} | {category} | Duration | Consolidated | {formatted_start} to {formatted_end} | Period from {formatted_start} to {formatted_end}")

                    elif "instant" in period_info:
                        # Instant context
                        code = f"I{i+1}"
                        context_code_map[context_ref] = code

                        formatted_date = period_info["instant"]

                        semantic_code = f"Instant_{formatted_date}"
                        category = self._get_fiscal_category(formatted_date, formatted_date, filing_type)

                        # Add to output
                        output.append(f"{code} | {semantic_code} | {category} | Instant | Consolidated | {formatted_date} | As of {formatted_date}")

        elif period_contexts or instant_contexts:
            for i, context in enumerate(period_contexts):
                code = f"P{i+1}"
                context_code_map[context["context_id"]] = code
                semantic_code = f"Duration_{context['start_date']}_{context['end_date']}"
                # Get fiscal year/quarter info if available
                category = self._get_fiscal_category(context["start_date"], context["end_date"], filing_type)
                output.append(f"{code} | {semantic_code} | {category} | Duration | {context['segment']} | {context['start_date']} to {context['end_date']} | {context['description']}")

            for i, context in enumerate(instant_contexts):
                code = f"I{i+1}"
                context_code_map[context["context_id"]] = code
                semantic_code = f"Instant_{context['date']}"
                # Get fiscal period info if available
                category = self._get_fiscal_category(context["date"], context["date"], filing_type)
                output.append(f"{code} | {semantic_code} | {category} | Instant | {context['segment']} | {context['date']} | {context['description']}")
        else:
            output.append("Note: No detailed context information available for this filing")

        output.append("")

        # Add Verification Keys section
        output.append("@VERIFICATION_KEYS:")
        output.append("These formulas can be used to verify data consistency:")
        if period_contexts or instant_contexts:
            # This is where you could add formulas that help verify data consistency
            output.append("- Current Assets + Non-Current Assets = Total Assets")
            output.append("- Current Liabilities + Non-Current Liabilities + Equity = Total Liabilities and Equity")
            output.append("- Gross Profit = Revenue - Cost of Revenue")
            output.append("- Net Income = Revenue - Expenses + Other Income/Expenses - Taxes")
        else:
            output.append("- No context information available for formula verification")

        output.append("")

        # Add Period Contexts section
        output.append("@PERIOD_CONTEXTS")
        if period_contexts:
            for context in period_contexts:
                output.append(f"{context_code_map[context['context_id']]}: {context['description']}")
                if context['segment']:
                    output.append(f"  Segment: {context['segment']}")
        else:
            output.append("No detailed period context information available")

        output.append("")

        # Add Instant Contexts section
        output.append("@INSTANT_CONTEXTS")
        if instant_contexts:
            for context in instant_contexts:
                output.append(f"{context_code_map[context['context_id']]}: {context['description']}")
                if context['segment']:
                    output.append(f"  Segment: {context['segment']}")
        else:
            output.append("No detailed instant context information available")

        output.append("")

        # Add Example Extraction section
        output.append("@EXAMPLE_EXTRACTION")
        output.append("Example of how to extract and format data from this document:")
        if period_contexts or instant_contexts:
            output.append("```")
            output.append("// Example of extracting Revenue for the most recent period")
            output.append("const revenue = extractData('Revenue', 'FY2023');")
            output.append("```")
        else:
            output.append("No context examples available")

        output.append("")

        # Add narrative sections
        if extracted_sections:
            # Priority sections are now defined earlier for coverage calculation
            # Use the same priority order for section processing

            # Process sections in priority order
            for section_id in priority_sections:
                if section_id in extracted_sections:
                    section_data = extracted_sections[section_id]
                    output.append(f"@SECTION: {section_id}")
                    output.append(f"@SECTION_TITLE: {section_data['name']}")
                    output.append("")

                    # Process text into manageable chunks
                    text = section_data['text']

                    # Split by paragraphs
                    paragraphs = re.split(r'\n\s*\n', text)

                    # Separate tables and narrative text
                    tables = []
                    narrative_paragraphs = []

                    # For data integrity tracking
                    section_id_safe = section_id.replace(" ", "_")
                    if section_id_safe not in self.data_integrity["section_tables"]:
                        self.data_integrity["section_tables"][section_id_safe] = {
                            "detected": 0,
                            "included": 0,
                            "rows": 0
                        }

                    for paragraph in paragraphs:
                        paragraph = paragraph.strip()
                        if not paragraph:
                            continue

                        # Comprehensive table detection
                        is_table = False
                        detection_method = "none"

                        # Method 1: Check for explicit table markers (pipe/tab)
                        if '|' in paragraph or '\t' in paragraph:
                            is_table = True
                            detection_method = "explicit_markers"

                        # Method 2: Check for aligned columns with financial indicators
                        elif any(financial_marker in paragraph for financial_marker in ['$', '%', '(Dollars', '(in millions', 'Three Months Ended']):
                            # Check for aligned numeric data - common in financial tables
                            lines = paragraph.split('\n')
                            if len(lines) >= 2:  # Need at least 2 rows to be a table
                                # Look for aligned numbers or currency symbols
                                numeric_pattern = r'[\s\d\$\(\)\.,%-]+'
                                aligned_positions = []

                                # Find positions of numbers in first line to check alignment
                                for match in re.finditer(r'\$\d+|\d+\.\d+|\(\d+\)|\d+%', lines[0]):
                                    aligned_positions.append((match.start(), match.end()))

                                # Check for numbers or currency symbols at similar positions in other lines
                                if aligned_positions:
                                    alignment_count = 0
                                    for line in lines[1:]:
                                        for start, end in aligned_positions:
                                            # Allow for some flexibility in position (±5 chars)
                                            if start >= 5 and end <= len(line) + 5:
                                                nearby_text = line[max(0, start-5):min(len(line), end+5)]
                                                if re.search(r'\$\d+|\d+\.\d+|\(\d+\)|\d+%', nearby_text):
                                                    alignment_count += 1

                                    # If we have good alignment, it's probably a table
                                    if alignment_count >= len(lines) - 1:
                                        is_table = True
                                        detection_method = "financial_indicators"

                        # Method 3: Detect space-delimited tables with column headers and consistent structure
                        elif len(paragraph.split('\n')) >= 3:  # Need header + at least 2 data rows
                            lines = paragraph.split('\n')

                            # Count spaces to detect column boundaries in first 2 lines
                            space_positions_1 = [i for i, char in enumerate(lines[0]) if char == ' ' and i > 0 and lines[0][i-1] != ' ']
                            if len(lines) > 1:
                                space_positions_2 = [i for i, char in enumerate(lines[1]) if char == ' ' and i > 0 and lines[1][i-1] != ' ']

                                # If space positions are similar in multiple lines, likely a table with aligned columns
                                matching_positions = 0
                                for pos1 in space_positions_1:
                                    for pos2 in space_positions_2:
                                        if abs(pos1 - pos2) <= 3:  # Allow slight misalignment
                                            matching_positions += 1

                                if matching_positions >= 2:  # At least 2 columns align
                                    is_table = True
                                    detection_method = "space_alignment"

                        # Add to appropriate category and update data integrity metrics
                        if is_table:
                            tables.append(paragraph)
                            line_count = len(paragraph.split('\n'))

                            # Update data integrity metrics
                            self.data_integrity["tables_detected"] += 1
                            self.data_integrity["total_table_rows"] += line_count
                            self.data_integrity["section_tables"][section_id_safe]["detected"] += 1
                            self.data_integrity["section_tables"][section_id_safe]["rows"] += line_count

                            # Log detection of important tables for verification
                            if any(financial_term in paragraph.lower() for financial_term in
                                  ["balance sheet", "income statement", "cash flow", "statement of operations"]):
                                logging.info(f"Detected important financial table in {section_id} using {detection_method}")
                        elif len(paragraph) >= 100:  # Only collect substantive paragraphs
                            narrative_paragraphs.append(paragraph)
                            self.data_integrity["narrative_paragraphs"] += 1

                    # For narrative text, select important paragraphs (limit quantity to keep file size reasonable)
                    # Focus on first few paragraphs which often contain key information
                    important_paragraphs = []
                    paragraph_count = 0

                    for paragraph in narrative_paragraphs:
                        important_paragraphs.append(paragraph)
                        paragraph_count += 1

                        # Update integrity metric
                        self.data_integrity["included_paragraphs"] += 1

                        # Limit paragraphs per section for narrative text only
                        if paragraph_count >= 5:
                            break

                    # Add selected narrative paragraphs
                    for paragraph in important_paragraphs:
                        output.append(f"@NARRATIVE_TEXT: {paragraph}")
                        output.append("")

                    # Add ALL tables with 100% fidelity - no filtering or summarization
                    for table in tables:
                        output.append(f"@TABLE_CONTENT: {table}")
                        output.append("")

                        # Update integrity metrics for included tables
                        self.data_integrity["tables_included"] += 1
                        self.data_integrity["section_tables"][section_id_safe]["included"] += 1

        # Add Data Integrity Report
        output.append("@DATA_INTEGRITY_REPORT")
        output.append("Table preservation metrics:")
        output.append(f"- XBRL facts: {self.data_integrity.get('xbrl_facts', 0)}")
        output.append(f"- XBRL tables created: {self.data_integrity.get('xbrl_tables_created', 0)}")
        output.append(f"- Text tables detected: {self.data_integrity['tables_detected'] - self.data_integrity.get('xbrl_tables_created', 0)}")
        output.append(f"- Total tables included: {self.data_integrity['tables_included']}")
        if self.data_integrity['tables_detected'] > 0:
            preservation_rate = 100 * self.data_integrity['tables_included'] / self.data_integrity['tables_detected']
            output.append(f"- Table preservation rate: {preservation_rate:.1f}%")
            # Warning if not all tables are preserved
            if preservation_rate < 100:
                output.append("WARNING: Not all tables were preserved!")
        output.append(f"- Total table rows: {self.data_integrity['total_table_rows']}")
        output.append(f"- Narrative paragraphs: {self.data_integrity['narrative_paragraphs']}")
        output.append(f"- Narrative paragraphs included: {self.data_integrity['included_paragraphs']}")

        # Document section coverage report
        output.append("")
        output.append("@DOCUMENT_COVERAGE")

        # Use priority_sections defined at the beginning of the method

        # Check for new format with document_sections in filing_metadata
        document_sections = {}
        if "html_content" in filing_metadata and isinstance(filing_metadata["html_content"], dict) and "document_sections" in filing_metadata["html_content"]:
            document_sections = filing_metadata["html_content"]["document_sections"]

        # Create lists of covered and missing sections, considering both extracted_sections and document_sections
        covered_sections = []
        # Check for direct exhibits flag from HTML processor
        has_exhibits_flag = False
        if "html_content" in filing_metadata and isinstance(filing_metadata["html_content"], dict):
            has_exhibits_flag = filing_metadata["html_content"].get("has_exhibits_section", False)
            if has_exhibits_flag:
                logging.info("Found direct exhibits flag from HTML processor")

        for section_id in priority_sections:
            # Check standard section detection
            if section_id in extracted_sections or section_id in document_sections:
                covered_sections.append(section_id)
            # Special handling for EXHIBITS section with alternative detection
            elif section_id == "ITEM_15_EXHIBITS":
                # Check for direct flag first
                if has_exhibits_flag:
                    covered_sections.append(section_id)
                    logging.info("EXHIBITS section included through direct flag")
                # Then check for alternative detection
                elif document_sections:
                    for section_key, section_info in document_sections.items():
                        if section_key == "ITEM_15_EXHIBITS" and section_info.get("detected_as") == "exhibits_alternative":
                            covered_sections.append(section_id)
                            logging.info(f"EXHIBITS section included through alternative detection: {section_info.get('heading', '')}")
                            break
            # Special handling for 1B and 1C which often have minimal content
            elif section_id in ["ITEM_1B_UNRESOLVED_STAFF_COMMENTS", "ITEM_1C_CYBERSECURITY"] and document_sections:
                # Check for alternative detection with content pattern match
                section_found = False
                for section_key, section_info in document_sections.items():
                    if section_key == section_id and section_info.get("detected_as") == "content_pattern_match":
                        covered_sections.append(section_id)
                        logging.info(f"{section_id} included through content pattern match: {section_info.get('heading', '')}")
                        section_found = True
                        break

                # Last resort - check raw text for 1B/1C keywords if section not found above
                if not section_found and "raw_html_text" in filing_metadata and filing_type == "10-K":
                    raw_text = filing_metadata.get("raw_html_text", "")
                    if section_id == "ITEM_1B_UNRESOLVED_STAFF_COMMENTS" and re.search(r'unresolved\s+staff\s+comments', raw_text, re.IGNORECASE):
                        covered_sections.append(section_id)
                        logging.info("Item 1B detected through raw text search")
                    elif section_id == "ITEM_1C_CYBERSECURITY" and re.search(r'cybersecurity', raw_text, re.IGNORECASE):
                        covered_sections.append(section_id)
                        logging.info("Item 1C detected through raw text search")

        missing_sections = [section_id for section_id in priority_sections if section_id not in covered_sections]

        if filing_type == "10-K":
            # Define required sections for 10-K filings
            required_10k_sections = [
                "ITEM_1_BUSINESS",
                "ITEM_1A_RISK_FACTORS",
                "ITEM_2_PROPERTIES",
                "ITEM_3_LEGAL_PROCEEDINGS",
                "ITEM_7_MD_AND_A",
                "ITEM_7A_MARKET_RISK",
                "ITEM_8_FINANCIAL_STATEMENTS",
                "ITEM_9A_CONTROLS",
                "ITEM_10_DIRECTORS",
                "ITEM_11_EXECUTIVE_COMPENSATION"
                # "ITEM_15_EXHIBITS" - Removed from required as often non-standard in company filings
            ]

            # Optional sections (may not be present depending on company)
            optional_10k_sections = [
                "ITEM_1B_UNRESOLVED_STAFF_COMMENTS",  # Only required for accelerated filers
                "ITEM_1C_CYBERSECURITY",              # Added in 2023, not required before
                "ITEM_4_MINE_SAFETY_DISCLOSURES",     # Only for mining companies
                "ITEM_5_MARKET",                      # Sometimes combined with Item 6
                "ITEM_6_SELECTED_FINANCIAL_DATA",     # No longer required since 2021
                "ITEM_9_DISAGREEMENTS",               # Only if there were disagreements
                "ITEM_9B_OTHER_INFORMATION",          # Only if there is other information
                "ITEM_9C_FOREIGN_JURISDICTIONS",      # Only for companies with foreign operations
                "ITEM_12_SECURITY_OWNERSHIP",         # Sometimes combined with other sections
                "ITEM_13_RELATIONSHIPS",              # Sometimes limited or combined
                "ITEM_14_ACCOUNTANT_FEES",            # Sometimes combined with Item 9
                "ITEM_15_EXHIBITS",                   # Made optional due to variation in reporting format
                "ITEM_16_SUMMARY"                     # Optional summary
            ]

            # Calculate coverage percentage based on 10-K sections
            # Only consider the main items for 10-K (skipping subsections)
            main_10k_sections = [s for s in priority_sections if s.startswith("ITEM_") and not s.endswith("_Q")]
            found_10k_sections = [s for s in covered_sections if s.startswith("ITEM_") and not s.endswith("_Q")]

            # Calculate required vs optional coverage
            covered_required = [s for s in covered_sections if s in required_10k_sections]
            covered_optional = [s for s in covered_sections if s in optional_10k_sections]

            if main_10k_sections:
                # Calculate different coverage metrics
                standard_coverage = (len(found_10k_sections) / len(main_10k_sections)) * 100
                required_coverage = (len(covered_required) / len(required_10k_sections)) * 100

                # Content-centric approach - focus on data completeness
                # First, record the proportion of document content captured
                raw_content = ""
                if "raw_html_text_length" in filing_metadata:
                    raw_content_length = filing_metadata["raw_html_text_length"]
                    processed_content_length = len("\n".join(output))

                    # Calculate text content coverage (more meaningful than section counting)
                    # This accounts for whitespace differences and formatting
                    text_coverage = min(100.0, (processed_content_length / max(1, raw_content_length)) * 100)
                    # Store in data integrity metrics
                    self.data_integrity["content_coverage"] = text_coverage
                    output.append(f"Content Completeness: {text_coverage:.1f}% (by text volume)")

                    # If content coverage is very high but section coverage is lower, suggest potential causes
                    if text_coverage > 95.0 and required_coverage < 95.0:
                        output.append("Note: High content coverage with lower section coverage may indicate non-standard section formatting")

                # Check for TOC-based actual sections
                if "html_content" in filing_metadata and isinstance(filing_metadata["html_content"], dict) and "actual_sections" in filing_metadata["html_content"]:
                    actual_sections = filing_metadata["html_content"]["actual_sections"]

                    # Ensure actual_sections is treated as a list consistently
                    actual_sections_list = actual_sections
                    if isinstance(actual_sections, dict):
                        # If it's a dictionary, use the keys as the section list
                        actual_sections_list = list(actual_sections.keys())

                    # Find the overlap between covered sections and actual sections
                    actually_covered = [s for s in covered_sections if s in actual_sections_list]
                    actually_missed = [s for s in actual_sections_list if s not in covered_sections]
                    actual_coverage = (len(actually_covered) / len(actual_sections_list)) * 100 if actual_sections_list else 0

                    # Report section coverage (secondary metric)
                    output.append(f"Document Structure Coverage: {actual_coverage:.1f}% ({len(actually_covered)}/{len(actual_sections_list)} TOC sections)")

                # Show standard metrics after the more informative ones
                output.append(f"10-K Required Coverage: {required_coverage:.1f}% ({len(covered_required)}/{len(required_10k_sections)} required sections)")
                output.append(f"10-K Standard Coverage: {standard_coverage:.1f}% ({len(found_10k_sections)}/{len(main_10k_sections)} standard sections)")

                # Show both metrics in a more readable way
                output.append(f"Found Required: {len(covered_required)} of {len(required_10k_sections)} required sections")
                if optional_10k_sections:
                    output.append(f"Found Optional: {len(covered_optional)} of {len(optional_10k_sections)} optional sections")

                # Show which sections were found (required first, then optional)
                output.append("Required sections found: " + ", ".join([s.replace("ITEM_", "Item ").replace("_", " ") for s in covered_required]))
                if covered_optional:
                    output.append("Optional sections found: " + ", ".join([s.replace("ITEM_", "Item ").replace("_", " ") for s in covered_optional]))

                # Show missing required sections (most important)
                missing_required = [s for s in required_10k_sections if s not in covered_sections]
                if missing_required:
                    output.append(f"Missing required section(s): " +
                                 ", ".join([s.replace("ITEM_", "Item ").replace("_", " ") for s in missing_required]))

                # Track alternative detection for improved metrics
                alternative_detected = []
                for section_id, section_info in document_sections.items():
                    if section_info.get("detected_as") == "exhibits_alternative":
                        alternative_detected.append(section_id)

                # Update data integrity metrics
                self.data_integrity["sections_detected"] = len(document_sections)
                self.data_integrity["sections_included"] = len(covered_sections)
                self.data_integrity["alternative_sections_detected"] = len(alternative_detected)

                # Add explanatory note about section detection
                output.append("Note: Companies may combine sections, use different formatting, or omit optional sections")

                # If alternative detection was used, add information about detection method
                if alternative_detected:
                    output.append(f"Note: {len(alternative_detected)} section(s) detected using alternative methods for non-standard formatting")

        elif filing_type == "10-Q":
            # Calculate coverage percentage based on 10-Q sections
            main_10q_sections = [s for s in priority_sections if s.startswith("ITEM_") and (s.endswith("_Q") or s in [
                "ITEM_1_FINANCIAL_STATEMENTS", "ITEM_2_MD_AND_A", "ITEM_3_MARKET_RISK", "ITEM_4_CONTROLS"
            ])]
            found_10q_sections = [s for s in covered_sections if s.startswith("ITEM_") and (s.endswith("_Q") or s in [
                "ITEM_1_FINANCIAL_STATEMENTS", "ITEM_2_MD_AND_A", "ITEM_3_MARKET_RISK", "ITEM_4_CONTROLS"
            ])]

            if main_10q_sections:
                coverage_pct = (len(found_10q_sections) / len(main_10q_sections)) * 100
                output.append(f"10-Q Coverage: {coverage_pct:.1f}% of standard sections found")

                # Items found
                output.append(f"Found {len(found_10q_sections)} of {len(main_10q_sections)} standard 10-Q sections:")
                output.append(", ".join([s.replace("ITEM_", "Item ").replace("_Q", "").replace("_", " ") for s in found_10q_sections]))

                # Items not found (might be absent in the original document)
                if missing_sections:
                    missing_10q = [s for s in missing_sections if s.startswith("ITEM_") and (s.endswith("_Q") or s in [
                        "ITEM_1_FINANCIAL_STATEMENTS", "ITEM_2_MD_AND_A", "ITEM_3_MARKET_RISK", "ITEM_4_CONTROLS"
                    ])]
                    if missing_10q:
                        output.append(f"Missing {len(missing_10q)} section(s):")
                        output.append(", ".join([s.replace("ITEM_", "Item ").replace("_Q", "").replace("_", " ") for s in missing_10q]))
                        output.append("Note: Missing sections may not exist in the original document")

        output.append("")

        # Enhanced Context Reference Guide and Data Dictionary
        # This entire section was moved to earlier in the file
        # to properly populate the context reference guide

        # The verification section is now added independently
        output.append("@VERIFICATION_KEYS:")
        output.append("These formulas can be used to verify data consistency:")

        # Only add verification formulas if we have context information
        if context_reference_guide:
            # Sort the context labels by year and period
            sorted_labels = sorted(context_reference_guide.keys())

            # Group contexts by year for formula creation
            years = {}
            for label in sorted_labels:
                info = context_reference_guide[label]
                if "category" in info:
                    category = info["category"]
                    if "_" in category:
                        year = category.split("_")[1]
                        if year not in years:
                            years[year] = []
                        years[year].append((info["code"], label))

            # Create verification formulas by year
            for year, contexts in years.items():
                if contexts:
                    codes = [code for code, _ in contexts]

                    # Get the semantic codes too for clarity
                    semantic_codes = []
                    for code, _ in contexts:
                        for _, info in context_reference_guide.items():
                            if info.get("code") == code and "semantic_code" in info:
                                semantic_codes.append(info["semantic_code"])
                                break

                    # Show formulas with both short codes and semantic codes
                    context_ref = f"contexts: {', '.join(codes)}"
                    if semantic_codes:
                        context_ref += f" ({', '.join(semantic_codes)})"

                    output.append(f"- Revenue_{year} = Product_Revenue_{year} + Service_Revenue_{year} ({context_ref})")
                    output.append(f"- Gross_Profit_{year} = Revenue_{year} - Cost_of_Revenue_{year} ({context_ref})")
                    output.append(f"- Operating_Income_{year} = Gross_Profit_{year} - Operating_Expenses_{year} ({context_ref})")
        else:
            output.append("- No context information available for formula verification")

        output.append("")

        # First list period contexts (fiscal years, quarters)
        output.append("@PERIOD_CONTEXTS")
        if context_reference_guide:
            sorted_labels = sorted(context_reference_guide.keys())
            period_labels = [label for label in sorted_labels if context_reference_guide[label]["type"] == "period"]
            if period_labels:
                for label in period_labels:
                    context_info = context_reference_guide[label]
                    code = context_info.get('code', "Unknown")
                    semantic_code = context_info.get('semantic_code', code)
                    segment = context_info.get('segment', 'Consolidated')

                    # Create a more descriptive entry with all available information
                    output.append(f"- {label}: {context_info['description']} (code: {code}, semantic: {semantic_code}, segment: {segment})")
            else:
                output.append("No period contexts found in this filing")
        else:
            output.append("No detailed period context information available")
        output.append("")

        # Then list instant contexts (balance sheet dates)
        output.append("@INSTANT_CONTEXTS")
        if context_reference_guide:
            sorted_labels = sorted(context_reference_guide.keys())
            instant_labels = [label for label in sorted_labels if context_reference_guide[label]["type"] == "instant"]
            if instant_labels:
                for label in instant_labels:
                    context_info = context_reference_guide[label]
                    code = context_info.get('code', "Unknown")
                    semantic_code = context_info.get('semantic_code', code)
                    segment = context_info.get('segment', 'Consolidated')
                    statement_type = context_info.get('statement_type', 'Unknown')

                    # Create a more descriptive entry with all available information
                    output.append(f"- {label}: {context_info['description']} (code: {code}, semantic: {semantic_code}, segment: {segment}, statement: {statement_type})")
            else:
                output.append("No instant contexts found in this filing")
        else:
            output.append("No detailed instant context information available")
        output.append("")

        # Add a mapping of fiscal periods to calendar periods if fiscal year info is available
        if fiscal_year and fiscal_period:
            output.append("@FISCAL_CALENDAR_MAPPING")
            output.append(f"- Fiscal Year {fiscal_year} corresponds to filing period ending {period_end}")
            if fiscal_period in ["Q1", "Q2", "Q3", "Q4"]:
                output.append(f"- {fiscal_period} is a quarterly period within Fiscal Year {fiscal_year}")
            elif fiscal_period == "annual":
                output.append(f"- This document represents the annual (10-K) filing for Fiscal Year {fiscal_year}")
            output.append("")

        # Add example extraction section
        output.append("@EXAMPLE_EXTRACTION")
        output.append("Example of how to extract and format data from this document:")
        output.append("")
        output.append("```python")
        output.append("# Extract revenue information for specific period with hierarchy awareness")
        output.append("def extract_revenue(document, context_code):")
        output.append("    # Find the Revenue section")
        output.append("    revenue_section = find_section(document, '<Revenue>')")
        output.append("    ")
        output.append("    # Extract all line items with their context codes")
        output.append("    revenue_data = {}")
        output.append("    totals = {}")
        output.append("    parent_child_map = {}")
        output.append("    ")
        output.append("    # First pass: collect all items and identify parent-child relationships")
        output.append("    for line in revenue_section:")
        output.append("        if context_code in line.split('|')[1].strip():")
        output.append("            columns = line.split('|')")
        output.append("            raw_item_name = columns[0].strip()")
        output.append("            ")
        output.append("            # Parse hierarchy information")
        output.append("            if raw_item_name.startswith('@TOTAL:'):")
        output.append("                # This is a total item")
        output.append("                item_name = raw_item_name.replace('@TOTAL:', '').strip()")
        output.append("                is_total = True")
        output.append("                level = 0  # Top level")
        output.append("            elif raw_item_name.startswith('@SUBTOTAL:'):")
        output.append("                # This is a subtotal item")
        output.append("                item_name = raw_item_name.replace('@SUBTOTAL:', '').strip()")
        output.append("                is_total = True")
        output.append("                level = 1  # Mid level")
        output.append("            elif '@CHILD_OF:' in raw_item_name:")
        output.append("                # This is a child item, extract its parent")
        output.append("                parts = raw_item_name.split('@CHILD_OF:')")
        output.append("                item_name = parts[0].replace('↳', '').strip()")
        output.append("                parent_name = parts[1].strip()")
        output.append("                parent_child_map[item_name] = parent_name")
        output.append("                is_total = False")
        output.append("                level = 2  # Child level")
        output.append("            else:")
        output.append("                # Regular item")
        output.append("                item_name = raw_item_name.strip()")
        output.append("                is_total = False")
        output.append("                level = 1  # Base level")
        output.append("            ")
        output.append("            # Find the value corresponding to the context code")
        output.append("            value = get_value_for_context(columns, context_code)")
        output.append("            ")
        output.append("            # Store with hierarchy information")
        output.append("            if is_total:")
        output.append("                totals[item_name] = {")
        output.append("                    'value': value,")
        output.append("                    'level': level,")
        output.append("                    'components': []  # Will be filled in second pass")
        output.append("                }")
        output.append("            else:")
        output.append("                revenue_data[item_name] = {")
        output.append("                    'value': value,")
        output.append("                    'level': level,")
        output.append("                    'parent': parent_child_map.get(item_name)")
        output.append("                }")
        output.append("    ")
        output.append("    # Second pass: associate children with their parent totals")
        output.append("    for item_name, item_data in revenue_data.items():")
        output.append("        parent = item_data.get('parent')")
        output.append("        if parent and parent in totals:")
        output.append("            totals[parent]['components'].append(item_name)")
        output.append("    ")
        output.append("    # Combine the data structures")
        output.append("    result = {")
        output.append("        'line_items': revenue_data,")
        output.append("        'totals': totals,")
        output.append("        # Include verification formulas")
        output.append("        'verify': {")
        output.append("            'sum_components': 'Sum of all components should equal the total',")
        output.append("            'formula': 'Total_Revenue = Product_Revenue + Service_Revenue + Other_Revenue'")
        output.append("        }")
        output.append("    }")
        output.append("    return result")
        output.append("```")
        output.append("")

        output.append("Example hierarchical data format for extraction:")
        output.append("")
        output.append("```json")
        output.append("{")
        output.append("  \"line_items\": {")
        output.append("    \"Product_Revenue\": {")
        output.append("      \"value\": 72480,")
        output.append("      \"level\": 1,")
        output.append("      \"parent\": \"Total_Revenue\"")
        output.append("    },")
        output.append("    \"Service_Revenue\": {")
        output.append("      \"value\": 16320,")
        output.append("      \"level\": 1,")
        output.append("      \"parent\": \"Total_Revenue\"")
        output.append("    },")
        output.append("    \"Automotive_Revenue\": {")
        output.append("      \"value\": 52300,")
        output.append("      \"level\": 2,")
        output.append("      \"parent\": \"Product_Revenue\"")
        output.append("    },")
        output.append("    \"Energy_Revenue\": {")
        output.append("      \"value\": 20180,")
        output.append("      \"level\": 2,")
        output.append("      \"parent\": \"Product_Revenue\"")
        output.append("    }")
        output.append("  },")
        output.append("  \"totals\": {")
        output.append("    \"Total_Revenue\": {")
        output.append("      \"value\": 88800,")
        output.append("      \"level\": 0,")
        output.append("      \"components\": [\"Product_Revenue\", \"Service_Revenue\"],")
        output.append("      \"contexts\": {")
        output.append("        \"c-1\": 88800,           // Using short code")
        output.append("        \"FY2023\": 88800,        // Using semantic code")
        output.append("        \"Consolidated_2023\": 88800  // Using segment information")
        output.append("      }")
        output.append("    },")
        output.append("    \"Product_Revenue\": {")
        output.append("      \"value\": 72480,")
        output.append("      \"level\": 1,")
        output.append("      \"components\": [\"Automotive_Revenue\", \"Energy_Revenue\"]")
        output.append("    }")
        output.append("  },")
        output.append("  \"verify\": {")
        output.append("    \"equations\": [")
        output.append("      \"Total_Revenue = Product_Revenue + Service_Revenue\",")
        output.append("      \"Product_Revenue = Automotive_Revenue + Energy_Revenue\"")
        output.append("    ],")
        output.append("    \"verified\": true")
        output.append("  },")
        output.append("  \"metadata\": {")
        output.append("    \"statement_type\": \"Income_Statement\",")
        output.append("    \"period\": \"FY2023\",")
        output.append("    \"hierarchical\": true")
        output.append("  }")
        output.append("}")
        output.append("```")
        output.append("")
        output.append("Note: This hierarchical structure makes parent-child relationships explicit, enabling accurate calculation and validation of totals. The hierarchy levels indicate:")
        output.append("- Level 0: Main totals (like Total Revenue)")
        output.append("- Level 1: Major categories (like Product Revenue)")
        output.append("- Level 2: Subcategories (like Automotive Revenue)")
        output.append("")
        output.append("You can use either the numeric short codes (c-1, c-2) or the semantic codes (FY2023, BS_2023_12_31) to access context values. The semantic codes are more descriptive and self-explanatory.")

        return "\n".join(output)

    def _get_segment_info(self, context):
        """Extract segment information from context in a readable format."""
        segment_text = "Consolidated"  # Default if no segment info

        entity_info = context.get("entity", {})
        segment_data = entity_info.get("segment", {})

        if segment_data:
            segment_items = []

            # Look for common segment dimensions like business unit or geography
            for dimension_key, dimension_value in segment_data.items():
                if isinstance(dimension_value, str):
                    # Clean up dimension names
                    dim_name = dimension_key.split(":")[-1] if ":" in dimension_key else dimension_key
                    dim_name = dim_name.replace("Segment", "").replace("Dimension", "")

                    # Clean up dimension values
                    dim_value = dimension_value.split(":")[-1] if ":" in dimension_value else dimension_value

                    segment_items.append(f"{dim_name}: {dim_value}")

            if segment_items:
                segment_text = ", ".join(segment_items)

        return segment_text

    def _get_fiscal_category(self, start_date, end_date, filing_type):
        """Determine fiscal category (Annual, Q1, Q2, etc.) from date range."""
        try:
            # For instant dates, use the same date for both start and end
            if start_date == end_date:
                # This is likely a balance sheet date
                date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                year = date_obj.year
                return f"FY{year}_End"

            # For duration contexts
            start_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            end_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")

            # Calculate duration in days
            duration_days = (end_obj - start_obj).days

            # Determine if it's annual or quarterly
            if 350 <= duration_days <= 380 or filing_type == "10-K":
                # Annual period
                return f"Annual_FY{end_obj.year}"
            elif 85 <= duration_days <= 95:
                # Quarterly period - determine which quarter
                end_month = end_obj.month
                if 1 <= end_month <= 3:
                    quarter = "Q1"
                elif 4 <= end_month <= 6:
                    quarter = "Q2"
                elif 7 <= end_month <= 9:
                    quarter = "Q3"
                else:
                    quarter = "Q4"
                return f"Quarterly_{end_obj.year}_{quarter}"
            elif 175 <= duration_days <= 190:
                # Six-month period
                return f"SixMonths_{end_obj.year}"
            elif 265 <= duration_days <= 280:
                # Nine-month period
                return f"NineMonths_{end_obj.year}"
            else:
                # Default category if we can't determine
                return f"Period_{start_date}_to_{end_date}"
        except Exception as e:
            # Default if any error occurs
            return "Unknown"

    def save_llm_format(self, llm_content, filing_metadata, output_path):
        """
        Save LLM format to a file

        Args:
            llm_content: LLM-formatted content string
            filing_metadata: Filing metadata
            output_path: Path to save the file

        Returns:
            Dict with save result
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Get original size for comparison
            original_size = len(llm_content.encode('utf-8'))
            logging.info(f"Original content size: {original_size / 1024:.2f} KB")

            # Check if this is a 10-K filing (which tends to be larger and cause recursion issues)
            filing_type = filing_metadata.get('filing_type', '')
            is_10k = filing_type == '10-K'

            # Add normalized financial statements with special handling for 10-K
            logging.info(f"Adding normalized financial statements to {output_path}")
            try:
                normalized_mapper = NormalizedFinancialMapper()
                # Use a more conservative approach for 10-K filings
                if is_10k:
                    logging.info("Using conservative mapping for 10-K filing to prevent recursion errors")
                    # Skip complex mapping operations for 10-K filings
                    llm_content = normalized_mapper.map_facts_to_financial_statements(llm_content, max_depth=2, max_children=5)
                else:
                    llm_content = normalized_mapper.map_facts_to_financial_statements(llm_content)
            except RecursionError as re:
                logging.warning(f"Recursion error during financial statement mapping: {str(re)}")
                logging.info("Proceeding with original content without financial statement mapping")
            except Exception as e:
                logging.warning(f"Error during financial statement mapping: {str(e)}")
                logging.info("Proceeding with original content without financial statement mapping")

            # Optimize file size
            logging.info(f"Optimizing file size for {output_path}")
            try:
                optimizer = FileSizeOptimizer()
                optimized_content = optimizer.optimize(llm_content)

                # Calculate size reduction
                optimized_size = len(optimized_content.encode('utf-8'))
                size_reduction = (original_size - optimized_size) / original_size * 100
                logging.info(f"Optimized content size: {optimized_size / 1024:.2f} KB (reduced by {size_reduction:.2f}%)")
            except Exception as e:
                logging.warning(f"Error during file size optimization: {str(e)}")
                logging.info("Proceeding with original content without optimization")
                optimized_content = llm_content
                optimized_size = original_size
                size_reduction = 0

            # Save file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(optimized_content)

            return {
                "success": True,
                "path": output_path,
                "size": os.path.getsize(output_path),
                "original_size": original_size,
                "optimized_size": optimized_size,
                "size_reduction_percent": size_reduction
            }
        except Exception as e:
            logging.error(f"Error saving LLM format: {str(e)}")
            return {"error": f"Error saving LLM format: {str(e)}"}

# Create a singleton instance
# recreate the singleton instance with our updated code
llm_formatter = LLMFormatter()