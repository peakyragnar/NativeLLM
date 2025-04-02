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
from .context_extractor import extract_contexts_from_html

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
        pass
    
    def generate_llm_format(self, parsed_xbrl, filing_metadata):
        """
        Generate LLM-native format from parsed XBRL and narrative text
        
        Args:
            parsed_xbrl: Dictionary with parsed XBRL data from extractor
            filing_metadata: Dictionary with metadata about the filing
            
        Returns:
            Formatted string with LLM-friendly content
        """
        logging.info(f"Generating LLM format for {filing_metadata.get('ticker', 'unknown')} {filing_metadata.get('filing_type', 'unknown')}")
        
        output = []
        
        # Add document header with metadata
        ticker = filing_metadata.get("ticker", "unknown")
        filing_type = filing_metadata.get("filing_type", "unknown")
        fiscal_year = filing_metadata.get("fiscal_year", "unknown")
        filing_date = filing_metadata.get("filing_date", "unknown")
        
        # Construct unique document ID (ticker-filing_type-fiscal_year)
        document_name = f"{ticker}-{filing_type}-{filing_date}"
        
        output.append(f"@DOCUMENT: {document_name}")
        output.append(f"@FILING_DATE: {filing_date}")
        output.append(f"@COMPANY: {ticker}")
        
        # Add CIK if available
        if "cik" in filing_metadata:
            output.append(f"@CIK: {filing_metadata['cik']}")
        
        output.append("")
        
        # Add document structure metadata
        output.append("@STRUCTURE: Financial_Statement")
        output.append("@MAIN_CATEGORIES: Revenues, Cost_of_Revenues, Gross_Profit, Operating_Expenses, Operating_Income, Net_Income")
        output.append("@STATEMENT_TYPES: Income_Statement, Balance_Sheet, Cash_Flow_Statement, Stockholders_Equity")
        
        # Add document parts structure based on filing type
        if filing_type == "10-K":
            output.append("@DOCUMENT_PARTS: Part_I (Items_1-4), Part_II (Items_5-9), Part_III (Items_10-14), Part_IV (Items_15-16)")
            output.append("@ALL_SECTIONS: Item_1_Business, Item_1A_Risk_Factors, Item_1B_Unresolved_Comments, Item_1C_Cybersecurity, Item_2_Properties, Item_3_Legal, Item_4_Mine_Safety, Item_5_Market, Item_6_Selected_Financial, Item_7_MD&A, Item_7A_Market_Risk, Item_8_Financial_Statements, Item_9_Accountant_Changes, Item_9A_Controls, Item_9B_Other, Item_9C_Foreign_Jurisdictions, Item_10_Directors, Item_11_Compensation, Item_12_Security_Ownership, Item_13_Related_Transactions, Item_14_Accountant_Fees, Item_15_Exhibits, Item_16_Summary")
        elif filing_type == "10-Q":
            output.append("@DOCUMENT_PARTS: Part_I (Items_1-4), Part_II (Items_1-6)")
            output.append("@ALL_SECTIONS: Item_1_Financial_Statements, Item_2_MD&A, Item_3_Market_Risk, Item_4_Controls, Item_1_Legal, Item_1A_Risk_Factors, Item_2_Unregistered_Sales, Item_3_Defaults, Item_4_Mine_Safety, Item_5_Other, Item_6_Exhibits")
        
        # Add standard taxonomy based on filing type
        output.append("@SUBCATEGORIES_REVENUES: Product_Revenue, Service_Revenue, Total_Revenue")
        output.append("@SUBCATEGORIES_EXPENSES: Cost_of_Revenue, Research_Development, Sales_Marketing, General_Administrative")
        
        output.append("")
        
        # Generate context reference guide
        context_map = {}  # For human-readable context labels
        context_reference_guide = {}  # Store context details for the reference guide
        context_code_map = {}  # For explicitly labeled context references
        
        # Start context section with data dictionary format
        output.append("@DATA_DICTIONARY: CONTEXTS")
        
        # Debug - log if contexts exist
        num_contexts = len(parsed_xbrl.get('contexts', {}))
        logging.info(f"Number of contexts found in XBRL: {num_contexts}")
        
        # Direct output of context information for debugging
        if num_contexts == 0:
            # If no contexts found, output a message directly to the document
            output.append("@NOTE: No contexts found in XBRL data")
            
            # Try one more approach to extract context information directly from the HTML file
            if "filing_path" in parsed_xbrl and os.path.exists(parsed_xbrl["filing_path"]):
                try:
                    logging.info(f"Attempting direct context extraction from HTML file: {parsed_xbrl['filing_path']}")
                    with open(parsed_xbrl["filing_path"], 'r', encoding='utf-8', errors='replace') as f:
                        html_content = f.read()
                    
                    # Use the context extractor module to extract contexts
                    extracted_contexts = extract_contexts_from_html(html_content, filing_metadata)
                    
                    if extracted_contexts:
                        # Update the contexts in parsed_xbrl
                        parsed_xbrl["contexts"] = extracted_contexts
                        num_contexts = len(extracted_contexts)
                        logging.info(f"Successfully extracted {num_contexts} contexts from HTML file")
                        
                        # Update the output
                        output[-1] = f"@NOTE: Found {num_contexts} contexts via direct HTML extraction"
                    else:
                        logging.warning("No contexts found via direct HTML extraction")
                except Exception as e:
                    logging.error(f"Error during direct context extraction: {str(e)}")
        else:
            # Output a message with the number of contexts found
            output.append(f"@NOTE: Found {num_contexts} contexts in XBRL data")
        
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
                    
                    # Add to reference guide with details 
                    context_reference_guide[context_id] = {
                        "context_code": context_code,
                        "semantic_code": semantic_code,
                        "category": category_label,
                        "type": "Duration",
                        "segment": segment_text if segment_text else "None",
                        "period": f"{period_info['startDate']} to {period_info['endDate']}",
                        "description": f"{readable_label}"
                    }
                    
                except Exception as e:
                    logging.warning(f"Error processing context duration {context_id}: {str(e)}")
                    # Fallback to basic label
                    context_map[context_id] = f"Period_{context_id}"
                    context_code_map[context_id] = context_code
            
            elif "instant" in period_info:
                try:
                    instant_date = period_info['instant']
                    date_parts = instant_date.split('-')
                    year = date_parts[0]
                    month = int(date_parts[1])
                    day = date_parts[2]
                    
                    # Check for segment/dimension information
                    segment_info = ""
                    segment_text = ""
                    entity_info = context.get("entity", {})
                    segment_data = {}
                    
                    # Get segment data either from entity or directly
                    if entity_info and "segment" in entity_info:
                        segment_data = entity_info.get("segment", {})
                    elif "dimensions" in context:
                        segment_data = context.get("dimensions", {})
                    
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
                    
                    # Different label based on month (Q1, Q2, Q3, Q4)
                    # Determine if this is likely a quarter end or year end
                    quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                    
                    # Is this a typical quarter end?
                    if month in quarter_map and day in ['31', '30', '29', '28']:
                        quarter_code = quarter_map[month]
                        readable_label = f"{year}_{quarter_code}_End"
                        category_label = f"QuarterEnd_{quarter_code}_{year}"
                        semantic_code = f"BS_{year}_{quarter_code}"
                    else:
                        # Treat as a general balance sheet date
                        readable_label = f"AsOf_{year}_{month:02d}_{day}"
                        category_label = f"BalanceSheet_{year}"
                        semantic_code = f"BS_{year}_{month:02d}_{day}"
                    
                    # Add segment info if present
                    if segment_info:
                        semantic_code += f"_{segment_info}"
                        readable_label += f" ({segment_text})"
                    
                    # Store mapping
                    context_map[context_id] = readable_label
                    context_code_map[context_id] = context_code
                    
                    # Add to reference guide with details
                    context_reference_guide[context_id] = {
                        "context_code": context_code,
                        "semantic_code": semantic_code,
                        "category": category_label,
                        "type": "Instant",
                        "segment": segment_text if segment_text else "None",
                        "period": instant_date,
                        "description": readable_label
                    }
                    
                except Exception as e:
                    logging.warning(f"Error processing context instant {context_id}: {str(e)}")
                    # Fallback to basic label
                    context_map[context_id] = f"Instant_{context_id}"
                    context_code_map[context_id] = context_code
            
            else:
                # No period info, use generic label
                context_map[context_id] = f"Context_{context_id}"
                context_code_map[context_id] = context_code
        
        # Add units and scaling information
        output.append("")
        output.append("@UNITS_AND_SCALING")
        
        # Detect the monetary scale
        scale = "unspecified"
        
        # Try to determine if values are in thousands, millions, etc.
        monetary_values = []
        for fact in parsed_xbrl.get("facts", []):
            if fact.get("unit_ref") and fact.get("unit_ref").lower() in ["usd", "eur", "gbp", "jpy", "cny"]:
                try:
                    # Try to convert to float, ignore non-numeric values
                    value = fact.get("value", "").replace(",", "")
                    if value and value.strip() and value.replace(".", "").replace("-", "").isdigit():
                        monetary_values.append(float(value))
                except (ValueError, TypeError):
                    pass
        
        # Determine scale based on value range
        if monetary_values:
            avg_value = sum(monetary_values) / len(monetary_values)
            if avg_value > 1_000_000_000:
                scale = "billions"
            elif avg_value > 1_000_000:
                scale = "millions"
            elif avg_value > 1_000:
                scale = "thousands"
            else:
                scale = "ones"
        
        output.append(f"@MONETARY_SCALE: {scale}")
        output.append(f"@SCALE_NOTE: All dollar amounts are in {scale} unless otherwise specified")
        
        # Add decimals usage explanation
        output.append("@DECIMALS_USAGE:")
        
        # Extract all unique decimals values
        decimals_values = set()
        for fact in parsed_xbrl.get("facts", []):
            if "decimals" in fact and fact["decimals"] is not None:
                decimals_values.add(str(fact["decimals"]))
        
        # Add explanation for each decimals value
        for decimals in sorted(decimals_values):
            # Skip empty decimals
            if not decimals:
                continue
                
            # Handle Infinity/INF special case
            if decimals.strip().upper() in ["INF", "INFINITY"]:
                output.append(f"  inf: Values rounded to 0.0 decimal places")
                continue
                
            try:
                decimals_int = int(decimals)
                if decimals_int > 0:
                    output.append(f"  {decimals}: Values rounded to 0.{'0' * (int(decimals) - 1)}1 decimal places")
                elif decimals_int == 0:
                    output.append(f"  {decimals}: Exact values")
                else:
                    # Negative decimals indicate rounding to 10^abs(decimals)
                    rounded_to = 10 ** abs(decimals_int)
                    if rounded_to == 1000:
                        output.append(f"  {decimals}: Values rounded to thousands")
                    elif rounded_to == 1000000:
                        output.append(f"  {decimals}: Values rounded to millions")
                    elif rounded_to == 1000000000:
                        output.append(f"  {decimals}: Values rounded to billions")
                    else:
                        output.append(f"  {decimals}: Values rounded to {rounded_to} decimal places")
            except ValueError:
                output.append(f"  {decimals}: Unknown rounding")
        
        # Add some standard sections based on filing type
        if filing_type == "10-K":
            output.append("")
            output.append("@SECTION: INCOME_STATEMENT")
        elif filing_type == "10-Q":
            output.append("")
            output.append("@SECTION: QUARTERLY_RESULTS")
        
        # Initialize fact organization by section
        section_facts = {}
        
        # Helper function to determine which section a concept belongs to
        def determine_section(concept):
            concept_lower = concept.lower()
            
            # Basic mapping of concepts to sections
            if any(term in concept_lower for term in ["revenue", "sales", "income", "earning"]):
                return "INCOME_STATEMENT"
            elif any(term in concept_lower for term in ["asset", "liability", "equity", "cash and cash equivalent"]):
                return "BALANCE_SHEET"
            elif any(term in concept_lower for term in ["cash flow", "cashflow", "operating activities", "investing activities", "financing activities"]):
                return "CASH_FLOW"
            else:
                return "OTHER"
        
        # Group facts by context
        facts_by_context = {}
        for fact in parsed_xbrl.get("facts", []):
            context_ref = fact.get("context_ref")
            if context_ref:
                if context_ref not in facts_by_context:
                    facts_by_context[context_ref] = []
                facts_by_context[context_ref].append(fact)
        
        # Organize facts into tables
        financial_tables = []
        
        # Create standard tables if we have enough contexts
        if len(context_map) > 3:
            # Basic tables for financial statements
            income_statement_table = {
                "id": "INCOME_STATEMENT_TABLE",
                "title": "INCOME STATEMENT TABLE",
                "type": "INCOME STATEMENT",
                "contexts": [],
                "concepts": []
            }
            
            balance_sheet_table = {
                "id": "BALANCE_SHEET_TABLE",
                "title": "BALANCE SHEET TABLE",
                "type": "BALANCE SHEET",
                "contexts": [],
                "concepts": []
            }
            
            cash_flow_table = {
                "id": "CASH_FLOW_TABLE",
                "title": "CASH FLOW STATEMENT TABLE",
                "type": "CASH FLOW",
                "contexts": [],
                "concepts": []
            }
            
            # Add to the list of financial tables
            financial_tables.append(income_statement_table)
            financial_tables.append(balance_sheet_table)
            financial_tables.append(cash_flow_table)
        
        # Add a comprehensive table with all facts
        comprehensive_table = {
            "id": "COMPREHENSIVE_TABLE",
            "title": "ALL FINANCIAL DATA",
            "type": "COMPREHENSIVE",
            "contexts": list(context_code_map.values()),
            "concepts": []
        }
        financial_tables.append(comprehensive_table)
        
        # Build concept lists for each table
        concept_sections = {}
        
        for fact in parsed_xbrl.get("facts", []):
            concept = fact.get("concept", "")
            section = determine_section(concept)
            
            # Add concept to the appropriate section
            if concept not in concept_sections:
                concept_sections[concept] = section
                
                # Add to the appropriate table's concept list
                if section == "INCOME_STATEMENT":
                    if concept not in income_statement_table["concepts"]:
                        income_statement_table["concepts"].append(concept)
                elif section == "BALANCE_SHEET":
                    if concept not in balance_sheet_table["concepts"]:
                        balance_sheet_table["concepts"].append(concept)
                elif section == "CASH_FLOW":
                    if concept not in cash_flow_table["concepts"]:
                        cash_flow_table["concepts"].append(concept)
                
                # Add to comprehensive table
                if concept not in comprehensive_table["concepts"]:
                    comprehensive_table["concepts"].append(concept)
        
        # Add table content to output
        for table in financial_tables:
            # Only include tables with concepts
            if table["concepts"]:
                output.append("")
                output.append(f"@TABLE_CONTENT: {table['title']} (9999)")
                output.append(f"@TABLE_TYPE: {table['type']}")
                
                # Create header row with context codes
                header_row = "Concept | Key"
                
                # Use all contexts from the table, or all available if none specified
                contexts_to_use = table["contexts"] if table["contexts"] else list(context_code_map.values())
                
                for context_code in contexts_to_use:
                    header_row += f" | Context {context_code}"
                
                output.append(header_row)
                
                # Create separator row
                separator = "------- | ---"
                for _ in contexts_to_use:
                    separator += " | -----------"
                output.append(separator)
                
                # Group concepts by category for output
                grouped_concepts = {}
                for concept in table["concepts"]:
                    # Simplified categorization - just extract the first part of the concept name
                    category = concept.split(":")[0] if ":" in concept else concept.split(".")[0]
                    
                    # Specific handling for common prefixes
                    if category.lower() in ["us-gaap", "gaap", "ifrs", "dei"]:
                        # Use second part for these standard taxonomies
                        if ":" in concept:
                            category = concept.split(":")[1].split(".")[0]
                    
                    # Convert underscores to spaces and capitalize first letters
                    category = " ".join(word.capitalize() for word in category.split("_"))
                    
                    # Use Revenue, Expense, etc. as top categories
                    concept_lower = concept.lower()
                    if any(term in concept_lower for term in ["revenue", "sales"]):
                        category = "Revenue"
                    elif any(term in concept_lower for term in ["expense", "cost"]):
                        category = "Expenses"
                    elif any(term in concept_lower for term in ["asset"]):
                        category = "Assets"
                    elif any(term in concept_lower for term in ["liability", "debt"]):
                        category = "Liabilities"
                    elif any(term in concept_lower for term in ["equity", "stock"]):
                        category = "Equity"
                    elif any(term in concept_lower for term in ["cashflow", "cash flow"]):
                        category = "Cash Flow"
                    
                    if category not in grouped_concepts:
                        grouped_concepts[category] = []
                    
                    grouped_concepts[category].append(concept)
                
                # Now output facts by category
                for category, concepts in grouped_concepts.items():
                    output.append(f"<{category}> | ")
                    
                    # Output each concept in this category
                    for concept in concepts:
                        fact_row = f"  {concept} | -"
                        
                        # Add values for each context
                        for context_code in contexts_to_use:
                            # Find the context ID for this code
                            context_id = None
                            for ctx_id, code in context_code_map.items():
                                if code == context_code:
                                    context_id = ctx_id
                                    break
                            
                            # Default to empty value
                            value = ""
                            
                            # Look for a fact with this concept and context
                            if context_id:
                                for fact in parsed_xbrl.get("facts", []):
                                    if fact.get("concept") == concept and fact.get("context_ref") == context_id:
                                        # Get and clean the value
                                        raw_value = fact.get("value", "")
                                        
                                        # Try to normalize the value
                                        unit = None
                                        if fact.get("unit_ref"):
                                            unit_id = fact.get("unit_ref")
                                            unit = parsed_xbrl.get("units", {}).get(unit_id, unit_id)
                                        
                                        normalized_value = normalize_value(
                                            raw_value, 
                                            unit=unit,
                                            decimals=fact.get("decimals")
                                        )
                                        
                                        if unit and unit.lower() in ["usd", "eur", "gbp", "jpy", "cny"]:
                                            # If this is a monetary value, add appropriate symbol
                                            unit_symbol = "$" if unit.lower() == "usd" else unit
                                            value = f"{normalized_value} [{unit_symbol}{scale[0].upper()}]"
                                        else:
                                            value = normalized_value
                                        
                                        break
                            
                            fact_row += f" | {value}"
                        
                        output.append(fact_row)
                    
                    output.append(f"</{category}> | ")
        
        # Add narrative content sections if available
        narrative_sections = filing_metadata.get("document_sections", {})
        if narrative_sections:
            output.append("")
            output.append("@SECTION: NARRATIVE_SECTIONS")
            
            # Add a summary of found sections
            if "10-K" in filing_type:
                main_10k_sections = [
                    "ITEM_1_BUSINESS", "ITEM_1A_RISK_FACTORS", "ITEM_7_MD_AND_A", 
                    "ITEM_7A_MARKET_RISK", "ITEM_8_FINANCIAL_STATEMENTS"
                ]
                
                found_10k_sections = [s for s in main_10k_sections if s in narrative_sections]
                missing_sections = [s for s in main_10k_sections if s not in narrative_sections]
                
                # Items found
                output.append(f"Found {len(found_10k_sections)} of {len(main_10k_sections)} standard 10-K sections:")
                output.append(", ".join([s.replace("ITEM_", "Item ").replace("_", " ") for s in found_10k_sections]))
                
                # Items not found (might be absent in the original document)
                if missing_sections:
                    output.append(f"Missing {len(missing_sections)} section(s):")
                    output.append(", ".join([s.replace("ITEM_", "Item ").replace("_", " ") for s in missing_sections]))
                    output.append("Note: Missing sections may not exist in the original document")
            
            elif "10-Q" in filing_type:
                main_10q_sections = [
                    "ITEM_1_FINANCIAL_STATEMENTS", "ITEM_2_MD_AND_A", 
                    "ITEM_3_MARKET_RISK", "ITEM_4_CONTROLS"
                ]
                
                found_10q_sections = [s for s in main_10q_sections if s in narrative_sections or (s+"_Q" in narrative_sections)]
                missing_sections = [s for s in main_10q_sections if s not in narrative_sections and (s+"_Q" not in narrative_sections)]
                
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
        # Always include the context reference guide, even if empty
        # This ensures the section is always present
        output.append("@CONTEXT_REFERENCE_GUIDE")
        output.append("This section provides a consolidated reference for all time periods used in this document.")
        output.append("")
        
        # Enhanced Data Dictionary Table for all context references
        output.append("@DATA_DICTIONARY:")
        context_dict_rows = []
        context_dict_rows.append("Context_Code | Semantic_Code | Category | Type | Segment | Period | Description")
        context_dict_rows.append("------------|---------------|----------|------|---------|--------|------------")
        
        # Include default row if no contexts found
        if not context_reference_guide:
            # Check if we have contexts that weren't properly mapped earlier
            if parsed_xbrl and len(parsed_xbrl.get("contexts", {})) > 0:
                logging.info(f"Found {len(parsed_xbrl.get('contexts', {}))} contexts but no entries in context_reference_guide")
                
                # Create fallback context mappings
                fallback_contexts = {}
                context_counter = 0
                
                for context_id, context in parsed_xbrl.get("contexts", {}).items():
                    period_info = context.get("period", {})
                    context_counter += 1
                    context_code = f"c-{context_counter}"
                    readable_label = ""
                    category_label = ""
                    description = ""
                    segment_info = ""
                    
                    # Extract period information
                    if "instant" in period_info:
                        instant_date = period_info["instant"]
                        try:
                            year = instant_date.split('-')[0]
                            month = instant_date.split('-')[1]
                            day = instant_date.split('-')[2]
                            
                            readable_label = f"As of {year}-{month}-{day}"
                            category_label = f"Instant_{year}"
                            description = f"Balance Sheet date {year}-{month}-{day}"
                            
                            # Create semantic code
                            semantic_code = f"BS_{year}_{month}_{day}"
                            
                            # Store in fallback contexts
                            fallback_contexts[context_id] = {
                                "context_code": context_code,
                                "semantic_code": semantic_code,
                                "category": category_label,
                                "type": "Instant",
                                "period": instant_date,
                                "segment": "None",
                                "description": description
                            }
                        except Exception as e:
                            logging.warning(f"Error processing instant date {instant_date}: {str(e)}")
                            
                    elif "startDate" in period_info and "endDate" in period_info:
                        start_date = period_info["startDate"]
                        end_date = period_info["endDate"]
                        
                        try:
                            start_year = start_date.split('-')[0]
                            end_year = end_date.split('-')[0]
                            end_month = int(end_date.split('-')[1])
                            
                            # Look for dimensions/segments
                            if "dimensions" in context:
                                dimensions = context.get("dimensions", {})
                                for dim_key, dim_value in dimensions.items():
                                    if not segment_info:
                                        segment_info = f"{dim_key}_{dim_value}"
                                    else:
                                        segment_info += f", {dim_key}_{dim_value}"
                            
                            # Create meaningful labels based on date patterns
                            if start_year == end_year:
                                # Same year period (annual or quarterly)
                                if filing_type and filing_type.lower() == "10-k":
                                    readable_label = f"Fiscal Year {end_year}"
                                    category_label = f"Annual_{end_year}"
                                    semantic_code = f"FY{end_year}"
                                    description = f"Annual period from {start_date} to {end_date}"
                                else:
                                    # Determine quarter
                                    quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                                    closest_month = min(quarter_map.keys(), key=lambda x: abs(x - end_month))
                                    quarter_code = quarter_map[closest_month]
                                    
                                    readable_label = f"{end_year} {quarter_code}"
                                    category_label = f"Quarter{quarter_code}_{end_year}"
                                    semantic_code = f"{end_year}_{quarter_code}"
                                    description = f"Quarter {quarter_code} from {start_date} to {end_date}"
                            else:
                                # Multi-year period
                                readable_label = f"Period {start_year}-{end_year}"
                                category_label = f"MultiYear_{start_year}_{end_year}"
                                semantic_code = f"FY{start_year}_to_{end_year}"
                                description = f"Multi-year period from {start_date} to {end_date}"
                            
                            # Add segment info to description
                            if segment_info:
                                description += f" ({segment_info})"
                                
                            # Store in fallback contexts
                            fallback_contexts[context_id] = {
                                "context_code": context_code,
                                "semantic_code": semantic_code,
                                "category": category_label,
                                "type": "Duration",
                                "period": f"{start_date} to {end_date}",
                                "segment": segment_info if segment_info else "None",
                                "description": description
                            }
                        except Exception as e:
                            logging.warning(f"Error processing period dates {start_date} to {end_date}: {str(e)}")
                
                # Add fallback context entries to the output
                if fallback_contexts:
                    logging.info(f"Created {len(fallback_contexts)} fallback context mappings")
                    
                    # Replace the existing rows and add fallback contexts
                    context_dict_rows = []
                    context_dict_rows.append("Context_Code | Semantic_Code | Category | Type | Segment | Period | Description")
                    context_dict_rows.append("------------|---------------|----------|------|---------|--------|------------")
                    
                    # Add each fallback context
                    for context_id, context_info in fallback_contexts.items():
                        segment = context_info.get("segment", "None")
                        row = (f"{context_info['context_code']} | {context_info['semantic_code']} | "
                              f"{context_info['category']} | {context_info['type']} | {segment} | "
                              f"{context_info['period']} | {context_info['description']}")
                        context_dict_rows.append(row)
                        
                        # Also add to context reference guide
                        context_reference_guide[context_id] = context_info
                        context_code_map[context_id] = context_info['context_code']
                else:
                    # If fallback creation failed, add the original note
                    context_dict_rows.append("Note: No detailed context information available for this filing")
                    logging.warning("Failed to create fallback context mappings")
            else:
                # Add a note explaining that no context information was available
                context_dict_rows.append("Note: No detailed context information available for this filing")
                logging.warning(f"No contexts found in context_reference_guide dictionary when generating output")
                
                # Add information about the number of contexts found
                num_contexts = 0
                if parsed_xbrl:
                    num_contexts = len(parsed_xbrl.get("contexts", {}))
                logging.warning(f"Number of contexts found in original XBRL: {num_contexts}")
            
            # Add fallback mapping section header
            output.append("")
            output.append("@FALLBACK_CONTEXT_MAPPING:")
            output.append("The following context mappings are provided as a fallback based on common patterns:")
            
            # Try to get filing date without referencing potentially undefined variables
            filing_date = ""
            
            # Try safely with proper error handling
            try:
                if 'document_info' in locals() or 'document_info' in globals():
                    filing_date = document_info.get("filing_date", "")
            except Exception:
                pass
                
            # If that failed, try the other variable name
            if not filing_date:
                try:
                    if 'metadata' in locals() or 'metadata' in globals():
                        filing_date = metadata.get("filing_date", "")
                except Exception:
                    pass
            # As a last resort, try to get year from parsed_xbrl
            if not filing_date and parsed_xbrl.get("document_info", {}).get("document", ""):
                try:
                    # Try to extract year from document name (e.g., NVDA-10-K-2025-01-26)
                    doc_name = parsed_xbrl.get("document_info", {}).get("document", "")
                    year_match = re.search(r'(\d{4})', doc_name)
                    
                    if year_match:
                        filing_date = f"{year_match.group(1)}-01-01"
                except Exception:
                    pass
            
            # Provide a mapping of common context codes to likely meanings
            filing_year = fiscal_year if fiscal_year and fiscal_year != "unknown" else "2023"
            
            # Common patterns
            output.append(f"c-1: Current reporting period (fiscal year {filing_year})")
            output.append(f"c-2: Prior fiscal year ({int(filing_year)-1})")
            output.append(f"c-3: Balance sheet date (end of {filing_year})")
            output.append(f"c-4: Balance sheet date (end of {int(filing_year)-1})")
            output.append("c-5+: Additional periods or segments")
            
            # Warning about heuristic nature
            output.append("")
            output.append("Note: These mappings are approximations and may not precisely match the actual periods")
        else:
            # Add each context reference as a row in the dictionary table
            for context_id, context_info in context_reference_guide.items():
                segment = context_info.get("segment", "None")
                row = (f"{context_info['context_code']} | {context_info['semantic_code']} | "
                      f"{context_info['category']} | {context_info['type']} | {segment} | "
                      f"{context_info['period']} | {context_info['description']}")
                context_dict_rows.append(row)
        
        # Add all context dictionary rows to output
        for row in context_dict_rows:
            output.append(row)
        
        # Create helper functions for extracting and manipulating the data
        output.append("")
        output.append("@HELPER_FUNCTIONS:")
        output.append("The following functions can be used to extract and analyze the financial data:")
        output.append("")
        
        # Revenue extraction function
        output.append("def extract_revenue(document, context_code):")
        output.append("    # Find the revenue section in the document")
        output.append("    start_index = document.find('<Revenue>')")
        output.append("    end_index = document.find('</Revenue>', start_index)")
        output.append("    if start_index == -1 or end_index == -1:")
        output.append("        return 'Revenue section not found'")
        output.append("    ")
        output.append("    # Extract the revenue section")
        output.append("    revenue_section = document[start_index:end_index]")
        output.append("    ")
        output.append("    # Split into lines and find the line with the specified context")
        output.append("    for line in revenue_section.split('\\n'):")
        output.append("        if f'Context {context_code}' in line:")
        output.append("            # Extract the value from the line")
        output.append("            parts = line.split('|')")
        output.append("            # Look for the context column index")
        output.append("            header_parts = document.split('\\n')[0].split('|')")
        output.append("            context_index = -1")
        output.append("            for i, part in enumerate(header_parts):")
        output.append("                if f'Context {context_code}' in part:")
        output.append("                    context_index = i")
        output.append("                    break")
        output.append("            # Get the value at the same index")
        output.append("            if context_index >= 0 and context_index < len(parts):")
        output.append("                return parts[context_index].strip()")
        output.append("    ")
        output.append("    return 'No revenue found for this context'")
        
        # Format as single string
        formatted_output = "\n".join(output)
        
        return formatted_output

# Global instance
llm_formatter = LLMFormatter()
