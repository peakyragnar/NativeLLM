"""
LLM Formatter Module

Responsible for converting parsed XBRL data and narrative content to LLM-friendly format.
"""

import os
import logging
import json
import re
import datetime

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
            "ITEM_1_BUSINESS": "Business",
            "ITEM_1A_RISK_FACTORS": "Risk Factors",
            "ITEM_7_MD_AND_A": "Management's Discussion and Analysis",
            "MANAGEMENT_DISCUSSION": "Management's Discussion and Analysis",
            "ITEM_7A_MARKET_RISK": "Market Risk",
            "ITEM_8_FINANCIAL_STATEMENTS": "Financial Statements",
            "ITEM_2_MD_AND_A": "Management's Discussion and Analysis",
            "ITEM_3_MARKET_RISK": "Market Risk",
            "ITEM_1_FINANCIAL_STATEMENTS": "Financial Statements",
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
            
        # Initialize data integrity tracking
        self.data_integrity = {
            "tables_detected": 0,
            "tables_included": 0,
            "total_table_rows": 0,
            "narrative_paragraphs": 0,
            "included_paragraphs": 0,
            "section_tables": {}
        }
        
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
        if filing_type == "10-K":
            output.append("@STATEMENT_TYPES: Income_Statement, Balance_Sheet, Cash_Flow_Statement, Stockholders_Equity")
        else:
            output.append("@STATEMENT_TYPES: Income_Statement, Balance_Sheet, Cash_Flow_Statement")
        output.append("@SUBCATEGORIES_REVENUES: Product_Revenue, Service_Revenue, Total_Revenue")
        output.append("@SUBCATEGORIES_EXPENSES: Cost_of_Revenue, Research_Development, Sales_Marketing, General_Administrative")
        output.append("")
        
        # Create enhanced context data dictionary
        context_map = {}
        context_reference_guide = {}  # Store context details for the reference guide
        context_code_map = {}  # For explicitly labeled context references
        
        # Start context section with data dictionary format
        output.append("@DATA_DICTIONARY: CONTEXTS")
        
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
                    
                    if start_year == end_year:
                        # Same year period - could be quarterly or annual
                        if filing_type == "10-K":
                            readable_label = f"FY{end_year}"
                            category_label = f"Annual_{end_year}"
                        else:
                            # Map month to quarter (approximate)
                            quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                            # Get closest quarter
                            closest_month = min(quarter_map.keys(), key=lambda x: abs(x - end_month))
                            readable_label = f"{end_year}_{quarter_map[closest_month]}"
                            category_label = f"Quarter{quarter_map[closest_month]}_{end_year}"
                    else:
                        # Multi-year period - likely annual
                        readable_label = f"FY{end_year}"
                        category_label = f"Annual_{end_year}"
                    
                    # Store mapping from context ID to readable label
                    context_map[context_id] = readable_label
                    context_code_map[context_id] = context_code
                    
                    # Store information for context reference guide
                    context_reference_guide[readable_label] = {
                        "type": "period",
                        "code": context_code,
                        "category": category_label,
                        "start_date": period_info['startDate'],
                        "end_date": period_info['endDate'],
                        "description": f"Period from {period_info['startDate']} to {period_info['endDate']}"
                    }
                    
                    # Enhanced explicit context labeling
                    output.append(f"{context_code}: {category_label} | Period: {period_info['startDate']} to {period_info['endDate']} | @LABEL: {readable_label}")
                except Exception:
                    # Fallback if parsing fails
                    output.append(f"{context_code}: Period: {period_info['startDate']} to {period_info['endDate']}")
            elif "instant" in period_info:
                # For instant dates (like balance sheet dates)
                try:
                    year = period_info['instant'].split('-')[0]
                    month = int(period_info['instant'].split('-')[1])
                    
                    if filing_type == "10-K":
                        readable_label = f"FY{year}_END"
                        category_label = f"Annual_{year}_End"
                    else:
                        # Map month to quarter (approximate)
                        quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                        # Get closest quarter
                        closest_month = min(quarter_map.keys(), key=lambda x: abs(x - month))
                        readable_label = f"{year}_{quarter_map[closest_month]}_END"
                        category_label = f"Quarter{quarter_map[closest_month]}_{year}_End"
                    
                    # Store mapping
                    context_map[context_id] = readable_label
                    context_code_map[context_id] = context_code
                    
                    # Store information for context reference guide
                    context_reference_guide[readable_label] = {
                        "type": "instant",
                        "code": context_code,
                        "category": category_label,
                        "date": period_info['instant'],
                        "description": f"Point in time at {period_info['instant']}"
                    }
                    
                    # Enhanced explicit context labeling
                    output.append(f"{context_code}: {category_label} | Instant: {period_info['instant']} | @LABEL: {readable_label}")
                except Exception:
                    # Fallback
                    output.append(f"{context_code}: Instant: {period_info['instant']}")
        output.append("")
        
        # Add units
        output.append("@UNITS")
        for unit_id, unit_value in parsed_xbrl.get("units", {}).items():
            output.append(f"@UNIT_DEF: {unit_id} | {unit_value}")
        output.append("")
        
        # Check for narrative content
        extracted_sections = {}
        
        # Try to get narrative content from filing_metadata
        if "html_content" in filing_metadata and isinstance(filing_metadata["html_content"], dict):
            html_content = filing_metadata["html_content"]
            if "document_sections" in html_content:
                for section_id, section_info in html_content.get("document_sections", {}).items():
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
        
        # Add key financial facts with improved organization
        # Group facts by type (Income Statement, Balance Sheet, etc.)
        
        # Organize facts by section and store for sorting later
        financial_sections = {
            "INCOME_STATEMENT": [],
            "BALANCE_SHEET": [],
            "CASH_FLOW": [],
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
            else:
                return "OTHER_FINANCIAL"
        
        # Track table data for integrity checks
        self.data_integrity["xbrl_facts"] = len(parsed_xbrl.get("facts", []))
        self.data_integrity["xbrl_tables_created"] = 0
        
        # Categorize facts by financial section
        for fact in parsed_xbrl.get("facts", []):
            concept = fact.get("concept", "")
            section = determine_section(concept)
            financial_sections[section].append(fact)
        
        # Track facts by context reference to build tables
        facts_by_context = {}
        for fact in parsed_xbrl.get("facts", []):
            context_ref = fact.get("context_ref", "")
            if context_ref not in facts_by_context:
                facts_by_context[context_ref] = []
            facts_by_context[context_ref].append(fact)
        
        # Add facts from each section - with both individual facts and tabular format
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
                    
                    # Sort concepts for hierarchical organization
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
                            
                        if category not in organized_concepts:
                            organized_concepts[category] = []
                        organized_concepts[category].append((concept_name, facts))
                    
                    # Add data rows with hierarchical organization
                    for category in ["Revenue", "Expenses", "Profit", "Income", "Assets", "Liabilities", "Equity", "Cash_Flow", "Other"]:
                        if category in organized_concepts:
                            # Add category header
                            if len(organized_concepts[category]) > 1:
                                category_row = f"<{category}>"
                                for _ in range(len(top_contexts) + 1):  # +1 for Key column
                                    category_row += " | "
                                table_rows.append(category_row)
                            
                            # Add items in this category
                            for concept_name, facts in organized_concepts[category]:
                                # Create a mapping from context to fact for this concept
                                context_to_fact = {fact.get("context_ref", ""): fact for fact in facts}
                                
                                # Only add rows that have data in at least one of our top contexts
                                if any(context_ref in context_to_fact for context_ref, _ in top_contexts):
                                    # Add hierarchical indentation for subcategories
                                    if len(organized_concepts[category]) > 1:
                                        row = f"  {concept_name}"
                                    else:
                                        row = concept_name
                                    
                                    # Add context key reference
                                    context_keys = []
                                    for context_ref, _ in top_contexts:
                                        if context_ref in context_to_fact and context_ref in context_code_map:
                                            context_keys.append(context_code_map[context_ref])
                                    
                                    if context_keys:
                                        row += f" | {','.join(context_keys)}"
                                    else:
                                        row += " | -"
                                    
                                    # Add values for each context
                                    for context_ref, _ in top_contexts:
                                        if context_ref in context_to_fact:
                                            fact = context_to_fact[context_ref]
                                            value = fact.get("value", "")
                                            # Add currency symbol if available
                                            unit_ref = fact.get("unit_ref", "")
                                            if unit_ref and unit_ref.lower() == "usd":
                                                # Only add $ if it's not already there
                                                if not value.startswith("$"):
                                                    value = f"${value}"
                                            row += f" | {value}"
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
                    context_ref = fact.get("context_ref", "")
                    if context_ref in context_map:
                        output.append(f"@CONTEXT_REF: {context_ref} | @CONTEXT: {context_map[context_ref]}")
                    else:
                        output.append(f"@CONTEXT_REF: {context_ref}")
                    output.append("")
        
        # Add narrative sections
        if extracted_sections:
            # Priority order for sections
            priority_sections = [
                "ITEM_1_BUSINESS",
                "ITEM_1A_RISK_FACTORS", 
                "ITEM_7_MD_AND_A",
                "MANAGEMENT_DISCUSSION",
                "RESULTS_OF_OPERATIONS",
                "LIQUIDITY_AND_CAPITAL"
            ]
            
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
        output.append("")
        
        # Enhanced Context Reference Guide and Data Dictionary
        if context_reference_guide:
            output.append("@CONTEXT_REFERENCE_GUIDE")
            output.append("This section provides a consolidated reference for all time periods used in this document.")
            output.append("")
            
            # Data Dictionary Table for all context references
            output.append("@DATA_DICTIONARY:")
            context_dict_rows = []
            context_dict_rows.append("Context_Code | Category | Type | Period | Description")
            context_dict_rows.append("------------|----------|------|--------|------------")
            
            # Sort by context code for logical ordering
            sorted_items = sorted([(info["code"], label) for label, info in context_reference_guide.items() if "code" in info])
            
            for code, label in sorted_items:
                info = context_reference_guide[label]
                category = info.get("category", "Unknown")
                type_str = info.get("type", "Unknown")
                
                # Format period information
                if type_str == "period":
                    period = f"{info.get('start_date', '')} to {info.get('end_date', '')}"
                elif type_str == "instant":
                    period = f"As of {info.get('date', '')}"
                else:
                    period = "Unknown"
                
                description = info.get("description", "")
                context_dict_rows.append(f"{code} | {category} | {type_str} | {period} | {description}")
            
            output.append("\n".join(context_dict_rows))
            output.append("")
            
            # Create a verification section with all verification formulas
            output.append("@VERIFICATION_KEYS:")
            output.append("These formulas can be used to verify data consistency:")
            
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
                    output.append(f"- Revenue_{year} = Product_Revenue_{year} + Service_Revenue_{year} (contexts: {', '.join(codes)})")
                    output.append(f"- Gross_Profit_{year} = Revenue_{year} - Cost_of_Revenue_{year} (contexts: {', '.join(codes)})")
                    output.append(f"- Operating_Income_{year} = Gross_Profit_{year} - Operating_Expenses_{year} (contexts: {', '.join(codes)})")
            output.append("")
            
            # First list period contexts (fiscal years, quarters)
            period_labels = [label for label in sorted_labels if context_reference_guide[label]["type"] == "period"]
            if period_labels:
                output.append("@PERIOD_CONTEXTS")
                for label in period_labels:
                    context_info = context_reference_guide[label]
                    output.append(f"- {label}: {context_info['description']} (code: {context_info.get('code', 'Unknown')})")
                output.append("")
            
            # Then list instant contexts (balance sheet dates)
            instant_labels = [label for label in sorted_labels if context_reference_guide[label]["type"] == "instant"]
            if instant_labels:
                output.append("@INSTANT_CONTEXTS")
                for label in instant_labels:
                    context_info = context_reference_guide[label]
                    output.append(f"- {label}: {context_info['description']} (code: {context_info.get('code', 'Unknown')})")
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
        output.append("# Extract revenue information for specific period")
        output.append("def extract_revenue(document, context_code):")
        output.append("    # Find the Revenue section")
        output.append("    revenue_section = find_section(document, '<Revenue>')")
        output.append("    ")
        output.append("    # Extract all line items with their context codes")
        output.append("    revenue_data = {}")
        output.append("    for line in revenue_section:")
        output.append("        if context_code in line.split('|')[1].strip():")
        output.append("            item_name = line.split('|')[0].strip()")
        output.append("            # Find the value corresponding to the context code")
        output.append("            columns = line.split('|')")
        output.append("            # The value is in the column after the Key column")
        output.append("            value = get_value_for_context(columns, context_code)")
        output.append("            revenue_data[item_name] = value")
        output.append("    ")
        output.append("    return revenue_data")
        output.append("```")
        output.append("")
        
        output.append("Example data format for extraction:")
        output.append("")
        output.append("```json")
        output.append("{")
        output.append("  \"Revenue\": {")
        output.append("    \"Product_Revenue\": {")
        output.append("      \"c-1\": 72480,")
        output.append("      \"c-2\": 65240")
        output.append("    },")
        output.append("    \"Service_Revenue\": {")
        output.append("      \"c-1\": 16320,")
        output.append("      \"c-2\": 13950")
        output.append("    },")
        output.append("    \"Total_Revenue\": {")
        output.append("      \"c-1\": 88800,")
        output.append("      \"c-2\": 79190")
        output.append("    }")
        output.append("  }")
        output.append("}")
        output.append("```")
        
        return "\n".join(output)
    
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
            
            # Save file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(llm_content)
            
            return {
                "success": True,
                "path": output_path,
                "size": os.path.getsize(output_path)
            }
        except Exception as e:
            logging.error(f"Error saving LLM format: {str(e)}")
            return {"error": f"Error saving LLM format: {str(e)}"}

# Create a singleton instance
llm_formatter = LLMFormatter()