"""
LLM Formatter Module

Responsible for converting parsed XBRL data and narrative content to LLM-friendly format.
"""

import os
import logging
import json
import re

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
        
        output.append(f"@DOCUMENT: {ticker}-{filing_type}-{period_end}")
        output.append(f"@FILING_DATE: {filing_date}")
        output.append(f"@COMPANY: {company_name}")
        output.append(f"@CIK: {cik}")
        if fiscal_year:
            output.append(f"@FISCAL_YEAR: {fiscal_year}")
        if fiscal_period:
            output.append(f"@FISCAL_PERIOD: {fiscal_period}")
        output.append("")
        
        # Add human-readable context markers
        context_map = {}
        output.append("@CONTEXTS")
        for context_id, context in parsed_xbrl.get("contexts", {}).items():
            period_info = context.get("period", {})
            readable_label = ""
            
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
                        else:
                            # Map month to quarter (approximate)
                            quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                            # Get closest quarter
                            closest_month = min(quarter_map.keys(), key=lambda x: abs(x - end_month))
                            readable_label = f"{end_year}_{quarter_map[closest_month]}"
                    else:
                        # Multi-year period - likely annual
                        readable_label = f"FY{end_year}"
                    
                    # Store mapping from context ID to readable label
                    context_map[context_id] = readable_label
                    
                    output.append(f"@CONTEXT_DEF: {context_id} | Period: {period_info['startDate']} to {period_info['endDate']} | @LABEL: {readable_label}")
                except Exception:
                    # Fallback if parsing fails
                    output.append(f"@CONTEXT_DEF: {context_id} | Period: {period_info['startDate']} to {period_info['endDate']}")
            elif "instant" in period_info:
                # For instant dates (like balance sheet dates)
                try:
                    year = period_info['instant'].split('-')[0]
                    month = int(period_info['instant'].split('-')[1])
                    
                    if filing_type == "10-K":
                        readable_label = f"FY{year}_END"
                    else:
                        # Map month to quarter (approximate)
                        quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                        # Get closest quarter
                        closest_month = min(quarter_map.keys(), key=lambda x: abs(x - month))
                        readable_label = f"{year}_{quarter_map[closest_month]}_END"
                    
                    # Store mapping
                    context_map[context_id] = readable_label
                    
                    output.append(f"@CONTEXT_DEF: {context_id} | Instant: {period_info['instant']} | @LABEL: {readable_label}")
                except Exception:
                    # Fallback
                    output.append(f"@CONTEXT_DEF: {context_id} | Instant: {period_info['instant']}")
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
                    
                    # Select important paragraphs (limit quantity to keep file size reasonable)
                    # Focus on first few paragraphs which often contain key information
                    important_paragraphs = []
                    paragraph_count = 0
                    
                    for paragraph in paragraphs:
                        # Skip very short paragraphs and tables
                        if len(paragraph.strip()) < 100 or '|' in paragraph or '\t' in paragraph:
                            continue
                            
                        # Add paragraph
                        important_paragraphs.append(paragraph.strip())
                        paragraph_count += 1
                        
                        # Limit paragraphs per section
                        if paragraph_count >= 5:
                            break
                    
                    # Add selected paragraphs
                    for paragraph in important_paragraphs:
                        output.append(f"@NARRATIVE_TEXT: {paragraph}")
                        output.append("")
        
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