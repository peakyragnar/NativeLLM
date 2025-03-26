"""
LLM Formatter Module

Responsible for converting parsed XBRL data to LLM-friendly format.
"""

import os
import logging
import json

class LLMFormatter:
    """
    Format parsed XBRL data for LLM input
    """
    
    def __init__(self):
        """
        Initialize LLM formatter
        """
        pass
    
    def generate_llm_format(self, parsed_xbrl, filing_metadata):
        """
        Generate LLM-native format from parsed XBRL
        
        Args:
            parsed_xbrl: Parsed XBRL data
            filing_metadata: Filing metadata
            
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
        
        output.append(f"@DOCUMENT: {ticker}-{filing_type}-{period_end}")
        output.append(f"@FILING_DATE: {filing_date}")
        output.append(f"@COMPANY: {company_name}")
        output.append(f"@CIK: {cik}")
        output.append("")
        
        # Add context definitions
        output.append("@CONTEXTS")
        for context_id, context in parsed_xbrl.get("contexts", {}).items():
            period_info = context.get("period", {})
            if "startDate" in period_info and "endDate" in period_info:
                output.append(f"@CONTEXT_DEF: {context_id} | Period: {period_info['startDate']} to {period_info['endDate']}")
            elif "instant" in period_info:
                output.append(f"@CONTEXT_DEF: {context_id} | Instant: {period_info['instant']}")
        output.append("")
        
        # Add units
        output.append("@UNITS")
        for unit_id, unit_value in parsed_xbrl.get("units", {}).items():
            output.append(f"@UNIT_DEF: {unit_id} | {unit_value}")
        output.append("")
        
        # Add all facts
        sorted_facts = sorted(parsed_xbrl.get("facts", []), key=lambda x: x.get("concept", ""))
        for fact in sorted_facts:
            output.append(f"@CONCEPT: {fact.get('concept', '')}")
            output.append(f"@VALUE: {fact.get('value', '')}")
            if fact.get("unit_ref"):
                output.append(f"@UNIT_REF: {fact.get('unit_ref', '')}")
            if fact.get("decimals"):
                output.append(f"@DECIMALS: {fact.get('decimals', '')}")
            output.append(f"@CONTEXT_REF: {fact.get('context_ref', '')}")
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