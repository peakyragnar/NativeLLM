# src/formatter/llm_formatter.py
import os
import sys
import json

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import PROCESSED_DATA_DIR

def generate_llm_format(parsed_xbrl, filing_metadata):
    """Generate LLM-native format from parsed XBRL"""
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

def save_llm_format(llm_content, filing_metadata):
    """Save LLM format to a file"""
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    period_end = filing_metadata.get("period_end_date", "unknown").replace("-", "")
    
    # Create directory
    dir_path = os.path.join(PROCESSED_DATA_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    
    # Create filename
    filename = f"{ticker}_{filing_type}_{period_end}_llm.txt"
    file_path = os.path.join(dir_path, filename)
    
    # Save file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(llm_content)
    
    return {
        "success": True,
        "file_path": file_path,
        "size": len(llm_content)
    }