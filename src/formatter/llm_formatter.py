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
    
    output.append(f"@DOCUMENT: {ticker.strip()}-{filing_type.strip()}-{period_end.strip()}")
    output.append(f"@FILING_DATE: {filing_date.strip()}")
    output.append(f"@COMPANY: {company_name.strip()}")
    output.append(f"@CIK: {cik.strip()}")
    output.append("")
    
    # First collect all unique periods and instants
    periods = {}  # Maps period strings to period IDs
    instants = {}  # Maps instant strings to instant IDs
    
    for context_id, context in parsed_xbrl.get("contexts", {}).items():
        period_info = context.get("period", {})
        if "startDate" in period_info and "endDate" in period_info:
            start_date = period_info['startDate'].strip()
            end_date = period_info['endDate'].strip()
            period_str = f"{start_date} to {end_date}"
            if period_str not in periods:
                periods[period_str] = f"p-{len(periods) + 1}"
        elif "instant" in period_info:
            instant = period_info['instant'].strip()
            if instant not in instants:
                instants[instant] = f"i-{len(instants) + 1}"
    
    # Add period definitions
    output.append("@PERIODS")
    for period_str, period_id in periods.items():
        output.append(f"@PERIOD_DEF: {period_id} | {period_str}")
    for instant_str, instant_id in instants.items():
        output.append(f"@INSTANT_DEF: {instant_id} | {instant_str}")
    output.append("")
    
    # Add context definitions with references to period/instant IDs
    output.append("@CONTEXTS")
    for context_id, context in parsed_xbrl.get("contexts", {}).items():
        period_info = context.get("period", {})
        if "startDate" in period_info and "endDate" in period_info:
            start_date = period_info['startDate'].strip()
            end_date = period_info['endDate'].strip()
            period_str = f"{start_date} to {end_date}"
            period_id = periods[period_str]
            output.append(f"@CONTEXT_DEF: {context_id.strip()} | PERIOD: {period_id}")
        elif "instant" in period_info:
            instant = period_info['instant'].strip()
            instant_id = instants[instant]
            output.append(f"@CONTEXT_DEF: {context_id.strip()} | INSTANT: {instant_id}")
    output.append("")
    
    # Add units
    output.append("@UNITS")
    for unit_id, unit_value in parsed_xbrl.get("units", {}).items():
        output.append(f"@UNIT_DEF: {unit_id.strip()} | {unit_value.strip()}")
    output.append("")
    
    # Add all facts
    sorted_facts = sorted(parsed_xbrl.get("facts", []), key=lambda x: x.get("concept", ""))
    for fact in sorted_facts:
        # Get and normalize values, removing extra whitespace
        concept = fact.get('concept', '').strip()
        value = fact.get('value', '').strip()
        
        # Normalize whitespace in value (replace multiple spaces with single space)
        value = ' '.join(value.split())
        
        # Skip empty concepts or values
        if not concept:
            continue
            
        output.append(f"@CONCEPT: {concept}")
        output.append(f"@VALUE: {value}")
        
        unit_ref = fact.get("unit_ref")
        if unit_ref:
            output.append(f"@UNIT_REF: {unit_ref.strip()}")
            
        decimals = fact.get("decimals")
        if decimals:
            output.append(f"@DECIMALS: {decimals.strip()}")
            
        context_ref = fact.get("context_ref", "").strip()
        output.append(f"@CONTEXT_REF: {context_ref}")
        output.append("")
    
    # Join with single newlines and return
    formatted_output = "\n".join(output)
    
    # Remove any triple+ newlines (normalize to double newlines max)
    while "\n\n\n" in formatted_output:
        formatted_output = formatted_output.replace("\n\n\n", "\n\n")
        
    return formatted_output

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