"""
Module for formatting iXBRL data into LLM-friendly format.
"""

import os
import sys
import json
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import PROCESSED_DATA_DIR

def format_context(context_id, context_data):
    """Format a context definition into a human-readable string"""
    output = []
    output.append(f"@CONTEXT_DEF: {context_id}")
    
    # Format period information
    period = context_data.get('period', {})
    if 'instant' in period:
        output.append(f"  Period: Instant {period['instant']}")
    elif 'startDate' in period and 'endDate' in period:
        output.append(f"  Period: {period['startDate']} to {period['endDate']}")
    
    # Format entity information
    entity = context_data.get('entity', {})
    if entity:
        entity_id = entity.get('identifier', '')
        scheme = entity.get('scheme', '')
        if entity_id and scheme:
            output.append(f"  Entity: {entity_id} ({scheme})")
    
    # Format dimensions
    dimensions = context_data.get('dimensions', {})
    if dimensions:
        dim_strs = []
        for dim, value in dimensions.items():
            dim_strs.append(f"{dim}={value}")
        if dim_strs:
            output.append(f"  Dimensions: {', '.join(dim_strs)}")
    
    return '\n'.join(output)

def format_unit(unit_id, unit_data):
    """Format a unit definition into a human-readable string"""
    output = []
    output.append(f"@UNIT_DEF: {unit_id}")
    
    unit_type = unit_data.get('type', '')
    if unit_type == 'simple':
        measure = unit_data.get('measure', '')
        output.append(f"  Measure: {measure}")
    elif unit_type == 'divide':
        numerator = unit_data.get('numerator', '')
        denominator = unit_data.get('denominator', '')
        output.append(f"  Divide: {numerator} / {denominator}")
    
    return '\n'.join(output)

def format_fact(fact):
    """Format a fact into a human-readable string"""
    output = []
    
    # Format common properties
    name = fact.get('name', '')
    if ':' in name:
        namespace, local_name = name.split(':', 1)
        output.append(f"@CONCEPT: {local_name}")
        output.append(f"@NAMESPACE: {namespace}")
    else:
        output.append(f"@CONCEPT: {name}")
    
    # Format value
    value = fact.get('value', '')
    output.append(f"@VALUE: {value}")
    
    # Format context reference
    context_ref = fact.get('context_ref', '')
    if context_ref:
        output.append(f"@CONTEXT_REF: {context_ref}")
    
    # Format unit reference (for numeric facts)
    if fact.get('type') == 'nonFraction':
        unit_ref = fact.get('unit_ref', '')
        if unit_ref:
            output.append(f"@UNIT_REF: {unit_ref}")
        
        # Add decimals and scale if available
        decimals = fact.get('decimals', '')
        if decimals:
            output.append(f"@DECIMALS: {decimals}")
        
        scale = fact.get('scale', '')
        if scale:
            output.append(f"@SCALE: {scale}")
    
    # Note if this was a hidden fact
    if fact.get('hidden', False):
        output.append("@HIDDEN: true")
    
    return '\n'.join(output)

def generate_llm_format_from_ixbrl(extracted_data, filing_metadata=None):
    """
    Generate LLM-native format from extracted iXBRL data.
    
    Parameters:
    - extracted_data: Dictionary containing contexts, units, and facts from iXBRL
    - filing_metadata: Optional metadata about the filing
    
    Returns:
    - String containing the LLM-friendly format of the data
    """
    output = []
    
    # Add document metadata
    document_info = extracted_data.get('document_info', {})
    if filing_metadata:
        document_info.update(filing_metadata)
    
    ticker = document_info.get("ticker", "unknown")
    filing_type = document_info.get("filing_type", "unknown")
    company_name = document_info.get("company_name", "unknown")
    cik = document_info.get("cik", "unknown")
    filing_date = document_info.get("filing_date", "unknown")
    period_end = document_info.get("period_end_date", "unknown")
    
    output.append(f"@DOCUMENT: {ticker}-{filing_type}-{period_end}")
    output.append(f"@FILING_DATE: {filing_date}")
    output.append(f"@COMPANY: {company_name}")
    output.append(f"@CIK: {cik}")
    output.append(f"@FORMAT: iXBRL")
    output.append(f"@FACTS: {extracted_data.get('fact_count', 0)}")
    output.append(f"@CONTEXTS: {extracted_data.get('context_count', 0)}")
    output.append(f"@UNITS: {extracted_data.get('unit_count', 0)}")
    output.append("")
    
    # Add context definitions
    output.append("@CONTEXTS")
    contexts = extracted_data.get('contexts', {})
    for context_id, context_data in contexts.items():
        output.append(format_context(context_id, context_data))
        output.append("")
    
    # Add unit definitions
    output.append("@UNITS")
    units = extracted_data.get('units', {})
    for unit_id, unit_data in units.items():
        output.append(format_unit(unit_id, unit_data))
        output.append("")
    
    # Add facts
    output.append("@FACTS")
    facts = extracted_data.get('facts', [])
    
    # Group facts by concept name to make it more organized
    facts_by_concept = {}
    for fact in facts:
        name = fact.get('name', '')
        if name not in facts_by_concept:
            facts_by_concept[name] = []
        facts_by_concept[name].append(fact)
    
    # Sort concept names for consistency
    sorted_concepts = sorted(facts_by_concept.keys())
    
    # Output facts grouped by concept
    for concept in sorted_concepts:
        for fact in facts_by_concept[concept]:
            output.append(format_fact(fact))
            output.append("")
    
    return '\n'.join(output)

def save_llm_format(llm_content, filing_metadata):
    """Save LLM format to a file"""
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    
    # Use period end date if available, otherwise use current date
    if filing_metadata.get("period_end_date"):
        period_end = filing_metadata.get("period_end_date", "").replace("-", "")
    else:
        period_end = datetime.now().strftime("%Y%m%d")
    
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