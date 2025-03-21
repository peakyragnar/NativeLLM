# src/formatter/llm_formatter.py
import os
import sys
import json
import re

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import PROCESSED_DATA_DIR

def normalize_unit(unit):
    """
    Standardize unit representations for consistency.
    
    Rules:
    1. Convert currency codes to uppercase (usd → USD)
    2. Standardize common financial units
    3. Normalize variations of the same unit
    
    Returns both the normalized unit and whether it was changed
    """
    if not unit:
        return unit, False
    
    original = unit.strip()
    
    # Standard currency codes should be uppercase
    currency_codes = {
        'usd': 'USD', 'eur': 'EUR', 'gbp': 'GBP', 'jpy': 'JPY', 'cny': 'CNY',
        'cad': 'CAD', 'aud': 'AUD', 'chf': 'CHF', 'inr': 'INR'
    }
    
    # Handle common unit variations
    unit_variations = {
        # Currency variations
        'iso4217:usd': 'USD', 'iso4217:USD': 'USD', 'us-gaap:usd': 'USD', 'us-gaap:USD': 'USD',
        'dollars': 'USD', 'us dollars': 'USD', 'u.s. dollars': 'USD', '$': 'USD',
        
        # Share variations
        'shares': 'Shares', 'share': 'Shares', 'sh': 'Shares', 
        'iso4217:shares': 'Shares', 'us-gaap:shares': 'Shares',
        
        # Percentage variations
        'percent': 'Percent', '%': 'Percent', 'pct': 'Percent',
        'pure': 'Pure', 'xbrli:pure': 'Pure',
        
        # Rate variations
        'perShare': 'PerShare', 'per_share': 'PerShare', 'per share': 'PerShare',
        
        # Time variations
        'years': 'Years', 'year': 'Years', 'months': 'Months', 'month': 'Months',
        'days': 'Days', 'day': 'Days',
    }
    
    # Check for exact matches in unit variations
    normalized = original.lower()
    if normalized in unit_variations:
        return unit_variations[normalized], True
    
    # Check if it's a currency code (simple 3-letter code)
    if len(normalized) == 3 and normalized.isalpha() and normalized in currency_codes:
        return currency_codes[normalized], True
    
    # Handle currency with prefix (like iso4217:USD)
    if ':' in normalized:
        prefix, code = normalized.split(':', 1)
        if code.lower() in currency_codes:
            return currency_codes[code.lower()], True
    
    # If no standardization applied, return original
    return original, False

def normalize_concept_name(concept_name):
    """
    Normalize XBRL concept names to ensure consistent formatting.
    
    Rules:
    1. Trim leading/trailing whitespace
    2. Ensure camelCase format (first letter capitalized for each word)
    3. Replace special characters with standardized alternatives
    4. Preserve acronyms (sequences of uppercase letters)
    
    Returns both the normalized name and whether it was changed
    """
    if not concept_name:
        return concept_name, False
    
    original = concept_name.strip()
    normalized = original
    
    # Handle special characters - replace with standardized forms
    replacements = {
        '&': 'And',
        '+': 'Plus',
        '-': '',  # Remove hyphens between words
        '_': '',  # Remove underscores between words
    }
    
    for char, replacement in replacements.items():
        normalized = normalized.replace(char, replacement)
    
    # Split on non-alphanumeric characters and capitalize each part
    # This creates camelCase with first letter capitalized
    parts = re.findall(r'[A-Za-z0-9]+', normalized)
    if not parts:
        return original, False
    
    # Capitalize first letter of each part, preserving acronyms
    processed_parts = []
    for part in parts:
        # Check if the part is an acronym (all uppercase)
        if part.isupper() and len(part) > 1:
            processed_parts.append(part)  # Preserve acronyms
        else:
            processed_parts.append(part.capitalize())  # Capitalize normal words
    
    normalized = ''.join(processed_parts)
    
    # Ensure first letter is capitalized
    if normalized and normalized[0].islower():
        normalized = normalized[0].upper() + normalized[1:]
    
    # Return the normalized name and whether it changed
    was_changed = (normalized != original)
    return normalized, was_changed

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
    
    # Add units with standardization
    output.append("@UNITS")
    
    # Track normalized units for reporting
    unit_normalization_changes = {}
    
    for unit_id, unit_value in parsed_xbrl.get("units", {}).items():
        # Normalize the unit value
        original_value = unit_value.strip()
        normalized_value, was_changed = normalize_unit(original_value)
        
        # Track changes for reporting
        if was_changed and original_value not in unit_normalization_changes:
            unit_normalization_changes[original_value] = normalized_value
        
        output.append(f"@UNIT_DEF: {unit_id.strip()} | {normalized_value}")
    output.append("")
    
    # Add all facts
    sorted_facts = sorted(parsed_xbrl.get("facts", []), key=lambda x: x.get("concept", ""))
    
    # Track normalized concepts for reporting
    normalization_changes = {}
    
    for fact in sorted_facts:
        # Get values and normalize whitespace
        original_concept = fact.get('concept', '').strip()
        value = fact.get('value', '').strip()
        
        # Skip empty concepts
        if not original_concept:
            continue
        
        # Normalize the concept name
        concept, was_changed = normalize_concept_name(original_concept)
        
        # Track changes for reporting
        if was_changed and original_concept not in normalization_changes:
            normalization_changes[original_concept] = concept
        
        # Normalize whitespace in value (replace multiple spaces with single space)
        value = ' '.join(value.split())
            
        output.append(f"@CONCEPT: {concept}")
        output.append(f"@VALUE: {value}")
        
        unit_ref = fact.get("unit_ref")
        if unit_ref:
            # Standardize unit references
            original_unit_ref = unit_ref.strip()
            normalized_unit_ref, was_changed = normalize_unit(original_unit_ref)
            
            # Track changes for reporting
            if was_changed and original_unit_ref not in unit_normalization_changes:
                unit_normalization_changes[original_unit_ref] = normalized_unit_ref
                
            output.append(f"@UNIT_REF: {normalized_unit_ref}")
            
        decimals = fact.get("decimals")
        if decimals:
            output.append(f"@DECIMALS: {decimals.strip()}")
            
        context_ref = fact.get("context_ref", "").strip()
        output.append(f"@CONTEXT_REF: {context_ref}")
        output.append("")
    
    # Add normalization mapping as a comment section at the end if any changes were made
    if normalization_changes:
        output.append("@CONCEPT_NORMALIZATION_MAP")
        for original, normalized in sorted(normalization_changes.items()):
            output.append(f"@ORIGINAL: {original}")
            output.append(f"@NORMALIZED: {normalized}")
            output.append("")
    
    # Add unit normalization mapping if any changes were made
    if unit_normalization_changes:
        output.append("@UNIT_NORMALIZATION_MAP")
        for original, normalized in sorted(unit_normalization_changes.items()):
            output.append(f"@ORIGINAL: {original}")
            output.append(f"@NORMALIZED: {normalized}")
            output.append("")
    
    # Create a cross-reference index mapping concepts to all contexts where they appear
    concept_context_map = {}
    
    # First build the mapping
    for fact in sorted_facts:
        concept = fact.get('concept', '').strip()
        context_ref = fact.get('context_ref', '').strip()
        
        if not concept or not context_ref:
            continue
            
        if concept not in concept_context_map:
            concept_context_map[concept] = []
            
        if context_ref not in concept_context_map[concept]:
            concept_context_map[concept].append(context_ref)
    
    # Add the cross-reference index to the output if we found any mappings
    if concept_context_map:
        output.append("@CONCEPT_CONTEXT_MAP")
        
        # Sort concepts alphabetically for consistent output
        for concept, context_refs in sorted(concept_context_map.items()):
            output.append(f"@CONCEPT: {concept}")
            
            # Sort context references for consistency
            sorted_context_refs = sorted(context_refs)
            
            # Format with comma separation for readability
            if len(sorted_context_refs) <= 10:
                # For shorter lists, put them on one line for compact representation
                contexts_str = ", ".join(sorted_context_refs)
                output.append(f"@CONTEXTS: {contexts_str}")
            else:
                # For longer lists, use multiple lines for better readability
                output.append("@CONTEXTS:")
                # Group context references for better readability (5 per line)
                for i in range(0, len(sorted_context_refs), 5):
                    group = sorted_context_refs[i:i+5]
                    output.append(f"  {', '.join(group)}")
            
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