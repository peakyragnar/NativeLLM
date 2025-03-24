# src/formatter/llm_formatter.py
import os
import sys
import json
import re
import logging

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
    
    # Import normalize_special_chars function
    from src.formatter.normalize_value import normalize_special_chars
    
    # Add document metadata
    ticker = filing_metadata.get("ticker", "unknown")
    filing_type = filing_metadata.get("filing_type", "unknown")
    company_name = filing_metadata.get("company_name", "unknown") 
    cik = filing_metadata.get("cik", "unknown")
    filing_date = filing_metadata.get("filing_date", "unknown")
    period_end = filing_metadata.get("period_end_date", "unknown")
    
    # Clean special characters in company name
    company_name, _ = normalize_special_chars(company_name)
    
    # Document identification
    output.append(f"@DOCUMENT: {ticker.strip()}-{filing_type.strip()}-{period_end.strip()}")
    output.append(f"@FILING_DATE: {filing_date.strip()}")
    output.append(f"@COMPANY: {company_name.strip()}")
    output.append(f"@CIK: {cik.strip()}")
    
    # Add processing metadata for provenance tracking
    import datetime
    import pkg_resources
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get version information - fall back to reasonable defaults if not available
    try:
        version = pkg_resources.get_distribution("llm-financial-data").version
    except pkg_resources.DistributionNotFound:
        version = "0.1.0"  # Default version if not installed as package
    
    output.append("")
    output.append("@PROCESSING_METADATA")
    output.append(f"@PROCESSED_AT: {current_time}")
    output.append(f"@PROCESSOR_VERSION: {version}")
    output.append(f"@FORMAT_VERSION: 1.0")
    output.append(f"@SOURCE: SEC EDGAR XBRL")
    
    # Get the original filing URL if available
    instance_url = filing_metadata.get("instance_url", "")
    if instance_url:
        output.append(f"@SOURCE_URL: {instance_url}")
    
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
        
        # Clean special characters in value
        value, chars_changed = normalize_special_chars(value)
            
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
    """Save LLM format to a file using fiscal year naming"""
    ticker = filing_metadata.get("ticker", "unknown")
    if ticker is None:
        ticker = "unknown"
        
    filing_type = filing_metadata.get("filing_type", "unknown")
    if filing_type is None:
        filing_type = "unknown"
        
    period_end = filing_metadata.get("period_end_date", "unknown")
    if period_end is None:
        period_end = "unknown"
        
    company_name = filing_metadata.get("company_name", "unknown")
    if company_name is None:
        company_name = f"Company_{ticker}"
    
    # Clean company name for filename
    clean_company = re.sub(r'[^\w\s]', '', company_name)  # Remove punctuation
    clean_company = re.sub(r'\s+', '_', clean_company.strip())  # Replace spaces with underscores
    if not clean_company:
        clean_company = "Unknown_Company"
    
    # Extract year from period end date (safely)
    try:
        if '-' in period_end:
            year = period_end.split('-')[0]
        elif len(period_end) >= 4:
            year = period_end[:4]
        else:
            year = datetime.datetime.now().strftime("%Y")
    except:
        year = datetime.datetime.now().strftime("%Y")
    
    # Create fiscal year based filename
    # First check if fiscal year and quarter are provided by batch_download.py
    if "fiscal_year" in filing_metadata and "fiscal_quarter" in filing_metadata:
        fiscal_year = filing_metadata.get("fiscal_year")
        fiscal_quarter = filing_metadata.get("fiscal_quarter")
        
        # Use provided fiscal information
        if filing_type == "10-K":
            fiscal_suffix = f"{fiscal_year}_FY"
        else:
            fiscal_suffix = f"{fiscal_year}_{fiscal_quarter}"
            
        logging.info(f"Using provided fiscal information: {fiscal_suffix}")
    else:
        # Fallback: Use traditional method to determine fiscal year/quarter
        if filing_type == "10-K":
            # For annual reports, use FY designation
            fiscal_suffix = f"{year}_FY"
        else:
            # For quarterly reports, use 1Q, 2Q, 3Q, 4Q format
            quarter_num = ""
            
            # Extract quarter mention from filing text if available
            filing_text = filing_metadata.get('filing_text', '')
            if filing_text and isinstance(filing_text, str):
                text = filing_text.lower()
                
                # Look for explicit quarter statements in the filing
                # These patterns are common in SEC filings to explicitly state which quarter is being reported
                quarter_patterns = [
                    # Pattern for "For the [ordinal] quarter ended [date]"
                    r"for\s+the\s+(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter\s+ended",
                    # Pattern for "Quarter [number]" or "Q[number]"
                    r"quarter\s+(one|two|three|four|1|2|3|4)|q(1|2|3|4)\s+",
                    # Pattern for "[ordinal] quarter of fiscal year"
                    r"(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter\s+of\s+fiscal\s+year",
                    # Pattern for "Form 10-Q for Q[number]"
                    r"form\s+10-q\s+for\s+q(1|2|3|4)",
                    # Pattern for explicit quarter mentions
                    r"\bfirst\s+quarter\b|\b1st\s+quarter\b|\bq1\b",
                    r"\bsecond\s+quarter\b|\b2nd\s+quarter\b|\bq2\b",
                    r"\bthird\s+quarter\b|\b3rd\s+quarter\b|\bq3\b",
                    r"\bfourth\s+quarter\b|\b4th\s+quarter\b|\bq4\b"
                ]
                
                # Search for quarter patterns
                for pattern in quarter_patterns:
                    match = re.search(pattern, text)
                    if match:
                        # Extract the quarter number
                        quarter_text = match.group(0).lower()
                        if "first" in quarter_text or "1st" in quarter_text or "one" in quarter_text or "q1" in quarter_text or "quarter 1" in quarter_text:
                            quarter_num = "1Q"
                            break
                        elif "second" in quarter_text or "2nd" in quarter_text or "two" in quarter_text or "q2" in quarter_text or "quarter 2" in quarter_text:
                            quarter_num = "2Q"
                            break
                        elif "third" in quarter_text or "3rd" in quarter_text or "three" in quarter_text or "q3" in quarter_text or "quarter 3" in quarter_text:
                            quarter_num = "3Q"
                            break
                        elif "fourth" in quarter_text or "4th" in quarter_text or "four" in quarter_text or "q4" in quarter_text or "quarter 4" in quarter_text:
                            quarter_num = "4Q"
                            break
            
            # If not found in text, use the period end date to determine quarter
            if not quarter_num and period_end and '-' in period_end:
                try:
                    month = int(period_end.split('-')[1])
                    # Rough quarter mapping (this is a fallback, not always accurate due to fiscal calendars)
                    quarter_map = {1: "1Q", 2: "1Q", 3: "1Q", 4: "2Q", 5: "2Q", 6: "2Q", 
                                  7: "3Q", 8: "3Q", 9: "3Q", 10: "4Q", 11: "4Q", 12: "4Q"}
                    quarter_num = quarter_map.get(month, "")
                except:
                    # Default to empty if we can't parse the month
                    pass
            
            # If we still don't have a quarter number, check the instance URL
            instance_url = filing_metadata.get("instance_url", "")
            if not quarter_num and instance_url and isinstance(instance_url, str):
                # Look for patterns like q1, q2, q3, q4 in the URL
                instance_url_lower = instance_url.lower()
                if "q1" in instance_url_lower or "-1q" in instance_url_lower:
                    quarter_num = "1Q"
                elif "q2" in instance_url_lower or "-2q" in instance_url_lower:
                    quarter_num = "2Q"
                elif "q3" in instance_url_lower or "-3q" in instance_url_lower:
                    quarter_num = "3Q"
                elif "q4" in instance_url_lower or "-4q" in instance_url_lower:
                    quarter_num = "4Q"
            
            # If we still couldn't determine quarter, default to plain Q
            if not quarter_num:
                quarter_num = "Q"
                
            fiscal_suffix = f"{year}_{quarter_num}"
    
    # Include original period end date as reference (for debugging and verification)
    try:
        period_end_compact = period_end.replace("-", "") if period_end and period_end != "unknown" else "unknown"
    except:
        period_end_compact = "unknown"
    
    # Create directory
    dir_path = os.path.join(PROCESSED_DATA_DIR, ticker)
    os.makedirs(dir_path, exist_ok=True)
    
    # Create filename with both naming schemes
    # Primary: Company_Year_FiscalPeriod
    # Secondary: Original format (ticker_filing-type_date) for reference
    filename = f"{clean_company}_{fiscal_suffix}_{ticker}_{filing_type}_{period_end_compact}_llm.txt"
    file_path = os.path.join(dir_path, filename)
    
    # Save file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(llm_content)
    
    return {
        "success": True,
        "file_path": file_path,
        "size": len(llm_content)
    }