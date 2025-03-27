#!/usr/bin/env python3
"""
Value Normalization Module

Provides functions to normalize values for consistent formatting without
changing their actual meaning or precision. Handles numeric values, special
characters, and currency formatting.
"""

import re

def normalize_special_chars(text):
    """
    Replace non-standard Unicode characters with ASCII equivalents when they don't change meaning.
    
    This function maintains a mapping of common special characters found in SEC filings
    and replaces them with standard ASCII equivalents without changing semantics.
    
    Returns the cleaned text and whether it was changed.
    """
    if not text or not isinstance(text, str):
        return text, False
    
    original = text
    
    # Map of special characters to their ASCII equivalents
    char_map = {
        # Whitespace characters
        '\u00A0': ' ',  # Non-breaking space
        '\u2007': ' ',  # Figure space
        '\u202F': ' ',  # Narrow non-breaking space
        
        # Quotes and apostrophes
        '\u2018': "'",  # Left single quote
        '\u2019': "'",  # Right single quote
        '\u201C': '"',  # Left double quote
        '\u201D': '"',  # Right double quote
        
        # Dashes and hyphens
        '\u2013': '-',  # En dash
        '\u2014': '--',  # Em dash
        '\u2212': '-',  # Minus sign
        
        # Other punctuation
        '\u2022': '*',  # Bullet
        '\u2023': '>',  # Triangular bullet
        '\u2043': '-',  # Hyphen bullet
        '\u25E6': 'o',  # White bullet
        '\u2043': '-',  # Hyphen bullet
        '\u25AA': '*',  # Black small square
        '\u25AB': '*',  # White small square
        '\u00B7': '*',  # Middle dot
        
        # Currency symbols (only if encoding is an issue)
        '\u20AC': 'EUR',  # Euro
        '\u00A3': 'GBP',  # Pound
        '\u00A5': 'JPY',  # Yen
        
        # Common symbols
        '\u00AE': '(R)',  # Registered trademark
        '\u2122': '(TM)',  # Trademark
        '\u00A9': '(C)',  # Copyright
        
        # Fractions
        '\u00BC': '1/4',  # Quarter
        '\u00BD': '1/2',  # Half
        '\u00BE': '3/4',  # Three quarters
        
        # Math symbols
        '\u00D7': 'x',  # Multiplication
        '\u00F7': '/',  # Division
    }
    
    # Apply replacements
    for char, replacement in char_map.items():
        if char in text:
            text = text.replace(char, replacement)
    
    # Check if any changes were made
    was_changed = (text != original)
    return text, was_changed

def normalize_value(value_str, decimals=None):
    """
    Normalize numeric values for consistent formatting without changing actual values.
    
    Rules:
    1. Convert numeric strings to a standard format
    2. Apply consistent decimal notation based on decimals attribute
    3. Handle scientific notation, commas, and other variations
    4. Preserve non-numeric values exactly as they are
    5. Replace non-standard Unicode characters with ASCII equivalents
    
    Returns the normalized value and whether it was changed
    """
    if not value_str or not isinstance(value_str, str):
        return value_str, False
    
    # First, normalize any special characters
    value_str, chars_changed = normalize_special_chars(value_str)
    
    original = value_str.strip()
    
    # Skip HTML content or clearly non-numeric values
    if '<' in original or '>' in original or len(original) > 100:
        return original, chars_changed
    
    # Check if it looks like a number (including scientific notation, negative, etc.)
    numeric_pattern = r'^[−-]?[\d,]+(\.\d*)?([eE][+-]?\d+)?$'
    if not re.match(numeric_pattern, original):
        # Not a standard numeric format, try special cases
        # Handle parentheses for negative numbers like (123.45)
        if original.startswith('(') and original.endswith(')'):
            inner = original[1:-1].strip()
            if re.match(r'^[\d,]+(\.\d*)?$', inner):
                try:
                    # Convert to standard negative notation
                    parsed_value = -float(inner.replace(',', ''))
                    # Continue with formatting below
                except ValueError:
                    return original, chars_changed
            else:
                return original, chars_changed
        else:
            # Not a number we can safely parse
            return original, chars_changed
    else:
        try:
            # Replace any minus sign variations with standard negative
            normalized = original.replace('−', '-')
            # Remove commas in numbers like 1,234,567
            normalized = normalized.replace(',', '')
            # Parse the numeric value
            parsed_value = float(normalized)
        except ValueError:
            # If we can't parse it safely, return original
            return original, chars_changed
    
    # Now we have a numeric parsed_value, format it consistently
    
    # Handle the decimals attribute (XBRL specific)
    if decimals is not None:
        try:
            # XBRL decimals attribute: 
            # -6 means millions (no decimal places)
            # 2 means 2 decimal places (0.01 precision)
            # INF means exact value (float precision)
            if str(decimals).strip().upper() == 'INF':
                # Format with all available precision
                formatted = str(parsed_value)
            else:
                decimals_value = int(decimals)
                if decimals_value >= 0:
                    # Positive decimals means precision to right of decimal point
                    formatted = f"{parsed_value:.{decimals_value}f}"
                else:
                    # Negative decimals means precision to left of decimal point
                    # (rounded to millions, billions, etc.)
                    # But we want to preserve the full number, not round it
                    formatted = f"{parsed_value:.0f}"
        except (ValueError, TypeError):
            # If decimals attribute isn't valid, format based on value
            formatted = str(parsed_value)
    else:
        # No decimals attribute, preserve the original precision
        # Count decimal places in original if it has a decimal point
        if '.' in original:
            decimal_places = len(original.split('.')[1])
            formatted = f"{parsed_value:.{decimal_places}f}"
        else:
            formatted = f"{parsed_value:.0f}"
    
    # Remove trailing zeros after decimal point, but keep one zero for whole numbers
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.') if '.' in formatted else formatted
    
    # Check if normalization actually changed anything
    was_changed = (formatted != original)
    return formatted, was_changed