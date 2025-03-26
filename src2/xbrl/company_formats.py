"""
Company-specific XBRL format tracking and adaptation module.

This module maintains a registry of company-specific format variations and 
provides mechanisms to dynamically adapt to different filing formats.
"""

import os
import sys
import json
import logging
from pathlib import Path

# Use a path relative to this file for the registry
FORMATS_REGISTRY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 
    "company_formats_registry.json"
)

# Default format handlers for common cases
FORMAT_HANDLERS = {
    "standard": {
        "description": "Standard XBRL format with contexts and facts",
        "contexts_xpath": "//*[local-name()='context']",
        "facts_requires_context": True
    },
    "cal_xml": {
        "description": "Apple's calendar XML format with different structure",
        "contexts_xpath": "//*[contains(local-name(), 'Period') or contains(local-name(), 'Report')]",
        "facts_requires_context": False
    },
    "htm_xml": {
        "description": "HTML-derived XML with simplified structure",
        "contexts_xpath": "//*[local-name()='context']",
        "facts_requires_context": True
    },
    "filing_summary": {
        "description": "SEC Filing Summary XML format",
        "contexts_xpath": None,  # No standard contexts in this format
        "facts_requires_context": False
    },
    "ixbrl": {
        "description": "Inline XBRL format embedded in HTML documents",
        "contexts_xpath": "//*[local-name()='context']",
        "facts_requires_context": True,
        "requires_html_parsing": True
    },
    "ixbrl_viewer": {
        "description": "Inline XBRL format displayed in SEC iXBRL viewer",
        "contexts_xpath": "//*[local-name()='context']",
        "facts_requires_context": True,
        "requires_html_parsing": True,
        "viewer_format": True
    }
}

# Company-specific format overrides
COMPANY_FORMATS = {
    "AAPL": {
        "default_format": "cal_xml",
        "format_patterns": {
            ".cal.xml": "cal_xml",
            "_htm.xml": "htm_xml"
        }
    },
    "MSFT": {
        "default_format": "standard",
        "format_patterns": {
            "_htm.xml": "htm_xml"
        }
    }
}

def load_company_formats():
    """Load company formats from the registry file, or create if not exists"""
    try:
        if os.path.exists(FORMATS_REGISTRY_PATH):
            with open(FORMATS_REGISTRY_PATH, 'r') as f:
                return json.load(f)
        else:
            # Create initial registry with default values
            with open(FORMATS_REGISTRY_PATH, 'w') as f:
                json.dump(COMPANY_FORMATS, f, indent=2)
            return COMPANY_FORMATS
    except Exception as e:
        logging.warning(f"Error loading company formats registry: {str(e)}")
        return COMPANY_FORMATS

def save_company_formats(formats_dict):
    """Save company formats to the registry file"""
    try:
        with open(FORMATS_REGISTRY_PATH, 'w') as f:
            json.dump(formats_dict, f, indent=2)
        return True
    except Exception as e:
        logging.warning(f"Error saving company formats registry: {str(e)}")
        return False

def detect_xbrl_format(file_path, ticker=None):
    """
    Detect the XBRL format type based on file path and company info
    
    Args:
        file_path: Path to the XBRL file
        ticker: Company ticker symbol (optional)
        
    Returns:
        Format type string
    """
    # Default format
    format_type = "standard"
    
    # Check extension and filename patterns first
    if file_path.endswith('.cal.xml'):
        format_type = "cal_xml"
    elif '.htm.xml' in file_path:
        format_type = "htm_xml"
    elif 'FilingSummary.xml' in file_path:
        format_type = "filing_summary"
    
    # If ticker is provided, check company-specific overrides
    if ticker:
        company_formats = load_company_formats()
        
        if ticker in company_formats:
            # Check if any patterns match
            for pattern, pattern_format in company_formats[ticker].get("format_patterns", {}).items():
                if pattern in file_path:
                    format_type = pattern_format
                    break
            
            # If no pattern matched, use company default if available
            if format_type == "standard" and "default_format" in company_formats[ticker]:
                format_type = company_formats[ticker]["default_format"]
    
    return format_type

def get_format_handler(format_type):
    """
    Get the format handler configuration for a specific format type
    
    Args:
        format_type: The format type string
        
    Returns:
        Dictionary with format handler configuration
    """
    return FORMAT_HANDLERS.get(format_type, FORMAT_HANDLERS["standard"])

def register_company_format(ticker, format_info):
    """
    Register or update company-specific format information
    
    Args:
        ticker: Company ticker symbol
        format_info: Dictionary with format information
        
    Returns:
        True if registration succeeded
    """
    company_formats = load_company_formats()
    
    # Update existing entry or add new one
    company_formats[ticker] = format_info
    
    # Save updated registry
    return save_company_formats(company_formats)

def learn_from_successful_parse(ticker, file_path, format_used):
    """
    Update the company format registry based on successful parsing
    
    Args:
        ticker: Company ticker symbol
        file_path: Path to the successfully parsed file
        format_used: Format type that was successfully used
        
    Returns:
        True if learning was successful
    """
    if not ticker:
        return False
    
    try:
        company_formats = load_company_formats()
        
        # Create entry if it doesn't exist
        if ticker not in company_formats:
            company_formats[ticker] = {
                "default_format": "standard",
                "format_patterns": {}
            }
        
        # Identify a pattern from the file path
        path = Path(file_path)
        filename = path.name
        
        # Extract a reasonable pattern from the file path
        pattern_candidates = []
        
        # First try extension
        if '.cal.xml' in filename:
            pattern_candidates.append('.cal.xml')
        elif '.htm.xml' in filename:
            pattern_candidates.append('_htm.xml')
        elif 'FilingSummary.xml' in filename:
            pattern_candidates.append('FilingSummary.xml')
        
        # Use the best pattern we found
        if pattern_candidates:
            pattern = pattern_candidates[0]
            company_formats[ticker]["format_patterns"][pattern] = format_used
            
            # Count how many formats we have for this company
            format_counts = {}
            for _, fmt in company_formats[ticker]["format_patterns"].items():
                format_counts[fmt] = format_counts.get(fmt, 0) + 1
            
            # Update default format to the most common one
            most_common_format = max(format_counts.items(), key=lambda x: x[1])[0]
            company_formats[ticker]["default_format"] = most_common_format
            
            # Save updated registry
            return save_company_formats(company_formats)
    
    except Exception as e:
        logging.warning(f"Error learning company format: {str(e)}")
    
    return False

def get_all_company_formats():
    """
    Get a dictionary of all registered company formats
    
    Returns:
        Dictionary with company ticker as key and format info as value
    """
    return load_company_formats()