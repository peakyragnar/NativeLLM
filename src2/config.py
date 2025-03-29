#!/usr/bin/env python3
"""
Configuration settings for the NativeLLM system

Central configuration for company information, SEC settings, and processing options.
"""

# Company information
COMPANY_NAME = "Exascale Capital"
COMPANY_EMAIL = "info@exascale.capital"

# SEC EDGAR settings
SEC_BASE_URL = "https://www.sec.gov"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
USER_AGENT = f"{COMPANY_NAME} {COMPANY_EMAIL}"  # Formatted user agent for SEC requests

# Output settings
RAW_DATA_DIR = "sec_downloads"
PROCESSED_DATA_DIR = "sec_processed"

# Initial companies to process
INITIAL_COMPANIES = [
    {"ticker": "AAPL", "name": "Apple Inc."},
    {"ticker": "MSFT", "name": "Microsoft Corporation"},
    {"ticker": "GOOGL", "name": "Alphabet Inc."},
    {"ticker": "AMZN", "name": "Amazon.com, Inc."},
    {"ticker": "META", "name": "Meta Platforms, Inc."}
]

# Filing types to process
FILING_TYPES = ["10-K", "10-Q"]

# HTML Optimization Configuration
HTML_OPTIMIZATION = {
    # Level of HTML optimization to apply
    # - "maximum_integrity": Ensures 100% data preservation (especially numeric values)
    # - "balanced": Provides good size reduction with high (but not perfect) data integrity
    # - "maximum_reduction": Maximizes size reduction, may lose some formatting
    "level": "maximum_integrity",
    
    # Safe mode - only remove definitely non-essential attributes
    # These are the HTML attributes that will be removed when using maximum_integrity
    "safe_removable_attributes": ["bgcolor", "color", "font", "face", "class"],
    
    # Structural attributes that should always be preserved
    "preserved_attributes": ["align", "padding", "margin", "width", "height", "border"],
    
    # Minimum reduction percentage required to apply changes
    "min_reduction_threshold": 1.0,  # Only apply changes if at least 1% reduction is achieved
    
    # Enable logging of HTML optimization metrics
    "enable_logging": True
}

# Output format configuration
OUTPUT_FORMAT = {
    # Whether to generate text.txt files (raw extracted text)
    # Set to False to only generate LLM-formatted files (llm.txt)
    "GENERATE_TEXT_FILES": True  # Default to True for backward compatibility
}