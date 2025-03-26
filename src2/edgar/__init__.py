"""
SEC EDGAR Utility Package

This package provides utility functions for interacting with SEC EDGAR:
- get_cik_from_ticker: Convert ticker to CIK
- get_company_name_from_cik: Get company name from CIK
"""

from .edgar_utils import get_cik_from_ticker, get_company_name_from_cik, sec_request