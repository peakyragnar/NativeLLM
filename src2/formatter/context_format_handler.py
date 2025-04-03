#!/usr/bin/env python3
"""
Context Format Handler

This module provides a robust and extensible system for handling different context reference formats
in XBRL filings. It uses a registry of format handlers to extract date information from context IDs.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Callable
from datetime import datetime

# Type definitions
ContextData = Dict[str, Any]
PeriodInfo = Dict[str, str]
FormatHandler = Callable[[str], Optional[PeriodInfo]]

class ContextFormatRegistry:
    """Registry of context format handlers"""
    
    def __init__(self):
        self.format_handlers: List[Tuple[str, str, FormatHandler]] = []
        self.register_default_handlers()
    
    def register_handler(self, name: str, description: str, handler: FormatHandler) -> None:
        """Register a new format handler"""
        self.format_handlers.append((name, description, handler))
        logging.info(f"Registered context format handler: {name}")
    
    def extract_period_info(self, context_ref: str) -> Optional[PeriodInfo]:
        """
        Extract period information from a context reference
        
        Args:
            context_ref: The context reference ID
            
        Returns:
            A dictionary with period information (startDate, endDate or instant),
            or None if no format handler could extract the information
        """
        for name, _, handler in self.format_handlers:
            try:
                period_info = handler(context_ref)
                if period_info:
                    logging.debug(f"Extracted period info from {context_ref} using {name} handler: {period_info}")
                    return period_info
            except Exception as e:
                logging.debug(f"Error in {name} handler for {context_ref}: {str(e)}")
        
        logging.debug(f"No handler could extract period info from {context_ref}")
        return None
    
    def register_default_handlers(self) -> None:
        """Register the default set of format handlers"""
        
        # Format 1: C_0000789019_20200701_20210630 (duration with CIK)
        def handle_c_duration(context_ref: str) -> Optional[PeriodInfo]:
            match = re.search(r'C_\d+_(\d{8})_(\d{8})', context_ref)
            if not match:
                return None
            
            start_date_str = match.group(1)
            end_date_str = match.group(2)
            
            formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
            formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
            
            return {
                "startDate": formatted_start,
                "endDate": formatted_end
            }
        
        # Format 2: C_0000789019_20200701 (instant with CIK)
        def handle_c_instant(context_ref: str) -> Optional[PeriodInfo]:
            match = re.search(r'C_\d+_(\d{8})$', context_ref)
            if not match:
                return None
            
            date_str = match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            return {
                "instant": formatted_date
            }
        
        # Format 3: _D20200701-20210630 (standard duration)
        def handle_d_duration(context_ref: str) -> Optional[PeriodInfo]:
            match = re.search(r'_D(\d{8})-(\d{8})', context_ref)
            if not match:
                return None
            
            start_date_str = match.group(1)
            end_date_str = match.group(2)
            
            formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
            formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
            
            return {
                "startDate": formatted_start,
                "endDate": formatted_end
            }
        
        # Format 4: _I20200701 (standard instant)
        def handle_i_instant(context_ref: str) -> Optional[PeriodInfo]:
            match = re.search(r'_I(\d{8})', context_ref)
            if not match:
                return None
            
            date_str = match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            return {
                "instant": formatted_date
            }
        
        # Format 5: NVDA format with embedded dates (e.g., i2c5e111a942340e08ad1e8d2e3b0fb71_D20210201-20220130)
        def handle_nvda_duration(context_ref: str) -> Optional[PeriodInfo]:
            match = re.search(r'i[a-z0-9]+_D(\d{8})-(\d{8})', context_ref)
            if not match:
                return None
            
            start_date_str = match.group(1)
            end_date_str = match.group(2)
            
            formatted_start = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
            formatted_end = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
            
            return {
                "startDate": formatted_start,
                "endDate": formatted_end
            }
        
        # Format 6: NVDA format with embedded instant date (e.g., i2c5e111a942340e08ad1e8d2e3b0fb71_I20210201)
        def handle_nvda_instant(context_ref: str) -> Optional[PeriodInfo]:
            match = re.search(r'i[a-z0-9]+_I(\d{8})', context_ref)
            if not match:
                return None
            
            date_str = match.group(1)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            return {
                "instant": formatted_date
            }
        
        # Format 7: MSFT format with embedded dates in context ID (e.g., FD2022Q3YTD_us-gaap_StatementOfCashFlowsAbstract)
        def handle_msft_fiscal_duration(context_ref: str) -> Optional[PeriodInfo]:
            # This is a placeholder for MSFT fiscal period format
            # We would need to map fiscal periods to actual dates
            match = re.search(r'FD(\d{4})(Q\d|)(YTD|)', context_ref)
            if not match:
                return None
            
            # This would require a lookup table to map fiscal periods to actual dates
            # For now, we'll just return None
            return None
        
        # Register all the handlers
        self.register_handler("C_Duration", "Duration with CIK (C_0000789019_20200701_20210630)", handle_c_duration)
        self.register_handler("C_Instant", "Instant with CIK (C_0000789019_20200701)", handle_c_instant)
        self.register_handler("D_Duration", "Standard duration (_D20200701-20210630)", handle_d_duration)
        self.register_handler("I_Instant", "Standard instant (_I20200701)", handle_i_instant)
        self.register_handler("NVDA_Duration", "NVDA duration (i2c5e111a942340e08ad1e8d2e3b0fb71_D20210201-20220130)", handle_nvda_duration)
        self.register_handler("NVDA_Instant", "NVDA instant (i2c5e111a942340e08ad1e8d2e3b0fb71_I20210201)", handle_nvda_instant)
        self.register_handler("MSFT_Fiscal", "MSFT fiscal period (FD2022Q3YTD)", handle_msft_fiscal_duration)

# Create a singleton instance
context_registry = ContextFormatRegistry()

def extract_period_info(context_ref: str) -> Optional[PeriodInfo]:
    """
    Extract period information from a context reference
    
    This is a convenience function that uses the singleton registry
    
    Args:
        context_ref: The context reference ID
        
    Returns:
        A dictionary with period information (startDate, endDate or instant),
        or None if no format handler could extract the information
    """
    return context_registry.extract_period_info(context_ref)

def register_format_handler(name: str, description: str, handler: FormatHandler) -> None:
    """
    Register a new format handler
    
    This is a convenience function that uses the singleton registry
    
    Args:
        name: A unique name for the handler
        description: A description of the format
        handler: A function that takes a context_ref and returns period info
    """
    context_registry.register_handler(name, description, handler)
