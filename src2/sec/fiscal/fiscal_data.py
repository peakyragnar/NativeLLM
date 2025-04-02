#!/usr/bin/env python3
"""
Fiscal Data Contract Module

This module defines the core data contract for fiscal period information
with strict validation to ensure data integrity throughout the system.
"""

import dataclasses
import datetime
import re
from typing import Optional, Dict, Any, List, Union


class FiscalDataError(Exception):
    """Exception raised for fiscal data validation errors"""
    pass


@dataclasses.dataclass
class FiscalPeriodInfo:
    """
    Immutable data contract for fiscal period information with strict validation
    
    This class ensures that fiscal period information is complete and valid
    before it can be used throughout the system, preventing partial information
    from propagating.
    """
    # Required fields
    ticker: str
    period_end_date: str
    fiscal_year: str
    fiscal_period: str
    
    # Optional metadata fields
    filing_type: Optional[str] = None
    source: str = "company_fiscal_registry"
    confidence: float = 1.0
    validation_timestamp: str = dataclasses.field(default_factory=lambda: datetime.datetime.now().isoformat())
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)
    
    def __post_init__(self):
        """Validate all fields on creation"""
        self._validate()
    
    def _validate(self):
        """
        Validate all fields to ensure data integrity
        
        Raises:
            FiscalDataError: If any validation fails
        """
        # Validate ticker (uppercase, non-empty)
        if not self.ticker or not isinstance(self.ticker, str):
            raise FiscalDataError(f"Invalid ticker: {self.ticker}")
        self.ticker = self.ticker.upper()
        
        # Validate period_end_date (YYYY-MM-DD format)
        if not self.period_end_date or not isinstance(self.period_end_date, str):
            raise FiscalDataError(f"Invalid period_end_date: {self.period_end_date}")
        
        # Check YYYY-MM-DD format
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        if not date_pattern.match(self.period_end_date):
            raise FiscalDataError(f"period_end_date must be in YYYY-MM-DD format: {self.period_end_date}")
        
        # Validate fiscal_year (non-empty string)
        if not self.fiscal_year or not isinstance(self.fiscal_year, str):
            raise FiscalDataError(f"Invalid fiscal_year: {self.fiscal_year}")
        
        # Validate fiscal_period (must be Q1, Q2, Q3, Q4, or annual)
        if not self.fiscal_period or not isinstance(self.fiscal_period, str):
            raise FiscalDataError(f"Invalid fiscal_period: {self.fiscal_period}")
        
        valid_periods = ["Q1", "Q2", "Q3", "Q4", "annual"]
        if self.fiscal_period not in valid_periods:
            raise FiscalDataError(f"fiscal_period must be one of {valid_periods}, got: {self.fiscal_period}")
        
        # Validate filing_type if provided
        if self.filing_type is not None:
            valid_types = ["10-K", "10-Q"]
            if self.filing_type not in valid_types:
                raise FiscalDataError(f"filing_type must be one of {valid_types}, got: {self.filing_type}")
        
        # Validate confidence (between 0.0 and 1.0)
        if not isinstance(self.confidence, (int, float)) or not (0.0 <= self.confidence <= 1.0):
            raise FiscalDataError(f"confidence must be between 0.0 and 1.0, got: {self.confidence}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        return dataclasses.asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FiscalPeriodInfo':
        """Create from dictionary"""
        # Filter out any unknown fields
        known_fields = {f.name for f in dataclasses.fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered_data)


def validate_period_end_date(period_end_date: str) -> str:
    """
    Validate a period end date string and normalize to YYYY-MM-DD format
    
    Args:
        period_end_date: The period end date string in any format
        
    Returns:
        Normalized period end date in YYYY-MM-DD format
        
    Raises:
        FiscalDataError: If the date is invalid or cannot be normalized
    """
    if not period_end_date:
        raise FiscalDataError("Period end date cannot be empty")
    
    # First try to parse as YYYY-MM-DD directly
    if re.match(r'^\d{4}-\d{2}-\d{2}$', period_end_date):
        try:
            # Validate that it's a valid date
            datetime.datetime.strptime(period_end_date, '%Y-%m-%d')
            return period_end_date
        except ValueError as e:
            raise FiscalDataError(f"Invalid date format: {e}")
    
    # Check for YYYYMMDD format (like in filenames nvda-20210502.htm)
    if re.match(r'^\d{8}$', period_end_date):
        try:
            parsed_date = datetime.datetime.strptime(period_end_date, '%Y%m%d')
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError as e:
            # Continue to other formats if this fails
            pass
    
    # Try other common formats
    for fmt in ('%m/%d/%Y', '%Y/%m/%d', '%m-%d-%Y', '%d-%m-%Y', '%B %d, %Y', '%b %d, %Y'):
        try:
            parsed_date = datetime.datetime.strptime(period_end_date, fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If we reach here, all format attempts failed
    raise FiscalDataError(f"Cannot parse period end date: {period_end_date}")