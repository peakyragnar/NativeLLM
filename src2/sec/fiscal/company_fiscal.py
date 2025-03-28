#!/usr/bin/env python3
"""
Explicit Fiscal Calendar Registry

This module uses explicit period end date mappings for maximum reliability.
Each company has a complete set of period end dates mapped to specific fiscal periods.
"""

import os
import json
import logging
import datetime
from pathlib import Path

class CompanyFiscalCalendar:
    """
    Company fiscal calendar using explicit period end date mappings
    """
    
    def __init__(self, ticker, fiscal_data):
        """
        Initialize with complete fiscal mapping data
        
        Args:
            ticker (str): Company ticker symbol
            fiscal_data (dict): Complete fiscal data including period_end_dates mapping
        """
        self.ticker = ticker.upper()
        self.period_end_dates = fiscal_data.get("period_end_dates", {})
        
    def to_dict(self):
        """Convert to dictionary for storage"""
        return {
            "ticker": self.ticker,
            "period_end_dates": self.period_end_dates
        }
        
    @classmethod
    def from_dict(cls, data):
        """Create from dictionary"""
        return cls(
            ticker=data["ticker"],
            fiscal_data={"period_end_dates": data.get("period_end_dates", {})}
        )
        
    def determine_fiscal_period(self, period_end_date, filing_type=None):
        """
        Determine fiscal year and period based on exact period end date
        
        Args:
            period_end_date (str): Period end date in YYYY-MM-DD format
            filing_type (str, optional): Type of filing (not used in explicit mapping)
            
        Returns:
            dict: Fiscal information with fiscal_year and fiscal_period
        """
        from .fiscal_data import validate_period_end_date, FiscalDataError
        
        if not period_end_date:
            return {"fiscal_year": None, "fiscal_period": None, "error": "Period end date is empty"}
        
        # Validate and normalize the period end date first
        try:
            validated_date = validate_period_end_date(period_end_date)
        except FiscalDataError as e:
            return {"fiscal_year": None, "fiscal_period": None, "error": str(e)}
        
        # Now that we have a validated date, look it up in our mappings
        period_data = self.period_end_dates.get(validated_date)
        
        if period_data:
            return {
                "fiscal_year": period_data.get("fiscal_year"),
                "fiscal_period": period_data.get("fiscal_period"),
                "validated_date": validated_date,
                "validated": True
            }
        else:
            error_msg = f"No mapping found for period end date: {validated_date}"
            logging.error(error_msg)
            return {
                "fiscal_year": None,
                "fiscal_period": None,
                "error": error_msg,
                "validated_date": validated_date,
                "validated": True
            }


class FiscalCalendarRegistry:
    """
    Registry for company fiscal calendars using explicit period end date mappings
    """
    
    # Pre-defined company fiscal calendars with explicit mappings
    COMPANY_CALENDARS = {
        # NVIDIA (NVDA) - Full fiscal years 2021-2025
        "NVDA": {
            "period_end_dates": {
                # Fiscal Year 2021 (Feb 2020 - Jan 2021)
                "2020-04-26": {"fiscal_year": "2021", "fiscal_period": "Q1"},
                "2020-07-26": {"fiscal_year": "2021", "fiscal_period": "Q2"},
                "2020-10-25": {"fiscal_year": "2021", "fiscal_period": "Q3"},
                "2021-01-31": {"fiscal_year": "2021", "fiscal_period": "annual"},
                
                # Fiscal Year 2022 (Feb 2021 - Jan 2022)
                "2021-04-25": {"fiscal_year": "2022", "fiscal_period": "Q1"},
                "2021-05-02": {"fiscal_year": "2022", "fiscal_period": "Q1"}, # Alternative date format
                "2021-07-25": {"fiscal_year": "2022", "fiscal_period": "Q2"},
                "2021-08-01": {"fiscal_year": "2022", "fiscal_period": "Q2"}, # Alternative date format
                "2021-10-31": {"fiscal_year": "2022", "fiscal_period": "Q3"},
                "2022-01-30": {"fiscal_year": "2022", "fiscal_period": "annual"},
                
                # Fiscal Year 2023 (Feb 2022 - Jan 2023)
                "2022-05-01": {"fiscal_year": "2023", "fiscal_period": "Q1"},
                "2022-07-31": {"fiscal_year": "2023", "fiscal_period": "Q2"},
                "2022-10-30": {"fiscal_year": "2023", "fiscal_period": "Q3"},
                "2023-01-29": {"fiscal_year": "2023", "fiscal_period": "annual"},
                
                # Fiscal Year 2024 (Feb 2023 - Jan 2024)
                "2023-04-30": {"fiscal_year": "2024", "fiscal_period": "Q1"},
                "2023-07-30": {"fiscal_year": "2024", "fiscal_period": "Q2"},
                "2023-10-29": {"fiscal_year": "2024", "fiscal_period": "Q3"},
                "2024-01-28": {"fiscal_year": "2024", "fiscal_period": "annual"},
                
                # Fiscal Year 2025 (Feb 2024 - Jan 2025)
                "2024-04-28": {"fiscal_year": "2025", "fiscal_period": "Q1"},
                "2024-07-28": {"fiscal_year": "2025", "fiscal_period": "Q2"},
                "2024-10-27": {"fiscal_year": "2025", "fiscal_period": "Q3"},
                "2025-01-26": {"fiscal_year": "2025", "fiscal_period": "annual"}
            }
        },
        
        # Microsoft (MSFT) - Full fiscal years 2021-2025
        "MSFT": {
            "period_end_dates": {
                # Fiscal Year 2021 (Jul 2020 - Jun 2021)
                "2020-09-30": {"fiscal_year": "2021", "fiscal_period": "Q1"},
                "2020-12-31": {"fiscal_year": "2021", "fiscal_period": "Q2"},
                "2021-03-31": {"fiscal_year": "2021", "fiscal_period": "Q3"},
                "2021-06-30": {"fiscal_year": "2021", "fiscal_period": "annual"},
                
                # Fiscal Year 2022 (Jul 2021 - Jun 2022)
                "2021-09-30": {"fiscal_year": "2022", "fiscal_period": "Q1"},
                "2021-12-31": {"fiscal_year": "2022", "fiscal_period": "Q2"},
                "2022-03-31": {"fiscal_year": "2022", "fiscal_period": "Q3"},
                "2022-06-30": {"fiscal_year": "2022", "fiscal_period": "annual"},
                
                # Fiscal Year 2023 (Jul 2022 - Jun 2023)
                "2022-09-30": {"fiscal_year": "2023", "fiscal_period": "Q1"},
                "2022-12-31": {"fiscal_year": "2023", "fiscal_period": "Q2"},
                "2023-03-31": {"fiscal_year": "2023", "fiscal_period": "Q3"},
                "2023-06-30": {"fiscal_year": "2023", "fiscal_period": "annual"},
                
                # Fiscal Year 2024 (Jul 2023 - Jun 2024)
                "2023-09-30": {"fiscal_year": "2024", "fiscal_period": "Q1"},
                "2023-12-31": {"fiscal_year": "2024", "fiscal_period": "Q2"},
                "2024-03-31": {"fiscal_year": "2024", "fiscal_period": "Q3"},
                "2024-06-30": {"fiscal_year": "2024", "fiscal_period": "annual"},
                
                # Fiscal Year 2025 (Jul 2024 - Jun 2025)
                "2024-09-30": {"fiscal_year": "2025", "fiscal_period": "Q1"},
                "2024-12-31": {"fiscal_year": "2025", "fiscal_period": "Q2"},
                "2025-03-31": {"fiscal_year": "2025", "fiscal_period": "Q3"},
                "2025-06-30": {"fiscal_year": "2025", "fiscal_period": "annual"}
            }
        },
        
        # Apple (AAPL) - Full fiscal years 2021-2025
        "AAPL": {
            "period_end_dates": {
                # Fiscal Year 2021 (Oct 2020 - Sep 2021)
                "2020-12-26": {"fiscal_year": "2021", "fiscal_period": "Q1"},
                "2021-03-27": {"fiscal_year": "2021", "fiscal_period": "Q2"},
                "2021-06-26": {"fiscal_year": "2021", "fiscal_period": "Q3"},
                "2021-09-25": {"fiscal_year": "2021", "fiscal_period": "annual"},
                
                # Fiscal Year 2022 (Oct 2021 - Sep 2022)
                "2021-12-25": {"fiscal_year": "2022", "fiscal_period": "Q1"},
                "2022-03-26": {"fiscal_year": "2022", "fiscal_period": "Q2"},
                "2022-06-25": {"fiscal_year": "2022", "fiscal_period": "Q3"},
                "2022-09-24": {"fiscal_year": "2022", "fiscal_period": "annual"},
                
                # Fiscal Year 2023 (Oct 2022 - Sep 2023)
                "2022-12-31": {"fiscal_year": "2023", "fiscal_period": "Q1"},
                "2023-04-01": {"fiscal_year": "2023", "fiscal_period": "Q2"},
                "2023-07-01": {"fiscal_year": "2023", "fiscal_period": "Q3"},
                "2023-09-30": {"fiscal_year": "2023", "fiscal_period": "annual"},
                
                # Fiscal Year 2024 (Oct 2023 - Sep 2024)
                "2023-12-30": {"fiscal_year": "2024", "fiscal_period": "Q1"},
                "2024-03-30": {"fiscal_year": "2024", "fiscal_period": "Q2"},
                "2024-06-29": {"fiscal_year": "2024", "fiscal_period": "Q3"},
                "2024-09-28": {"fiscal_year": "2024", "fiscal_period": "annual"},
                
                # Fiscal Year 2025 (Oct 2024 - Sep 2025)
                "2024-12-28": {"fiscal_year": "2025", "fiscal_period": "Q1"},
                "2025-03-29": {"fiscal_year": "2025", "fiscal_period": "Q2"},
                "2025-06-28": {"fiscal_year": "2025", "fiscal_period": "Q3"},
                "2025-09-27": {"fiscal_year": "2025", "fiscal_period": "annual"}
            }
        },
        
        # Google/Alphabet (GOOGL) - Full fiscal years 2022-2025 (Calendar year)
        "GOOGL": {
            "period_end_dates": {
                # Fiscal Year 2022 (Jan 2022 - Dec 2022)
                "2022-03-31": {"fiscal_year": "2022", "fiscal_period": "Q1"},
                "2022-06-30": {"fiscal_year": "2022", "fiscal_period": "Q2"},
                "2022-09-30": {"fiscal_year": "2022", "fiscal_period": "Q3"},
                "2022-12-31": {"fiscal_year": "2022", "fiscal_period": "annual"},
                
                # Fiscal Year 2023 (Jan 2023 - Dec 2023)
                "2023-03-31": {"fiscal_year": "2023", "fiscal_period": "Q1"},
                "2023-06-30": {"fiscal_year": "2023", "fiscal_period": "Q2"},
                "2023-09-30": {"fiscal_year": "2023", "fiscal_period": "Q3"},
                "2023-12-31": {"fiscal_year": "2023", "fiscal_period": "annual"},
                
                # Fiscal Year 2024 (Jan 2024 - Dec 2024)
                "2024-03-31": {"fiscal_year": "2024", "fiscal_period": "Q1"},
                "2024-06-30": {"fiscal_year": "2024", "fiscal_period": "Q2"},
                "2024-09-30": {"fiscal_year": "2024", "fiscal_period": "Q3"},
                "2024-12-31": {"fiscal_year": "2024", "fiscal_period": "annual"},
                
                # Fiscal Year 2025 (Jan 2025 - Dec 2025)
                "2025-03-31": {"fiscal_year": "2025", "fiscal_period": "Q1"},
                "2025-06-30": {"fiscal_year": "2025", "fiscal_period": "Q2"},
                "2025-09-30": {"fiscal_year": "2025", "fiscal_period": "Q3"},
                "2025-12-31": {"fiscal_year": "2025", "fiscal_period": "annual"}
            }
        }
    }
    
    def __init__(self, registry_path=None):
        """
        Initialize the registry
        
        Args:
            registry_path (str, optional): Path to registry JSON file
        """
        self.registry = {}
        
        # Set default registry path if not provided
        if not registry_path:
            module_dir = Path(os.path.dirname(os.path.abspath(__file__)))
            self.registry_path = module_dir / "fiscal_calendars.json"
        else:
            self.registry_path = Path(registry_path)
            
        # Load the registry
        self._load_registry()
        
    def _load_registry(self):
        """Load registry from pre-defined calendars and JSON file"""
        # First load pre-defined calendars
        for ticker, calendar_data in self.COMPANY_CALENDARS.items():
            self.registry[ticker] = CompanyFiscalCalendar(
                ticker=ticker,
                fiscal_data=calendar_data
            )
            
        # Then load from JSON file if it exists
        if os.path.exists(self.registry_path):
            try:
                with open(self.registry_path, 'r') as f:
                    registry_data = json.load(f)
                    
                for ticker, data in registry_data.items():
                    # Only add if not already in registry
                    if ticker not in self.registry:
                        self.registry[ticker] = CompanyFiscalCalendar.from_dict(data)
                        
                logging.info(f"Loaded fiscal calendars for {len(self.registry)} companies")
            except Exception as e:
                logging.error(f"Error loading fiscal registry: {str(e)}")
                
    def save_registry(self):
        """Save registry to JSON file"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.registry_path), exist_ok=True)
            
            # Convert all calendars to dictionaries 
            registry_data = {
                ticker: calendar.to_dict()
                for ticker, calendar in self.registry.items()
            }
            
            # Save to file
            with open(self.registry_path, 'w') as f:
                json.dump(registry_data, f, indent=2)
                
            logging.info(f"Saved fiscal registry with {len(self.registry)} companies")
            return True
        except Exception as e:
            logging.error(f"Error saving fiscal registry: {str(e)}")
            return False
            
    def get_calendar(self, ticker):
        """
        Get fiscal calendar for a company
        
        Args:
            ticker (str): Company ticker symbol
            
        Returns:
            CompanyFiscalCalendar or None: The company's fiscal calendar
        """
        ticker = ticker.upper()
        return self.registry.get(ticker)
    
    def add_period_end_date(self, ticker, period_end_date, fiscal_year, fiscal_period):
        """
        Add a new period end date mapping for a company
        
        Args:
            ticker (str): Company ticker symbol
            period_end_date (str): Period end date in YYYY-MM-DD format
            fiscal_year (str): Fiscal year (e.g., "2023")
            fiscal_period (str): Fiscal period (e.g., "Q1", "Q2", "Q3", "annual")
            
        Returns:
            bool: True if successful
        """
        ticker = ticker.upper()
        
        # Get the company's calendar
        calendar = self.get_calendar(ticker)
        
        # If company doesn't exist, create it
        if not calendar:
            calendar = CompanyFiscalCalendar(
                ticker=ticker,
                fiscal_data={"period_end_dates": {}}
            )
            self.registry[ticker] = calendar
        
        # Add the period end date mapping
        calendar.period_end_dates[period_end_date] = {
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period
        }
        
        # Save the registry
        return self.save_registry()
            
    def add_calendar(self, ticker, period_end_dates):
        """
        Add a new company fiscal calendar
        
        Args:
            ticker (str): Company ticker symbol
            period_end_dates (dict): Mapping from period end dates to fiscal info
            
        Returns:
            CompanyFiscalCalendar: The added calendar
        """
        ticker = ticker.upper()
        
        calendar = CompanyFiscalCalendar(
            ticker=ticker,
            fiscal_data={"period_end_dates": period_end_dates}
        )
        
        self.registry[ticker] = calendar
        self.save_registry()
        return calendar
        
    def determine_fiscal_period(self, ticker, period_end_date, filing_type=None):
        """
        Determine fiscal year and period for a company and specific period end date
        
        This is the SINGLE SOURCE OF TRUTH for fiscal period determination in the system.
        All components should use this method to ensure consistency.
        
        Args:
            ticker (str): Company ticker symbol
            period_end_date (str): Period end date in any format
            filing_type (str, optional): Type of filing
            
        Returns:
            dict: Fiscal information with comprehensive metadata
        """
        from .fiscal_data import FiscalPeriodInfo, FiscalDataError, validate_period_end_date
        
        ticker = ticker.upper()
        
        # Create a complete audit trail in the result
        result = {
            "fiscal_year": None,
            "fiscal_period": None,
            "ticker": ticker,
            "raw_period_end_date": period_end_date,
            "filing_type": filing_type,
            "determination_timestamp": datetime.datetime.now().isoformat(),
        }
        
        # Step 1: Validate the period_end_date first to ensure clean data
        try:
            normalized_date = validate_period_end_date(period_end_date)
            result["validated_date"] = normalized_date
            result["date_validated"] = True
        except FiscalDataError as e:
            error_msg = f"Invalid period_end_date: {e}"
            logging.error(error_msg)
            result["error"] = error_msg
            result["date_validated"] = False
            return result
        
        # Step 2: Get the company's fiscal calendar
        calendar = self.get_calendar(ticker)
        
        if not calendar:
            error_msg = f"No fiscal calendar found for {ticker}. Please add this company to the fiscal registry."
            logging.error(error_msg)
            result["error"] = error_msg
            return result
        
        # Step 3: Get the fiscal period information from the company calendar    
        calendar_result = calendar.determine_fiscal_period(normalized_date, filing_type)
        
        # Extract fiscal information
        fiscal_year = calendar_result.get("fiscal_year")
        fiscal_period = calendar_result.get("fiscal_period")
        
        # If either is missing, return error result
        if not fiscal_year or not fiscal_period:
            error_msg = calendar_result.get("error", "Unknown error determining fiscal period")
            logging.error(error_msg)
            result["error"] = error_msg
            return result
        
        # Step 4: Create a validated FiscalPeriodInfo object to ensure data integrity
        try:
            fiscal_info = FiscalPeriodInfo(
                ticker=ticker,
                period_end_date=normalized_date,
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                filing_type=filing_type,
                source="company_fiscal_registry",
                confidence=1.0,  # Highest confidence for explicit mappings
                metadata={
                    "origin": "company_fiscal_registry",
                    "validated": True,
                    "registry_lookup": True
                }
            )
            
            # Convert to dictionary and add additional metadata
            result = fiscal_info.to_dict()
            result["source"] = "company_fiscal_registry"
            result["validated"] = True
            
            # Log successful determination
            logging.info(f"Fiscal period determined for {ticker}, {normalized_date}: {fiscal_year} {fiscal_period}")
            
            return result
            
        except FiscalDataError as e:
            error_msg = f"Data contract validation failed: {e}"
            logging.error(error_msg)
            result["error"] = error_msg
            result["fiscal_year"] = fiscal_year
            result["fiscal_period"] = fiscal_period
            return result

# Initialize global registry
fiscal_registry = FiscalCalendarRegistry()