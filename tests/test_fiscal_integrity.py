#!/usr/bin/env python3
"""
Test Fiscal Data Integrity System

This script tests the fiscal period determination system to ensure data integrity.
It verifies that all known period end dates for test companies are correctly mapped
to their fiscal periods using the single source of truth.
"""

import sys
import unittest
import logging
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import the fiscal data contract and registry
try:
    from src2.sec.fiscal.fiscal_data import FiscalPeriodInfo, FiscalDataError, validate_period_end_date
    from src2.sec.fiscal.company_fiscal import fiscal_registry
except ImportError as e:
    logging.error(f"Could not import fiscal registry: {e}")
    sys.exit(1)


class TestFiscalIntegrity(unittest.TestCase):
    """Test the fiscal data integrity system"""

    def test_fiscal_validation_failure(self):
        """Test that invalid dates are properly rejected"""
        invalid_dates = [
            "",                  # Empty string
            "not a date",        # Nonsense
            "2024/99/99",        # Invalid month
            "25-25-2023"         # Invalid format
        ]
        
        for date in invalid_dates:
            with self.assertRaises(FiscalDataError):
                validate_period_end_date(date)
    
    def test_fiscal_validation_success(self):
        """Test that valid dates in different formats are properly normalized"""
        test_cases = [
            # (input, expected output)
            ("2024-01-31", "2024-01-31"),      # Already in correct format
            ("01/31/2024", "2024-01-31"),      # MM/DD/YYYY
            ("2024/01/31", "2024-01-31"),      # YYYY/MM/DD
            ("01-31-2024", "2024-01-31"),      # MM-DD-YYYY
            ("January 31, 2024", "2024-01-31") # Long format
        ]
        
        for input_date, expected in test_cases:
            normalized = validate_period_end_date(input_date)
            self.assertEqual(normalized, expected)
    
    def test_nvda_fiscal_periods(self):
        """Test NVDA fiscal period determination"""
        ticker = "NVDA"
        
        # Key test cases covering different fiscal periods
        test_cases = [
            # (period_end_date, expected_fiscal_year, expected_fiscal_period)
            ("2023-04-30", "2024", "Q1"),  # Q1 FY2024
            ("2023-07-30", "2024", "Q2"),  # Q2 FY2024
            ("2023-10-29", "2024", "Q3"),  # Q3 FY2024
            ("2024-01-28", "2024", "annual"),  # Annual FY2024
            
            ("2024-04-28", "2025", "Q1"),  # Q1 FY2025
            ("2024-07-28", "2025", "Q2"),  # Q2 FY2025
            ("2024-10-27", "2025", "Q3"),  # Q3 FY2025
            ("2025-01-26", "2025", "annual"),  # Annual FY2025
        ]
        
        for period_end_date, expected_fiscal_year, expected_fiscal_period in test_cases:
            with self.subTest(f"Testing {ticker} {period_end_date}"):
                # Test with 10-K filing type
                filing_type = "10-K" if expected_fiscal_period == "annual" else "10-Q"
                
                fiscal_info = fiscal_registry.determine_fiscal_period(
                    ticker, period_end_date, filing_type
                )
                
                self.assertEqual(fiscal_info.get("fiscal_year"), expected_fiscal_year)
                self.assertEqual(fiscal_info.get("fiscal_period"), expected_fiscal_period)
                # Our implementation includes validation
                self.assertTrue(fiscal_info.get("validated", False))
    
    def test_msft_fiscal_periods(self):
        """Test MSFT fiscal period determination"""
        ticker = "MSFT"
        
        # Key test cases covering different fiscal periods
        test_cases = [
            # (period_end_date, expected_fiscal_year, expected_fiscal_period)
            ("2023-09-30", "2024", "Q1"),  # Q1 FY2024
            ("2023-12-31", "2024", "Q2"),  # Q2 FY2024
            ("2024-03-31", "2024", "Q3"),  # Q3 FY2024
            ("2024-06-30", "2024", "annual"),  # Annual FY2024
            
            ("2024-09-30", "2025", "Q1"),  # Q1 FY2025
            ("2024-12-31", "2025", "Q2"),  # Q2 FY2025
            ("2025-03-31", "2025", "Q3"),  # Q3 FY2025
            ("2025-06-30", "2025", "annual"),  # Annual FY2025
        ]
        
        for period_end_date, expected_fiscal_year, expected_fiscal_period in test_cases:
            with self.subTest(f"Testing {ticker} {period_end_date}"):
                # Test with 10-K filing type
                filing_type = "10-K" if expected_fiscal_period == "annual" else "10-Q"
                
                fiscal_info = fiscal_registry.determine_fiscal_period(
                    ticker, period_end_date, filing_type
                )
                
                self.assertEqual(fiscal_info.get("fiscal_year"), expected_fiscal_year)
                self.assertEqual(fiscal_info.get("fiscal_period"), expected_fiscal_period)
                # Our implementation includes validation
                self.assertTrue(fiscal_info.get("validated", False))
    
    def test_data_contract_validation(self):
        """Test the FiscalPeriodInfo data contract validation"""
        # Valid data
        valid_data = {
            "ticker": "NVDA",
            "period_end_date": "2024-01-28",
            "fiscal_year": "2024",
            "fiscal_period": "annual",
            "filing_type": "10-K"
        }
        
        # This should work
        fiscal_info = FiscalPeriodInfo(**valid_data)
        self.assertEqual(fiscal_info.ticker, "NVDA")
        self.assertEqual(fiscal_info.fiscal_year, "2024")
        self.assertEqual(fiscal_info.fiscal_period, "annual")
        
        # Test invalid fiscal period
        invalid_period = valid_data.copy()
        invalid_period["fiscal_period"] = "invalid"
        with self.assertRaises(FiscalDataError):
            FiscalPeriodInfo(**invalid_period)
        
        # Test invalid date format
        invalid_date = valid_data.copy()
        invalid_date["period_end_date"] = "01/28/2024"  # Wrong format
        with self.assertRaises(FiscalDataError):
            FiscalPeriodInfo(**invalid_date)
        
        # Test missing required field
        missing_field = valid_data.copy()
        del missing_field["fiscal_year"]
        with self.assertRaises(TypeError):  # Python's dataclass will raise TypeError
            FiscalPeriodInfo(**missing_field)
    
    def test_all_companies_all_quarters(self):
        """Test fiscal period determination for all companies and all quarters"""
        # Companies to test
        companies = ["NVDA", "MSFT", "AAPL", "GOOGL"]
        
        for ticker in companies:
            # Get the company's fiscal calendar
            calendar = fiscal_registry.get_calendar(ticker)
            self.assertIsNotNone(calendar, f"No fiscal calendar found for {ticker}")
            
            # Get all period end dates for this company
            period_end_dates = calendar.period_end_dates
            self.assertGreater(len(period_end_dates), 0, f"No period end dates found for {ticker}")
            
            # Test each period end date
            for period_end_date, expected_data in period_end_dates.items():
                with self.subTest(f"Testing {ticker} {period_end_date}"):
                    # Determine filing type based on fiscal period
                    filing_type = "10-K" if expected_data.get("fiscal_period") == "annual" else "10-Q"
                    
                    # Get fiscal info from registry
                    fiscal_info = fiscal_registry.determine_fiscal_period(
                        ticker, period_end_date, filing_type
                    )
                    
                    # Verify result matches expected data
                    self.assertEqual(fiscal_info.get("fiscal_year"), expected_data.get("fiscal_year"))
                    self.assertEqual(fiscal_info.get("fiscal_period"), expected_data.get("fiscal_period"))
                    # Our implementation includes validation
                    self.assertTrue(fiscal_info.get("validated", False))


if __name__ == "__main__":
    print("\n=== Testing Fiscal Data Integrity System ===\n")
    unittest.main()