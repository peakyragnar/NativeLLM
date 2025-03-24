"""
Microsoft Fiscal Period Tests

These tests verify that the Microsoft fiscal period logic is correctly implemented.
Microsoft's fiscal year:
- Q1: Jul-Sep
- Q2: Oct-Dec
- Q3: Jan-Mar
- Annual (never Q4): Apr-Jun
"""

import unittest
import sys
import os
import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.edgar.company_fiscal import CompanyFiscalCalendar

class TestMicrosoftFiscalPeriods(unittest.TestCase):
    """Test Microsoft fiscal period determination"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.calendar = CompanyFiscalCalendar("MSFT")
    
    def test_q1_period(self):
        """Test Q1 period (Jul-Sep)"""
        # Q1 2023 = Jul-Sep 2022 (fiscal year 2023)
        result = self.calendar.determine_fiscal_period("2022-07-31", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q1")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2022-08-15", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q1")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2022-09-30", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q1")
        self.assertEqual(result["fiscal_year"], "2023")
    
    def test_q2_period(self):
        """Test Q2 period (Oct-Dec)"""
        # Q2 2023 = Oct-Dec 2022 (fiscal year 2023)
        result = self.calendar.determine_fiscal_period("2022-10-31", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q2")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2022-11-15", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q2")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2022-12-31", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q2")
        self.assertEqual(result["fiscal_year"], "2023")
    
    def test_q3_period(self):
        """Test Q3 period (Jan-Mar)"""
        # Q3 2023 = Jan-Mar 2023 (fiscal year 2023)
        result = self.calendar.determine_fiscal_period("2023-01-31", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q3")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2023-02-15", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q3")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2023-03-31", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q3")
        self.assertEqual(result["fiscal_year"], "2023")
    
    def test_annual_period(self):
        """Test annual period (Apr-Jun) - should NEVER be Q4"""
        # Annual 2023 = Apr-Jun 2023 (fiscal year 2023)
        
        # For 10-Q filings in Apr-Jun, fiscal period should be "annual"
        result = self.calendar.determine_fiscal_period("2023-04-30", "10-Q")
        self.assertEqual(result["fiscal_period"], "annual")
        self.assertEqual(result["fiscal_year"], "2023")
        
        result = self.calendar.determine_fiscal_period("2023-05-15", "10-Q")
        self.assertEqual(result["fiscal_period"], "annual")
        self.assertEqual(result["fiscal_year"], "2023")
        
        # Should NEVER be "Q4" for any filing type
        result = self.calendar.determine_fiscal_period("2023-06-30", "10-Q")
        self.assertEqual(result["fiscal_period"], "annual")
        self.assertEqual(result["fiscal_year"], "2023")
        
        # 10-K filings should always be "annual"
        result = self.calendar.determine_fiscal_period("2023-06-30", "10-K")
        self.assertEqual(result["fiscal_period"], "annual")
        self.assertEqual(result["fiscal_year"], "2023")
    
    def test_fiscal_year_transition(self):
        """Test fiscal year transition behavior"""
        # June 30 marks the end of the fiscal year
        # After June 30, 2023, we're in fiscal year 2024
        
        # Last day of FY2023
        result = self.calendar.determine_fiscal_period("2023-06-30", "10-K")
        self.assertEqual(result["fiscal_period"], "annual")
        self.assertEqual(result["fiscal_year"], "2023")
        
        # First day of FY2024
        result = self.calendar.determine_fiscal_period("2023-07-01", "10-Q")
        self.assertEqual(result["fiscal_period"], "Q1")
        self.assertEqual(result["fiscal_year"], "2024")

if __name__ == "__main__":
    unittest.main()