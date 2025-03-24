import datetime
import sys
import os

# Add the project root to the path
sys.path.append(os.path.abspath('.'))

from src.edgar.company_fiscal import fiscal_registry

def test_apple_fiscal_periods():
    """Test Apple's fiscal period determination"""
    # Apple's fiscal year ends in September (Q4 is Jul-Sep)
    # Q1: Oct-Dec, Q2: Jan-Mar, Q3: Apr-Jun, Q4: Jul-Sep
    
    test_cases = [
        # Date format: YYYY-MM-DD
        # Q1 tests (Oct-Dec)
        {"date": "2022-10-15", "expected_year": "2023", "expected_period": "Q1"},
        {"date": "2022-11-30", "expected_year": "2023", "expected_period": "Q1"},
        {"date": "2022-12-31", "expected_year": "2023", "expected_period": "Q1"},
        
        # Q2 tests (Jan-Mar)
        {"date": "2023-01-15", "expected_year": "2023", "expected_period": "Q2"},
        {"date": "2023-02-28", "expected_year": "2023", "expected_period": "Q2"},
        {"date": "2023-03-31", "expected_year": "2023", "expected_period": "Q2"},
        
        # Q3 tests (Apr-Jun)
        {"date": "2023-04-15", "expected_year": "2023", "expected_period": "Q3"},
        {"date": "2023-05-31", "expected_year": "2023", "expected_period": "Q3"},
        {"date": "2023-06-30", "expected_year": "2023", "expected_period": "Q3"},
        
        # Q4 tests (Jul-Sep) - should be annual for 10-K, Q3 for 10-Q
        {"date": "2023-07-15", "expected_year": "2023", "expected_period": "Q3", "filing_type": "10-Q"},
        {"date": "2023-08-31", "expected_year": "2023", "expected_period": "Q3", "filing_type": "10-Q"},
        {"date": "2023-09-30", "expected_year": "2023", "expected_period": "annual", "filing_type": "10-K"},
    ]
    
    for i, test in enumerate(test_cases):
        # Test our script's fiscal period logic
        period_date = datetime.datetime.strptime(test["date"], '%Y-%m-%d')
        
        # Determine fiscal period manually using the logic from calendar_download.py
        fiscal_year = None
        fiscal_period = None
        filing_type = test.get("filing_type", "10-Q")  # Default to 10-Q
        
        month = period_date.month
        
        if month in [10, 11, 12]:  # Oct-Dec = Q1 of next calendar year
            fiscal_year = str(period_date.year + 1)
            if filing_type != "10-K":
                fiscal_period = "Q1"
        elif month in [1, 2, 3]:  # Jan-Mar = Q2
            fiscal_year = str(period_date.year)
            if filing_type != "10-K":
                fiscal_period = "Q2"
        elif month in [4, 5, 6]:  # Apr-Jun = Q3
            fiscal_year = str(period_date.year)
            if filing_type != "10-K":
                fiscal_period = "Q3"
        else:  # Jul-Sep
            fiscal_year = str(period_date.year)
            # For Apple, there should be no 10-Q for Q4, only a 10-K
            if filing_type == "10-K":
                fiscal_period = "annual"
            else:
                fiscal_period = "Q3"  # Important: Apple doesn't have Q4 10-Q filings
        
        if filing_type == "10-K" and not fiscal_period:
            fiscal_period = "annual"
        
        # Compare with expected values
        expected_year = test["expected_year"]
        expected_period = test["expected_period"]
        
        result = f"✓" if fiscal_year == expected_year and fiscal_period == expected_period else "✗"
        
        print(f"Test {i+1}: {test['date']} ({filing_type}) -> FY{fiscal_year} {fiscal_period} {result}")
        if result == "✗":
            print(f"  Expected: FY{expected_year} {expected_period}")
        
        # Also test using the fiscal_registry
        filing_type = test.get("filing_type", "10-Q")  # Default to 10-Q
        reg_result = fiscal_registry.determine_fiscal_period("AAPL", test["date"], filing_type)
        reg_year = reg_result.get("fiscal_year")
        reg_period = reg_result.get("fiscal_period")
        
        reg_match = f"✓" if reg_year == expected_year and reg_period == expected_period else "✗"
        print(f"  Registry: FY{reg_year} {reg_period} {reg_match}")

if __name__ == "__main__":
    test_apple_fiscal_periods()