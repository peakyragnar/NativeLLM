#!/usr/bin/env python3
"""
Script to examine a company's fiscal calendar and validate period mappings.
"""

import os
import sys
import calendar
from tabulate import tabulate

# Import fiscal registry
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.edgar.company_fiscal import fiscal_registry, CompanyFiscalCalendar

def print_fiscal_calendar(ticker):
    """Print the fiscal calendar details for a company"""
    calendar = fiscal_registry.get_calendar(ticker)
    
    print(f"Fiscal Calendar for {ticker}:")
    print(f"Fiscal Year End Month: {calendar.fiscal_year_end_month} ({calendar_month_name(calendar.fiscal_year_end_month)})")
    print(f"Fiscal Year End Day: {calendar.fiscal_year_end_day}")
    print(f"Confidence Score: {calendar.confidence_score:.2f}")
    print()

def calendar_month_name(month_num):
    """Get month name from month number"""
    if month_num is None:
        return "Unknown"
    return calendar.month_name[month_num]

def generate_period_mapping_table(ticker):
    """Generate a table showing how calendar months map to fiscal periods"""
    company_cal = fiscal_registry.get_calendar(ticker)
    
    # Generate test dates (15th of each month in 2023)
    test_dates = []
    for month in range(1, 13):
        test_dates.append(f"2023-{month:02d}-15")
    
    # Prepare table data
    table_data = []
    for date_str in test_dates:
        # For 10-Q
        q_result = fiscal_registry.determine_fiscal_period(ticker, date_str, "10-Q")
        q_year = q_result.get('fiscal_year')
        q_period = q_result.get('fiscal_period')
        
        # For 10-K
        k_result = fiscal_registry.determine_fiscal_period(ticker, date_str, "10-K")
        k_year = k_result.get('fiscal_year')
        k_period = k_result.get('fiscal_period')
        
        # Extract month from date
        month = int(date_str.split('-')[1])
        month_name = calendar.month_name[month]
        
        table_data.append([
            month_name,
            f"{q_year}-{q_period}",
            f"{k_year}-{k_period}"
        ])
    
    # Print table
    headers = ["Calendar Month", "10-Q Fiscal Period", "10-K Fiscal Period"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def test_specific_dates(ticker):
    """Test specific period end dates that are causing issues"""
    test_dates = [
        "2023-04-01",  # The AAPL-10-Q-2023-Q2 filing with issue
        "2023-07-01",  # The AAPL-10-Q-2023-Q3 filing
    ]
    
    print("\nTesting Specific Dates:")
    
    table_data = []
    for date_str in test_dates:
        # For 10-Q
        q_result = fiscal_registry.determine_fiscal_period(ticker, date_str, "10-Q")
        q_year = q_result.get('fiscal_year')
        q_period = q_result.get('fiscal_period')
        
        table_data.append([
            date_str,
            f"{q_year}-{q_period}"
        ])
    
    # Print table
    headers = ["Period End Date", "Expected Fiscal Period"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Examine a company's fiscal calendar")
    parser.add_argument("ticker", help="Company ticker to examine")
    
    args = parser.parse_args()
    
    print_fiscal_calendar(args.ticker)
    generate_period_mapping_table(args.ticker)
    test_specific_dates(args.ticker)