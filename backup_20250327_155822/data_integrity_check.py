#!/usr/bin/env python
# Data integrity checks for HTML optimization
# This script verifies that all financial data maintains 100% integrity

import os
import sys
import re
import logging
import random
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.xbrl.xbrl_parser import extract_text_only_from_html, process_table_safely

def check_numeric_integrity():
    """Verify that numeric values are always preserved"""
    # Create a list of different numeric formats to test
    test_cases = [
        {"value": "$123,456.78", "html": "<span style='color:red;'>$123,456.78</span>"},
        {"value": "12.34%", "html": "<div style='font-family:Arial;'>12.34%</div>"},
        {"value": "(42,000)", "html": "<td style='background-color:#f0f0f0;'>(42,000)</td>"},
        {"value": "$0.05", "html": "<p style='text-align:right;'>$0.05</p>"},
        {"value": "1,234,567", "html": "<strong style='color:blue;'>1,234,567</strong>"},
        {"value": "(0.025%)", "html": "<em style='font-style:italic;'>(0.025%)</em>"},
        {"value": "$1.23 million", "html": "<div>Revenue of <span style='color:green;'>$1.23 million</span></div>"},
        {"value": "42%", "html": "<div>Increase of <b style='color:green;'>42%</b> year-over-year</div>"},
        {"value": "n/a", "html": "<td style='text-align:center;'>n/a</td>"},
        {"value": "—", "html": "<td style='font-family:Arial;'>—</td>"}
    ]
    
    failures = []
    success_count = 0
    
    # Process each test case
    print("Checking numeric integrity for various formats...\n")
    for i, test in enumerate(test_cases, 1):
        value = test["value"]
        html = test["html"]
        
        # Process with our HTML cleaner
        processed = extract_text_only_from_html(html)
        
        # Check if the value is preserved
        if value in processed:
            success_count += 1
            print(f"✅ Test {i}: {value} preserved")
        else:
            failures.append({"value": value, "html": html, "processed": processed})
            print(f"❌ Test {i}: {value} LOST")
    
    # Report results
    print(f"\nNumeric integrity: {success_count}/{len(test_cases)} values preserved")
    if failures:
        print("\nFailed cases:")
        for i, failure in enumerate(failures, 1):
            print(f"  {i}. Value: {failure['value']}")
            print(f"     Original HTML: {failure['html']}")
            print(f"     Processed: {failure['processed']}")
    else:
        print("\nAll numeric values preserved correctly!")
    
    return len(failures) == 0

def check_table_integrity():
    """Verify table integrity with complex financial tables"""
    # Create a complex financial table with various data formats
    financial_table = """
    <table style="width:100%; border-collapse:collapse; font-family:Arial; color:#333;">
        <tr style="background-color:#f2f2f2;">
            <th style="padding:8px; text-align:left;">Financial Metric</th>
            <th style="padding:8px; text-align:right;">2023</th>
            <th style="padding:8px; text-align:right;">2022</th>
            <th style="padding:8px; text-align:right;">% Change</th>
        </tr>
        <tr>
            <td style="padding:8px; border-bottom:1px solid #ddd;">Revenue</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$95,281</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$84,310</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+13.0%</td>
        </tr>
        <tr>
            <td style="padding:8px; border-bottom:1px solid #ddd;">Operating Income</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$32,131</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$30,457</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+5.5%</td>
        </tr>
        <tr>
            <td style="padding:8px; border-bottom:1px solid #ddd;">Net Income</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$106,572</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$99,803</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+6.8%</td>
        </tr>
        <tr>
            <td style="padding:8px; border-bottom:1px solid #ddd;">EPS (Diluted)</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$6.14</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">$5.61</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">+9.4%</td>
        </tr>
        <tr>
            <td style="padding:8px; border-bottom:1px solid #ddd;">Operating Margin</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">33.7%</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">36.1%</td>
            <td style="padding:8px; border-bottom:1px solid #ddd; text-align:right;">-2.4%</td>
        </tr>
    </table>
    """
    
    # Critical financial values to check
    critical_values = [
        "$95,281", "$84,310", "+13.0%", 
        "$32,131", "$30,457", "+5.5%",
        "$106,572", "$99,803", "+6.8%",
        "$6.14", "$5.61", "+9.4%",
        "33.7%", "36.1%", "-2.4%"
    ]
    
    # Process with our table processor
    processed_table = process_table_safely(financial_table)
    
    # Check all critical values
    missing_values = []
    for value in critical_values:
        if value not in processed_table:
            missing_values.append(value)
    
    # Check table structure
    soup = BeautifulSoup(processed_table, 'html.parser')
    rows = soup.find_all('tr')
    cells = soup.find_all(['td', 'th'])
    
    print("\nChecking table integrity...\n")
    print(f"Original table size: {len(financial_table)} bytes")
    print(f"Processed table size: {len(processed_table)} bytes")
    print(f"Size reduction: {(1 - len(processed_table)/len(financial_table)) * 100:.2f}%")
    print(f"Rows preserved: {len(rows) == 6}")
    print(f"Cells preserved: {len(cells) == 24}")
    
    if missing_values:
        print("\n❌ Missing critical financial values:")
        for value in missing_values:
            print(f"  - {value}")
        return False
    else:
        print("\n✅ All critical financial values preserved!")
        return True

def main():
    print("=== Data Integrity Check for HTML Optimization ===\n")
    
    numeric_result = check_numeric_integrity()
    table_result = check_table_integrity()
    
    print("\n=== Summary ===")
    print(f"Numeric integrity: {'✅ PASSED' if numeric_result else '❌ FAILED'}")
    print(f"Table integrity: {'✅ PASSED' if table_result else '❌ FAILED'}")
    
    overall_result = numeric_result and table_result
    print(f"\nOverall data integrity: {'✅ PASSED' if overall_result else '❌ FAILED'}")
    
    return 0 if overall_result else 1

if __name__ == "__main__":
    sys.exit(main())