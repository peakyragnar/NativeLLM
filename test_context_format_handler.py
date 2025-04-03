#!/usr/bin/env python3
"""
Test script for the context format handler
"""

import logging
from src2.formatter.context_format_handler import extract_period_info

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_context_format_handler():
    """Test the context format handler with various context formats"""
    
    # Test cases for different context formats
    test_cases = [
        # Format 1: C_0000789019_20200701_20210630 (duration with CIK)
        {
            "context_ref": "C_0000789019_20200701_20210630",
            "expected": {
                "startDate": "2020-07-01",
                "endDate": "2021-06-30"
            }
        },
        # Format 2: C_0000789019_20200701 (instant with CIK)
        {
            "context_ref": "C_0000789019_20200701",
            "expected": {
                "instant": "2020-07-01"
            }
        },
        # Format 3: _D20200701-20210630 (standard duration)
        {
            "context_ref": "SomePrefix_D20200701-20210630",
            "expected": {
                "startDate": "2020-07-01",
                "endDate": "2021-06-30"
            }
        },
        # Format 4: _I20200701 (standard instant)
        {
            "context_ref": "SomePrefix_I20200701",
            "expected": {
                "instant": "2020-07-01"
            }
        },
        # Format 5: NVDA format with embedded dates
        {
            "context_ref": "i2c5e111a942340e08ad1e8d2e3b0fb71_D20210201-20220130",
            "expected": {
                "startDate": "2021-02-01",
                "endDate": "2022-01-30"
            }
        },
        # Format 6: NVDA format with embedded instant date
        {
            "context_ref": "i2c5e111a942340e08ad1e8d2e3b0fb71_I20210201",
            "expected": {
                "instant": "2021-02-01"
            }
        },
        # Format 7: MSFT format with embedded dates (not fully implemented yet)
        {
            "context_ref": "FD2022Q3YTD_us-gaap_StatementOfCashFlowsAbstract",
            "expected": None
        },
        # Format 8: Simple context ID (c-1)
        {
            "context_ref": "c-1",
            "expected": None  # This should be handled by HTML extraction, not pattern matching
        },
        # Unknown format
        {
            "context_ref": "unknown_format_123",
            "expected": None
        }
    ]
    
    # Run the tests
    print("Testing context format handler...")
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases):
        context_ref = test_case["context_ref"]
        expected = test_case["expected"]
        
        # Extract period info
        result = extract_period_info(context_ref)
        
        # Check if the result matches the expected output
        if result == expected:
            print(f"✓ Test {i+1} passed: {context_ref}")
            passed += 1
        else:
            print(f"✗ Test {i+1} failed: {context_ref}")
            print(f"  Expected: {expected}")
            print(f"  Got: {result}")
            failed += 1
    
    # Print summary
    print(f"\nTest summary: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("All tests passed!")
    else:
        print(f"Failed {failed} tests.")

if __name__ == "__main__":
    test_context_format_handler()
