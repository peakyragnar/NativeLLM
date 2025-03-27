#!/usr/bin/env python3
"""
Test script to verify src2 module functionality.

This script tests various components of the src2 modules to ensure
they work correctly before removing the src folder.
"""

import os
import sys
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_config():
    """Test the config module"""
    logging.info("Testing config module...")
    try:
        from src2 import config
        logging.info(f"Company Name: {config.COMPANY_NAME}")
        logging.info(f"Filing Types: {config.FILING_TYPES}")
        logging.info(f"HTML Optimization Level: {config.HTML_OPTIMIZATION['level']}")
        return True
    except Exception as e:
        logging.error(f"Error in config module: {str(e)}")
        return False

def test_enhanced_processor():
    """Test the enhanced processor module"""
    logging.info("Testing enhanced processor module...")
    try:
        from src2.processor.enhanced_processor import (
            determine_preferred_format, 
            process_company_filing
        )
        
        # Test format detection
        filing_metadata = {
            "filing_date": "2023-12-31"
        }
        format_type = determine_preferred_format(filing_metadata)
        logging.info(f"Detected format type: {format_type}")
        
        return True
    except Exception as e:
        logging.error(f"Error in enhanced processor module: {str(e)}")
        return False

def test_fiscal_manager():
    """Test the fiscal manager module"""
    logging.info("Testing fiscal manager module...")
    try:
        from src2.sec.fiscal.fiscal_manager import FiscalPeriodManager
        
        # Create manager
        manager = FiscalPeriodManager()
        
        # Test Microsoft fiscal period detection
        msft_info = manager.determine_fiscal_period("MSFT", "2023-12-31", "10-Q")
        logging.info(f"MSFT fiscal period for 2023-12-31: {msft_info}")
        
        # Test Apple fiscal period detection
        aapl_info = manager.determine_fiscal_period("AAPL", "2023-12-31", "10-Q")
        logging.info(f"AAPL fiscal period for 2023-12-31: {aapl_info}")
        
        return True
    except Exception as e:
        logging.error(f"Error in fiscal manager module: {str(e)}")
        return False

def test_batch_pipeline():
    """Test that the batch pipeline imports work"""
    logging.info("Testing batch pipeline imports...")
    try:
        from src2.sec.batch_pipeline import BatchSECPipeline
        
        # Just test that we can import it
        logging.info("Successfully imported BatchSECPipeline")
        return True
    except Exception as e:
        logging.error(f"Error importing batch pipeline: {str(e)}")
        return False

def test_value_normalization():
    """Test the value normalization module"""
    logging.info("Testing value normalization...")
    try:
        from src2.formatter.normalize_value import normalize_value
        
        # Test with different value types
        test_values = [
            "1,234,567.89",
            "1234.56",
            "1,234",
            "(123.45)"
        ]
        
        for val in test_values:
            normalized, changed = normalize_value(val)
            logging.info(f"Original: {val}, Normalized: {normalized}, Changed: {changed}")
        
        return True
    except Exception as e:
        logging.error(f"Error in value normalization module: {str(e)}")
        return False

def main():
    """Main test function"""
    parser = argparse.ArgumentParser(description="Test src2 modules")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--config", action="store_true", help="Test config module")
    parser.add_argument("--processor", action="store_true", help="Test enhanced processor")
    parser.add_argument("--fiscal", action="store_true", help="Test fiscal manager")
    parser.add_argument("--pipeline", action="store_true", help="Test batch pipeline imports")
    parser.add_argument("--normalize", action="store_true", help="Test value normalization")
    
    args = parser.parse_args()
    
    # If no specific tests selected, run all
    run_all = args.all or not (args.config or args.processor or args.fiscal or 
                               args.pipeline or args.normalize)
    
    tests = []
    results = {}
    
    # Add selected tests
    if run_all or args.config:
        tests.append(("config", test_config))
    if run_all or args.processor:
        tests.append(("processor", test_enhanced_processor))
    if run_all or args.fiscal:
        tests.append(("fiscal", test_fiscal_manager))
    if run_all or args.pipeline:
        tests.append(("pipeline", test_batch_pipeline))
    if run_all or args.normalize:
        tests.append(("normalize", test_value_normalization))
    
    # Run tests
    success_count = 0
    for name, test_func in tests:
        logging.info(f"\n=== Running {name} test ===")
        try:
            success = test_func()
            results[name] = success
            if success:
                success_count += 1
        except Exception as e:
            logging.error(f"Unexpected error in {name} test: {str(e)}")
            results[name] = False
    
    # Print summary
    logging.info("\n=== Test Summary ===")
    for name, success in results.items():
        status = "PASSED" if success else "FAILED"
        logging.info(f"{name}: {status}")
    
    logging.info(f"\nOverall: {success_count}/{len(tests)} tests passed")
    
    return 0 if success_count == len(tests) else 1

if __name__ == "__main__":
    sys.exit(main())