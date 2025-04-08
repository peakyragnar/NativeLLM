#!/usr/bin/env python3
"""
Test Firestore Pipeline Integration

This script tests the Firestore integration in the SEC pipeline.
"""

import os
import sys
import logging
import datetime
import json
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_firestore_pipeline_integration():
    """
    Test the Firestore integration in the SEC pipeline.
    """
    try:
        # Import the necessary modules
        from src2.storage.gcp_storage import GCPStorage
        
        # Create a GCP storage instance
        gcp_storage = GCPStorage(bucket_name="native-llm-filings", project_id="nativellmfilings")
        
        # Check if Firestore is enabled
        if not gcp_storage.is_firestore_enabled():
            logging.error("Firestore is not enabled")
            return False
        
        # Create test metadata
        metadata = {
            "ticker": "TEST",
            "cik": "0000000000",
            "filing_type": "10-K",
            "filing_date": "2025-04-08",
            "period_end_date": "2025-01-31",
            "company_name": "Test Company",
            "source_url": "https://example.com",
            "fiscal_year": "2025",
            "fiscal_period": "annual"
        }
        
        # Create test metadata update
        metadata_update = {
            "llm_path": "gs://native-llm-filings/TEST/TEST_10-K_2025_annual_llm.txt",
            "llm_size": 1000,
            "text_size": 0,
            "text_file_skipped": True
        }
        
        # Update Firestore
        logging.info("Adding test metadata to Firestore...")
        metadata_result = gcp_storage.add_filing_metadata(
            metadata,
            **metadata_update
        )
        
        # Check result
        if metadata_result.get("success", False):
            logging.info("Successfully added test metadata to Firestore")
            logging.info(f"Document ID: {metadata_result.get('document_id')}")
            return True
        else:
            logging.error(f"Failed to add test metadata to Firestore: {metadata_result.get('error')}")
            return False
    
    except ImportError as e:
        logging.error(f"Import error: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Error testing Firestore pipeline integration: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_firestore_pipeline_integration()
    
    # Exit with appropriate status code
    if success:
        sys.exit(0)
    else:
        sys.exit(1)
