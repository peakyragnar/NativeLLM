#!/usr/bin/env python3
"""
Data Integrity Validator for SEC Filing Pipeline

This module provides validation functions that can be called directly from the pipeline
to ensure the integrity of processed SEC filings.
"""

import os
import logging
import re
from google.cloud import firestore
from google.cloud import storage
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def validate_filing_integrity(ticker, filing_type, fiscal_year, fiscal_period=None, bucket_name="native-llm-filings"):
    """
    Validate filing integrity and return the result directly.
    
    This function is called from the pipeline after a successful download.
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (10-K, 10-Q)
        fiscal_year: Fiscal year
        fiscal_period: Fiscal period (optional, required for 10-Q)
        bucket_name: GCS bucket name
        
    Returns:
        Dict with validation results
    """
    try:
        # First get filing metadata from Firestore
        filing_metadata = get_filing_metadata(ticker, filing_type, fiscal_year, fiscal_period)
        
        if not filing_metadata:
            logging.error(f"No metadata found for {ticker} {filing_type} {fiscal_year} {fiscal_period}")
            return {"status": "FAIL", "error": "Metadata not found"}
        
        # Get file paths
        text_path = filing_metadata.get('text_file_path')
        llm_path = filing_metadata.get('llm_file_path')
        
        if not text_path or not llm_path:
            logging.error(f"Missing file paths in metadata")
            return {"status": "FAIL", "error": "Missing file paths"}
        
        # Read files from GCS
        text_content, llm_content = read_gcs_files(text_path, llm_path, bucket_name)
        
        if not text_content or not llm_content:
            logging.error(f"Failed to read GCS files")
            return {"status": "FAIL", "error": "Failed to read files"}
        
        # Validate LLM format
        llm_validation = validate_llm_format(llm_content)
        
        # Validate data consistency
        data_validation = validate_data_consistency(text_content, llm_content)
        
        # Compile results
        result = {
            "ticker": ticker,
            "filing_type": filing_type,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "text_file_path": text_path,
            "llm_file_path": llm_path,
            "llm_format_valid": llm_validation["valid"],
            "data_consistent": data_validation["valid"],
            "llm_format_details": llm_validation["details"],
            "data_consistency_details": data_validation["details"],
            "processed_text_size": len(text_content),
            "processed_llm_size": len(llm_content),
            "status": "PASS" if (llm_validation["valid"] and data_validation["valid"]) else "FAIL"
        }
        
        # Log results
        if result["status"] == "PASS":
            logging.info(f"Data integrity validation PASSED for {ticker} {filing_type} {fiscal_year} {fiscal_period}")
        else:
            logging.warning(f"Data integrity validation FAILED for {ticker} {filing_type} {fiscal_year} {fiscal_period}")
            if not llm_validation["valid"]:
                logging.warning(f"LLM format validation failed: {llm_validation['details']}")
            if not data_validation["valid"]:
                logging.warning(f"Data consistency validation failed: {data_validation['details']}")
        
        return result
        
    except Exception as e:
        logging.error(f"Error during data integrity validation: {str(e)}")
        return {"status": "ERROR", "error": str(e)}

def get_filing_metadata(ticker, filing_type, fiscal_year, fiscal_period=None):
    """Get filing metadata from Firestore"""
    try:
        # Initialize Firestore client
        db = firestore.Client(database='nativellm')
        
        # Determine document ID
        if fiscal_period and filing_type == "10-Q":
            # For 10-Q, we need the fiscal period
            document_id = f"{ticker}_{filing_type}_{fiscal_year}_{fiscal_period}"
        else:
            # For 10-K, just use fiscal year
            document_id = f"{ticker}_{filing_type}_{fiscal_year}"
        
        # Get document
        doc_ref = db.collection('filings').document(document_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            logging.error(f"Filing not found: {document_id}")
            return None
        
        return doc.to_dict()
        
    except Exception as e:
        logging.error(f"Error getting filing metadata: {str(e)}")
        return None

def read_gcs_files(text_path, llm_path, bucket_name):
    """Read files from GCS"""
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Read text file
        text_blob = bucket.blob(text_path)
        text_content = text_blob.download_as_text()
        
        # Read LLM file
        llm_blob = bucket.blob(llm_path)
        llm_content = llm_blob.download_as_text()
        
        return text_content, llm_content
        
    except Exception as e:
        logging.error(f"Error reading GCS files: {str(e)}")
        return None, None

def validate_llm_format(llm_content):
    """Validate LLM format structure"""
    # Check for required markers
    required_markers = [
        "@DOCUMENT:", 
        "@COMPANY:", 
        "@FILING_DATE:", 
        "@CONCEPT:"
    ]
    
    # Check each marker
    missing_markers = []
    for marker in required_markers:
        if marker not in llm_content:
            missing_markers.append(marker)
    
    # Result
    is_valid = len(missing_markers) == 0
    
    return {
        "valid": is_valid,
        "details": {
            "missing_markers": missing_markers
        }
    }

def validate_data_consistency(text_content, llm_content):
    """Validate data consistency"""
    # Check file sizes
    min_text_size = 50 * 1024  # 50 KB
    min_llm_size = 50 * 1024   # 50 KB
    
    text_size_ok = len(text_content) >= min_text_size
    llm_size_ok = len(llm_content) >= min_llm_size
    
    # Check for important sections in text file
    financial_section_markers = [
        "Financial Statements", 
        "Balance Sheet", 
        "Income Statement", 
        "Statement of Operations", 
        "Cash Flow", 
        "Consolidated"
    ]
    
    metadata_section_markers = [
        "PART I", 
        "ITEM 1", 
        "ITEM 2", 
        "MANAGEMENT"
    ]
    
    # Check for section markers
    has_financial_section = any(marker in text_content for marker in financial_section_markers)
    has_metadata_section = any(marker in text_content for marker in metadata_section_markers)
    
    # Result
    is_valid = text_size_ok and llm_size_ok and has_financial_section and has_metadata_section
    
    return {
        "valid": is_valid,
        "details": {
            "text_size_ok": text_size_ok,
            "llm_size_ok": llm_size_ok,
            "has_financial_section": has_financial_section,
            "has_metadata_section": has_metadata_section
        }
    }

# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate SEC filing integrity")
    parser.add_argument("--ticker", required=True, help="Company ticker symbol")
    parser.add_argument("--filing-type", required=True, choices=["10-K", "10-Q"], help="Filing type")
    parser.add_argument("--fiscal-year", required=True, help="Fiscal year")
    parser.add_argument("--fiscal-period", help="Fiscal period (required for 10-Q)")
    parser.add_argument("--bucket", default="native-llm-filings", help="GCS bucket name")
    
    args = parser.parse_args()
    
    # Ensure fiscal period is provided for 10-Q
    if args.filing_type == "10-Q" and not args.fiscal_period:
        parser.error("--fiscal-period is required for 10-Q filings")
    
    # Run validation
    result = validate_filing_integrity(
        args.ticker,
        args.filing_type,
        args.fiscal_year,
        args.fiscal_period,
        args.bucket
    )
    
    # Print result
    print(f"\nValidation Result: {result['status']}")
    if result["status"] != "PASS":
        print(f"Error: {result.get('error', 'Unknown error')}")