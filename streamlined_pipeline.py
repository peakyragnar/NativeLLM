#!/usr/bin/env python3
"""
Streamlined SEC XBRL Pipeline

This module combines the best of both approaches:
1. Uses secedgar for reliable URL construction and filing discovery
2. Maintains calendar-based filtering from enhanced_pipeline.py
3. Preserves full HTML processing and cloud integration

The goal is to create a unified, reliable pipeline that works for all filing years
from 2022-2025+ with consistent handling of all document types.
"""

import os
import sys
import json
import time
import logging
import re
import datetime
import argparse
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("streamlined_pipeline.log"),
        logging.StreamHandler()
    ]
)

# Import secedgar for reliable URL handling
from secedgar.client import NetworkClient
from secedgar.core.filings import FilingSet, filings
from secedgar.core.filing_types import FilingType

# Import GCP modules
try:
    from google.cloud import storage, firestore
except ImportError:
    logging.warning("Google Cloud libraries not found. Cloud storage integration will be disabled.")

# Import core processing modules
from src.edgar.edgar_utils import get_cik_from_ticker, get_company_name_from_cik
from src.edgar.filing_finder import find_company_filings
from src.edgar.fiscal_manager import fiscal_manager
from src.xbrl.enhanced_processor import process_company_filing
from src.xbrl.html_text_extractor import process_html_filing
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import INITIAL_COMPANIES, PROCESSED_DATA_DIR

# Thresholds for file size warnings
SIZE_THRESHOLDS = {
    "text": {
        "min": 50 * 1024,     # 50 KB minimum
        "warn": 200 * 1024    # 200 KB warning threshold
    },
    "llm": {
        "min": 100 * 1024,    # 100 KB minimum
        "warn": 500 * 1024    # 500 KB warning threshold
    }
}

# GCP settings (override with environment variables)
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "nativellm-filings")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nativellm-sec")

def configure_gcp():
    """
    Configure Google Cloud Platform credentials and services
    
    Returns:
        bool: True if configuration succeeded, False otherwise
    """
    if os.environ.get("SKIP_GCP_UPLOAD") == "1":
        logging.info("Skipping GCP configuration due to SKIP_GCP_UPLOAD environment variable")
        return True
        
    try:
        # Check for GOOGLE_APPLICATION_CREDENTIALS environment variable
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            # Look for gcp-credentials.json in the current directory
            if os.path.exists("gcp-credentials.json"):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("gcp-credentials.json")
                logging.info(f"Using GCP credentials from: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")
            else:
                logging.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set and no gcp-credentials.json found")
                return False
        
        # Initialize GCS client and check bucket exists
        storage_client = storage.Client()
        try:
            bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
            logging.info(f"Connected to GCS bucket: {GCS_BUCKET_NAME}")
        except Exception as e:
            logging.error(f"Error accessing GCS bucket {GCS_BUCKET_NAME}: {str(e)}")
            return False
        
        # Initialize Firestore client
        db = firestore.Client(project=GCP_PROJECT_ID)
        logging.info(f"Connected to Firestore project: {GCP_PROJECT_ID}")
        
        return True
    except Exception as e:
        logging.error(f"Error configuring GCP: {str(e)}")
        return False

def upload_to_gcs(local_file_path, ticker, filing_type, fiscal_year, fiscal_period, file_format, bucket_name=GCS_BUCKET_NAME):
    """
    Upload a file to Google Cloud Storage
    
    Args:
        local_file_path: Path to the local file
        ticker: Company ticker symbol
        filing_type: Filing type (10-K, 10-Q, etc.)
        fiscal_year: Fiscal year of the filing
        fiscal_period: Fiscal period of the filing
        file_format: Format of the file (text, llm, etc.)
        bucket_name: GCS bucket name
        
    Returns:
        Tuple of (gcs_path, file_size) or (None, 0) if failed
    """
    # Check if we're running with --skip-gcp
    if os.environ.get("SKIP_GCP_UPLOAD") == "1":
        logging.info(f"Skipping GCS upload for {local_file_path} due to --skip-gcp flag")
        # Return a mock path and the actual file size
        mock_gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{fiscal_period}/{file_format}.txt"
        return mock_gcs_path, os.path.getsize(local_file_path)
        
    # Use fiscal_manager to standardize the period for folder naming
    quarter_folder = fiscal_manager.standardize_period(fiscal_period, "folder")
    
    # Construct GCS path
    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_folder}/{file_format}.txt"
    
    try:
        # Initialize client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create blob
        blob = bucket.blob(gcs_path)
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        logging.info(f"Successfully uploaded {local_file_path} to gs://{bucket_name}/{gcs_path}")
        return gcs_path, os.path.getsize(local_file_path)
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {str(e)}")
        return None, 0

def adapt_secedgar_to_metadata(ticker, filing_type, secedgar_filing, count=1):
    """
    Adapt a secedgar filing object to our metadata format
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (10-K, 10-Q, etc.)
        secedgar_filing: Filing object from secedgar
        count: Number of filings to process
        
    Returns:
        List of filing metadata dictionaries
    """
    # Get CIK and company name
    cik = get_cik_from_ticker(ticker)
    if not cik:
        logging.error(f"Could not find CIK for ticker {ticker}")
        return []
        
    company_name = get_company_name_from_cik(cik)
    if not company_name:
        company_name = f"Company {ticker}"
    
    # Create a temporary directory for downloading the filing
    temp_dir = Path(f"data/temp_secedgar/{ticker}_{filing_type}")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Download the filing using secedgar
        logging.info(f"Downloading {filing_type} filings for {ticker} (max {count})")
        secedgar_filing.save(temp_dir)
        
        # Find all downloaded files
        all_files = list(temp_dir.glob("**/*"))
        logging.info(f"Found {len(all_files)} files downloaded by secedgar")
        
        # Extract filing information
        filing_metadatas = []
        
        # Find index files which contain filing metadata
        index_files = list(temp_dir.glob("**/*.idx"))
        
        if not index_files:
            logging.warning(f"No index files found for {ticker} {filing_type}")
            # Try finding other indicator files
            filing_docs = list(temp_dir.glob("**/*-index.htm*"))
            
            if not filing_docs:
                logging.error(f"Could not find filing documents for {ticker} {filing_type}")
                return []
        
        # Process each filing (there may be multiple if count > 1)
        found_filings = list(temp_dir.glob("**/*[0-9]"))
        found_filings = [f for f in found_filings if f.is_dir()]
        
        if not found_filings:
            logging.warning(f"No filing directories found for {ticker} {filing_type}")
            return []
            
        logging.info(f"Found {len(found_filings)} filing directories for {ticker} {filing_type}")
        
        for filing_dir in found_filings[:count]:
            # Extract accession number from directory name
            accession_number = filing_dir.name
            
            # Find the index file for metadata
            index_file = list(filing_dir.glob("*-index.htm*"))
            if not index_file:
                logging.warning(f"No index file found for {accession_number}")
                continue
                
            index_file = index_file[0]
            
            # Find the XBRL file
            xbrl_files = list(filing_dir.glob("**/*.xml"))
            if not xbrl_files:
                logging.warning(f"No XBRL files found for {accession_number}")
                continue
                
            # Look for primary XBRL instance document
            instance_file = None
            for xbrl_file in xbrl_files:
                if xbrl_file.name.endswith('_htm.xml') or '_' in xbrl_file.name:
                    instance_file = xbrl_file
                    break
            
            if not instance_file:
                # Just use the first XML file
                instance_file = xbrl_files[0]
            
            # Create a metadata entry for this filing
            metadata = {
                "ticker": ticker,
                "company_name": company_name,
                "cik": cik,
                "filing_type": filing_type,
                "accession_number": accession_number,
                "instance_url": str(instance_file),  # Local path from secedgar
                "document_url": str(index_file),     # Local path to HTML index
                "primary_doc_url": str(index_file),  # Duplicate for compatibility
                "index_url": str(index_file),        # For finding HTML documents
                "local_base_dir": str(filing_dir),   # Base directory with all files
                "is_secedgar": True                  # Flag to indicate this is from secedgar
            }
            
            # Try to extract filing date and period end date from the files
            try:
                # Read the index file to find dates
                with open(index_file, 'r', encoding='utf-8', errors='replace') as f:
                    index_content = f.read()
                
                # Extract filing date
                filing_date_match = re.search(r'Filing Date</div>\s*<div[^>]*>([^<]+)', index_content)
                if filing_date_match:
                    filing_date = filing_date_match.group(1).strip()
                    metadata["filing_date"] = filing_date
                    
                    # Try to extract fiscal year from filing date
                    if filing_date and len(filing_date) >= 4:
                        try:
                            fiscal_year = filing_date.split('-')[0]
                            metadata["fiscal_year"] = fiscal_year
                        except Exception:
                            pass
                
                # Extract period end date
                period_match = re.search(r'Period of Report</div>\s*<div[^>]*>([^<]+)', index_content)
                if period_match:
                    period_end_date = period_match.group(1).strip()
                    metadata["period_end_date"] = period_end_date
                    
                    # Try to extract fiscal period from period end date
                    if period_end_date and len(period_end_date) >= 10:
                        try:
                            date_parts = period_end_date.split('-')
                            month = int(date_parts[1])
                            day = int(date_parts[2])
                            
                            # Fiscal period logic from fiscal_manager.py
                            if filing_type == "10-K":
                                metadata["fiscal_period"] = "FY"
                            elif filing_type == "10-Q":
                                if month in [1, 2, 3, 4]:
                                    metadata["fiscal_period"] = "Q1"
                                elif month in [5, 6, 7]:
                                    metadata["fiscal_period"] = "Q2"
                                else:
                                    metadata["fiscal_period"] = "Q3"
                        except Exception:
                            pass
            except Exception as e:
                logging.warning(f"Error extracting dates from index file: {str(e)}")
            
            filing_metadatas.append(metadata)
            
        return filing_metadatas
    except Exception as e:
        logging.error(f"Error processing secedgar filing: {str(e)}")
        return []

def process_filing_with_secedgar(ticker, filing_type, count=1, client=None):
    """
    Process filings for a ticker using secedgar library
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (10-K, 10-Q, etc.)
        count: Number of filings to process
        client: NetworkClient from secedgar (or None to create new)
        
    Returns:
        List of processed filing results
    """
    # Create NetworkClient if not provided
    if client is None:
        client = NetworkClient(retry_count=3, pause=0.5)
    
    # Map to secedgar filing type
    if filing_type == "10-K":
        sec_filing_type = FilingType.FILING_10K
    elif filing_type == "10-Q":
        sec_filing_type = FilingType.FILING_10Q
    else:
        logging.error(f"Unsupported filing type: {filing_type}")
        return []
    
    # Create filing object
    filing = filings(cik_lookup=ticker,
                   filing_type=sec_filing_type,
                   count=count,
                   client=client)
    
    # Adapt secedgar filing to our metadata format
    filing_metadatas = adapt_secedgar_to_metadata(ticker, filing_type, filing, count)
    
    if not filing_metadatas:
        logging.error(f"Failed to extract filing metadata for {ticker} {filing_type}")
        return []
    
    # Process each filing with our existing pipeline
    results = []
    for filing_metadata in filing_metadatas:
        # Process the filing with our existing functions
        result = process_filing(filing_metadata)
        results.append(result)
    
    return results

def process_filing(filing_metadata, include_html=True, include_xbrl=True):
    """Process a single filing and upload to cloud storage with enhanced processing"""
    try:
        ticker = filing_metadata.get("ticker")
        company_name = filing_metadata.get("company_name")
        filing_type = filing_metadata.get("filing_type")
        filing_date = filing_metadata.get("filing_date")
        period_end_date = filing_metadata.get("period_end_date")
        fiscal_year = filing_metadata.get("fiscal_year")
        fiscal_period = filing_metadata.get("fiscal_period")
        cik = filing_metadata.get("cik")
        
        logging.info(f"Processing filing: {ticker} {filing_type} for period {period_end_date}")
        
        # Track results
        results = {
            "ticker": ticker,
            "filing_type": filing_type,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "status": "processing"
        }
        
        # Create output directories
        output_dir = os.path.join(PROCESSED_DATA_DIR, ticker, filing_type)
        if fiscal_year and fiscal_period:
            output_dir = os.path.join(output_dir, fiscal_year, fiscal_period)
        os.makedirs(output_dir, exist_ok=True)
        
        # Check if we're using a secedgar filing (local files)
        is_secedgar = filing_metadata.get("is_secedgar", False)
        
        # Initialize variables
        xbrl_processed = False
        html_processed = False
        
        # Process XBRL if requested
        if include_xbrl:
            try:
                # For secedgar, we use the local file path
                if is_secedgar:
                    raw_file_path = filing_metadata.get("instance_url")
                    if os.path.exists(raw_file_path):
                        # Parse the XBRL file
                        parsed_result = parse_xbrl_file(raw_file_path)
                        
                        if "error" in parsed_result:
                            logging.error(f"Error parsing XBRL: {parsed_result['error']}")
                            results["xbrl_error"] = parsed_result["error"]
                        else:
                            # Generate LLM format
                            llm_content = generate_llm_format(parsed_result, filing_metadata)
                            
                            # Save LLM format
                            local_llm_path = os.path.join(output_dir, f"{ticker}_{filing_type}_llm.txt")
                            with open(local_llm_path, 'w', encoding='utf-8') as f:
                                f.write(llm_content)
                            
                            # Check file size
                            llm_size = os.path.getsize(local_llm_path)
                            results["llm_file_size_bytes"] = llm_size
                            results["llm_file_size_mb"] = llm_size / (1024 * 1024)
                            
                            # Upload to GCS
                            llm_gcs_path, llm_size = upload_to_gcs(
                                local_llm_path,
                                ticker,
                                filing_type,
                                fiscal_year,
                                fiscal_period,
                                "llm"
                            )
                            
                            results["llm_file"] = {
                                "local_path": local_llm_path,
                                "gcs_path": llm_gcs_path,
                                "size": llm_size
                            }
                            
                            xbrl_processed = True
                            logging.info(f"Successfully processed XBRL for {ticker} {filing_type}")
                    else:
                        logging.error(f"XBRL file not found: {raw_file_path}")
                        results["xbrl_error"] = "XBRL file not found"
                # Use traditional pipeline for non-secedgar
                else:
                    from src.xbrl.xbrl_downloader import download_xbrl_instance
                    from src.xbrl.xbrl_parser import parse_xbrl_file
                    
                    # Use instance_url from metadata
                    xbrl_url = filing_metadata.get("instance_url")
                    if xbrl_url:
                        download_result = download_xbrl_instance(filing_metadata)
                        
                        if "error" in download_result:
                            logging.error(f"Error downloading XBRL: {download_result['error']}")
                            results["xbrl_error"] = download_result["error"]
                        else:
                            xbrl_file_path = download_result.get("file_path")
                            
                            # Parse the XBRL file
                            parsed_result = parse_xbrl_file(xbrl_file_path)
                            
                            if "error" in parsed_result:
                                logging.error(f"Error parsing XBRL: {parsed_result['error']}")
                                results["xbrl_error"] = parsed_result["error"]
                            else:
                                # Generate LLM format
                                llm_content = generate_llm_format(parsed_result, filing_metadata)
                                
                                # Save LLM format
                                local_llm_path = os.path.join(output_dir, f"{ticker}_{filing_type}_llm.txt")
                                with open(local_llm_path, 'w', encoding='utf-8') as f:
                                    f.write(llm_content)
                                
                                # Check file size
                                llm_size = os.path.getsize(local_llm_path)
                                results["llm_file_size_bytes"] = llm_size
                                results["llm_file_size_mb"] = llm_size / (1024 * 1024)
                                
                                # Upload to GCS
                                llm_gcs_path, llm_size = upload_to_gcs(
                                    local_llm_path,
                                    ticker,
                                    filing_type,
                                    fiscal_year,
                                    fiscal_period,
                                    "llm"
                                )
                                
                                results["llm_file"] = {
                                    "local_path": local_llm_path,
                                    "gcs_path": llm_gcs_path,
                                    "size": llm_size
                                }
                                
                                xbrl_processed = True
                                logging.info(f"Successfully processed XBRL for {ticker} {filing_type}")
                    else:
                        logging.error("No XBRL URL found in metadata")
                        results["xbrl_error"] = "No XBRL URL found in metadata"
            except Exception as e:
                logging.error(f"Error in XBRL processing: {str(e)}")
                results["xbrl_error"] = str(e)
        
        # Process HTML if requested
        if include_html:
            try:
                html_metadata = filing_metadata.copy()
                
                # For secedgar, handle HTML differently
                if is_secedgar:
                    # Find the HTML document file
                    base_dir = filing_metadata.get("local_base_dir")
                    
                    if base_dir and os.path.exists(base_dir):
                        # Look for potential HTML files
                        html_files = list(Path(base_dir).glob("**/*.htm*"))
                        
                        if html_files:
                            # Sort by size (largest first) as main filing is usually largest
                            html_files.sort(key=lambda x: os.path.getsize(x), reverse=True)
                            
                            # Filter out index files and small files
                            main_html_files = [f for f in html_files 
                                              if "index" not in f.name.lower() 
                                              and os.path.getsize(f) > 50000]
                            
                            if main_html_files:
                                main_html_file = str(main_html_files[0])
                                html_metadata["document_url"] = main_html_file
                                html_metadata["primary_doc_url"] = main_html_file
                                logging.info(f"Using HTML file for extraction: {main_html_file}")
                            else:
                                # Fall back to any HTML file
                                main_html_file = str(html_files[0])
                                html_metadata["document_url"] = main_html_file
                                html_metadata["primary_doc_url"] = main_html_file
                                logging.warning(f"Using fallback HTML file: {main_html_file}")
                        else:
                            logging.error(f"No HTML files found in {base_dir}")
                            results["html_error"] = "No HTML files found"
                            # We'll continue with traditional HTML extraction as fallback
                
                # Process HTML with our existing function
                html_result = process_html_filing(html_metadata)
                
                if "error" in html_result:
                    logging.error(f"Error processing HTML: {html_result['error']}")
                    results["html_error"] = html_result["error"]
                else:
                    local_text_path = html_result.get("text_file_path")
                    
                    if local_text_path and os.path.exists(local_text_path):
                        # Check file size
                        text_size = os.path.getsize(local_text_path)
                        
                        # Upload to GCS
                        text_gcs_path, text_size = upload_to_gcs(
                            local_text_path,
                            ticker,
                            filing_type,
                            fiscal_year,
                            fiscal_period,
                            "text"
                        )
                        
                        results["text_file"] = {
                            "local_path": local_text_path,
                            "gcs_path": text_gcs_path,
                            "size": text_size
                        }
                        
                        html_processed = True
                        logging.info(f"Successfully processed HTML for {ticker} {filing_type}")
                    else:
                        logging.error(f"HTML processing did not produce a text file")
                        results["html_error"] = "No text file produced"
            except Exception as e:
                logging.error(f"Error in HTML processing: {str(e)}")
                results["html_error"] = str(e)
        
        # Set final status
        if xbrl_processed and html_processed:
            results["status"] = "success"
        elif xbrl_processed:
            results["status"] = "partial"
            results["status_detail"] = "XBRL processed, HTML failed"
        elif html_processed:
            results["status"] = "partial"
            results["status_detail"] = "HTML processed, XBRL failed"
        else:
            results["status"] = "failed"
            results["status_detail"] = "Both XBRL and HTML processing failed"
        
        logging.info(f"Filing processing complete with status: {results['status']}")
        return results
    
    except Exception as e:
        logging.error(f"Error processing filing: {str(e)}")
        return {
            "ticker": filing_metadata.get("ticker"),
            "filing_type": filing_metadata.get("filing_type"),
            "status": "error",
            "error": str(e)
        }

def process_ticker_by_calendar(ticker, start_year, end_year, include_10k=True, include_10q=True, max_workers=1):
    """
    Process all filings for a ticker within a calendar year range
    
    Args:
        ticker: Company ticker symbol
        start_year: Start year (inclusive)
        end_year: End year (inclusive)
        include_10k: Whether to include 10-K filings
        include_10q: Whether to include 10-Q filings
        max_workers: Maximum number of concurrent workers
        
    Returns:
        Dict with processing results
    """
    filing_types = []
    if include_10k:
        filing_types.append("10-K")
    if include_10q:
        filing_types.append("10-Q")
    
    if not filing_types:
        logging.error("No filing types specified")
        return {"error": "No filing types specified"}
    
    # Create secedgar client
    client = NetworkClient(retry_count=3, pause=0.5)
    
    # Set up result structure
    results = {
        "ticker": ticker,
        "calendar_range": f"{start_year}-{end_year}",
        "filings_processed": []
    }
    
    # Current year
    current_year = datetime.datetime.now().year
    
    # Process each year
    for year in range(start_year, end_year + 1):
        # For future years, skip
        if year > current_year:
            logging.info(f"Skipping future year {year}")
            continue
            
        logging.info(f"Processing {ticker} for year {year}")
        
        # For each filing type
        for filing_type in filing_types:
            # Determine number of filings to process
            # For current year, we need fewer filings
            count = 1
            if filing_type == "10-K":
                count = 1  # Only one annual report per year
            elif filing_type == "10-Q":
                count = 4  # Up to 4 quarterly reports per year
            
            # Process filings for this year and type
            logging.info(f"Processing {ticker} {filing_type} for {year} (max {count})")
            
            # Use secedgar to find and process filings
            filing_results = process_filing_with_secedgar(ticker, filing_type, count, client)
            
            if not filing_results:
                logging.warning(f"No {filing_type} filings found for {ticker} in {year}")
                continue
                
            # For each filing result
            for filing_result in filing_results:
                filing_year = filing_result.get("fiscal_year")
                
                # Check if this filing belongs to the current calendar year
                if filing_year and str(filing_year) == str(year):
                    results["filings_processed"].append(filing_result)
                    logging.info(f"Added {filing_type} filing for {ticker} {year}")
                else:
                    logging.info(f"Filing year {filing_year} doesn't match target year {year}, skipping")
    
    # Summarize results
    successful_filings = sum(1 for f in results["filings_processed"] if f.get("status") == "success")
    partial_filings = sum(1 for f in results["filings_processed"] if f.get("status") == "partial")
    failed_filings = sum(1 for f in results["filings_processed"] if f.get("status") == "failed" or f.get("status") == "error")
    
    results["summary"] = {
        "total_filings": len(results["filings_processed"]),
        "successful_filings": successful_filings,
        "partial_filings": partial_filings,
        "failed_filings": failed_filings
    }
    
    logging.info(f"Processed {len(results['filings_processed'])} filings for {ticker} ({successful_filings} successful, {partial_filings} partial, {failed_filings} failed)")
    
    return results

def download_filings_by_calendar_years(start_year, end_year, companies=None, 
                                     include_10k=True, include_10q=True, 
                                     max_workers=3):
    """
    Download all SEC filings within the specified calendar year range
    
    Args:
        start_year: Starting calendar year (e.g., 2022)
        end_year: Ending calendar year (e.g., 2025)
        companies: List of company tickers (default: use configured list)
        include_10k: Whether to include 10-K filings
        include_10q: Whether to include 10-Q filings
        max_workers: Maximum parallel workers
        
    Returns:
        Summary of downloaded filings
    """
    # First ensure GCP is configured
    if not configure_gcp():
        logging.error("Failed to configure GCP. Exiting.")
        return {"error": "GCP configuration failed"}
    
    # Use the provided company list, or fallback to default
    if companies:
        company_list = companies
        logging.info(f"Processing specified companies: {company_list}")
    else:
        # This should only happen in test mode
        company_list = [company["ticker"] for company in INITIAL_COMPANIES]
        logging.info(f"Using default companies from config: {company_list}")
    
    logging.info(f"Starting download for {len(company_list)} companies from {start_year} to {end_year}")
    logging.info(f"Companies: {', '.join(company_list)}")
    logging.info(f"Filing types: {'10-K' if include_10k else ''} {'10-Q' if include_10q else ''}")
    logging.info(f"Max workers: {max_workers}")
    
    # Process companies in parallel
    results = {}
    
    # If only processing one company, do it directly
    if len(company_list) == 1:
        ticker = company_list[0]
        result = process_ticker_by_calendar(
            ticker, 
            start_year, 
            end_year, 
            include_10k, 
            include_10q,
            max_workers=1
        )
        results[ticker] = result
    else:
        # Use ThreadPoolExecutor for multiple companies
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_company = {
                executor.submit(
                    process_ticker_by_calendar, 
                    ticker, 
                    start_year, 
                    end_year, 
                    include_10k, 
                    include_10q,
                    max_workers=1  # Single worker per company
                ): ticker for ticker in company_list
            }
            
            for future in as_completed(future_to_company):
                ticker = future_to_company[future]
                try:
                    result = future.result()
                    results[ticker] = result
                    logging.info(f"Completed processing for {ticker}")
                except Exception as e:
                    results[ticker] = {"error": str(e)}
                    logging.error(f"Failed processing for {ticker}: {str(e)}")
    
    # Summarize results
    successful_companies = sum(1 for r in results.values() if "error" not in r)
    
    # Count filings
    total_filings = sum(
        len(r.get("filings_processed", [])) 
        for r in results.values() 
        if "error" not in r
    )
    successful_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") == "success")
        for r in results.values() 
        if "error" not in r
    )
    partial_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") == "partial")
        for r in results.values() 
        if "error" not in r
    )
    failed_filings = sum(
        sum(1 for f in r.get("filings_processed", []) if f.get("status") in ["failed", "error"])
        for r in results.values() 
        if "error" not in r
    )
    
    # Find small files (less than 1MB)
    small_files = []
    for ticker, result in results.items():
        if "error" not in result:
            for filing in result.get("filings_processed", []):
                if filing.get("status") == "success":
                    # Check LLM file size
                    llm_file = filing.get("llm_file", {})
                    if "size" in llm_file and llm_file["size"] < 1024 * 1024:
                        small_files.append({
                            "ticker": ticker,
                            "filing_type": filing.get("filing_type"),
                            "period": filing.get("fiscal_period"),
                            "year": filing.get("fiscal_year"),
                            "size_mb": llm_file["size"] / 1024 / 1024,
                            "path": llm_file.get("local_path")
                        })
    
    summary = {
        "calendar_range": f"{start_year}-{end_year}",
        "companies_processed": len(results),
        "successful_companies": successful_companies,
        "failed_companies": len(results) - successful_companies,
        "total_filings_found": total_filings,
        "successful_filings": successful_filings,
        "partial_filings": partial_filings,
        "failed_filings": failed_filings,
        "small_files_count": len(small_files),
        "small_files": small_files,
        "details": results
    }
    
    logging.info(f"Download summary: {summary['successful_filings']}/{summary['total_filings_found']} filings processed successfully")
    logging.info(f"Partial filings: {summary['partial_filings']}")
    logging.info(f"Failed filings: {summary['failed_filings']}")
    
    # Log information about small files
    if small_files:
        logging.info(f"Found {len(small_files)} files below 1MB:")
        for file_info in small_files:
            logging.info(f"  - {file_info['ticker']} {file_info['filing_type']} {file_info['year']} {file_info['period']}: {file_info['size_mb']:.2f} MB - {file_info['path']}")
    
    return summary

def process_single_filing(ticker, filing_type, gcp_upload=True):
    """Process a single filing for a specific ticker and filing type"""
    
    if gcp_upload:
        if not configure_gcp():
            logging.error("Failed to configure GCP. Exiting.")
            return {"error": "GCP configuration failed"}
    else:
        logging.info("Skipping GCP upload as requested.")
        os.environ["SKIP_GCP_UPLOAD"] = "1"
    
    logging.info(f"Processing latest {filing_type} filing for {ticker}")
    
    # Create secedgar client
    client = NetworkClient(retry_count=3, pause=0.5)
    
    # Process the filing
    filing_results = process_filing_with_secedgar(ticker, filing_type, count=1, client=client)
    
    if not filing_results:
        return {"error": f"No {filing_type} filing found for {ticker}"}
    
    return {
        "ticker": ticker,
        "filing_type": filing_type,
        "result": filing_results[0]
    }

def main():
    """Main entry point for the streamlined pipeline"""
    parser = argparse.ArgumentParser(description="Streamlined SEC Filing Processing Pipeline")
    
    # Primary operation modes
    parser.add_argument("--calendar-range", action="store_true", help="Process filings by calendar year range")
    parser.add_argument("--single-filing", action="store_true", help="Process a single filing for a specific ticker")
    
    # Calendar range parameters
    parser.add_argument("--start-year", type=int, help="Starting calendar year (e.g., 2022)")
    parser.add_argument("--end-year", type=int, help="Ending calendar year (e.g., 2025)")
    
    # Single filing parameters
    parser.add_argument("--ticker", help="Company ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], help="Type of filing to process")
    
    # Common parameters
    parser.add_argument("--tickers", nargs="+", help="Specific company tickers to process")
    parser.add_argument("--skip-10k", action="store_true", help="Skip 10-K filings")
    parser.add_argument("--skip-10q", action="store_true", help="Skip 10-Q filings")
    parser.add_argument("--workers", type=int, default=3, help="Maximum number of parallel workers")
    parser.add_argument("--skip-gcp", action="store_true", help="Skip uploading to GCP")
    
    # Test mode
    parser.add_argument("--test", action="store_true", help="Run in test mode with limited scope")
    
    args = parser.parse_args()
    
    # Set environment variables based on arguments
    if args.skip_gcp:
        os.environ["SKIP_GCP_UPLOAD"] = "1"
        logging.info("Setting SKIP_GCP_UPLOAD=1 environment variable")
    
    # Set up test mode if requested
    if args.test:
        logging.info("Running in TEST MODE with limited scope")
        args.tickers = ["MSFT"]  # Just process Microsoft
        args.start_year = 2022
        args.end_year = 2023
        args.workers = 1
    
    # Determine processing mode
    if args.calendar_range or (args.start_year and args.end_year):
        # Validate years
        if not args.start_year or not args.end_year:
            parser.error("Both --start-year and --end-year are required for calendar range mode")
            
        if args.start_year > args.end_year:
            parser.error("start_year must be less than or equal to end_year")
            
        # Require tickers for calendar range mode
        if not args.tickers and not args.test:
            parser.error("At least one ticker must be specified with --tickers for calendar range mode")
            
        # Get company list
        companies = args.tickers
        
        # Process filings by calendar range
        download_filings_by_calendar_years(
            start_year=args.start_year,
            end_year=args.end_year,
            companies=companies,
            include_10k=not args.skip_10k,
            include_10q=not args.skip_10q,
            max_workers=args.workers
        )
        
    elif args.single_filing or (args.ticker and args.filing_type):
        # Validate parameters
        if not args.ticker or not args.filing_type:
            parser.error("Both --ticker and --filing-type are required for single filing mode")
            
        # Process a single filing
        result = process_single_filing(
            ticker=args.ticker,
            filing_type=args.filing_type,
            gcp_upload=not args.skip_gcp
        )
        
        # Report result
        if "error" in result:
            logging.error(f"Error processing filing: {result['error']}")
        else:
            filing_result = result.get("result", {})
            status = filing_result.get("status", "unknown")
            
            if status == "success":
                logging.info(f"Successfully processed {args.ticker} {args.filing_type}")
                
                # Get file paths and sizes
                text_file = filing_result.get('text_file', {})
                llm_file = filing_result.get('llm_file', {})
                text_path = text_file.get('local_path')
                llm_path = llm_file.get('local_path')
                
                # Display file sizes
                text_size = text_file.get('size', 0)
                llm_size = llm_file.get('size', 0)
                text_size_mb = text_size / 1024 / 1024
                llm_size_mb = llm_size / 1024 / 1024
                
                logging.info(f"Text file: {text_path} ({text_size_mb:.2f} MB)")
                logging.info(f"LLM file: {llm_path} ({llm_size_mb:.2f} MB)")
                
                # Check for small files
                if llm_size < 1024 * 1024:
                    logging.warning(f"WARNING: LLM file size is below 1MB ({llm_size_mb:.2f} MB). File may be incomplete.")
                
            elif status == "partial":
                logging.info(f"Partially processed {args.ticker} {args.filing_type}")
                logging.info(f"Status detail: {filing_result.get('status_detail', 'unknown')}")
            else:
                logging.warning(f"Failed to process: {filing_result}")
    
    else:
        # No valid mode specified, show help
        parser.print_help()

if __name__ == "__main__":
    main()