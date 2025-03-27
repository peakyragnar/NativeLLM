#!/usr/bin/env python3
"""
Direct SEC EDGAR Processing Pipeline

This script handles the complete process:
1. Download SEC filings using direct HTTP requests (no secedgar)
2. Process XBRL data
3. Extract and format text from HTML
4. Generate LLM-friendly format
5. Upload to GCP (optional)

This avoids the JSON parsing issues with secedgar.
"""

import os
import sys
import json
import time
import logging
import argparse
import concurrent.futures
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("direct_pipeline.log"),
        logging.StreamHandler()
    ]
)

# Import DirectEdgarDownloader
from src2.downloader.direct_edgar_downloader import DirectEdgarDownloader

# Import processing modules
from src.edgar.edgar_utils import get_company_name_from_cik
from src.xbrl.enhanced_processor import process_company_filing
from src2.xbrl.html_text_extractor import process_html_filing
from src.formatter.llm_formatter import generate_llm_format, save_llm_format
from src.config import PROCESSED_DATA_DIR, USER_AGENT

# Import for GCP
try:
    from google.cloud import storage, firestore
except ImportError:
    logging.warning("Google Cloud libraries not found. Cloud storage integration will be disabled.")

# GCP settings (override with environment variables)
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "native-llm-filings")
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
        # Remove "FY" from folder names, use just the year
        period_path = fiscal_period
        if period_path == "FY":
            period_path = ""
            
        quarter_path = f"{period_path}/{file_format}.txt" if period_path else f"{file_format}.txt"
        mock_gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_path}"
        return mock_gcs_path, os.path.getsize(local_file_path)
        
    # Standardize the period for folder naming
    quarter_folder = fiscal_period
    
    # Remove "FY" from folder names, use just the year
    if quarter_folder == "FY":
        quarter_folder = ""
    
    # Construct GCS path
    quarter_path = f"{quarter_folder}/{file_format}.txt" if quarter_folder else f"{file_format}.txt"
    gcs_path = f"companies/{ticker}/{filing_type}/{fiscal_year}/{quarter_path}"
    
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

def adapt_direct_edgar_to_metadata(result):
    """
    Adapt DirectEdgarDownloader result to our metadata format
    
    Args:
        result: Result from DirectEdgarDownloader.download_filing
        
    Returns:
        List of filing metadata dictionaries
    """
    ticker = result.get("ticker")
    filing_type = result.get("filing_type")
    cik = result.get("cik")
    
    if "error" in result:
        logging.error(f"Error in DirectEdgarDownloader result: {result['error']}")
        return []
    
    filings = result.get("filings", [])
    if not filings:
        logging.error(f"No filings found in DirectEdgarDownloader result")
        return []
    
    # Get company name from CIK
    company_name = get_company_name_from_cik(cik)
    if not company_name:
        company_name = f"Company {ticker}"
    
    filing_metadatas = []
    for filing in filings:
        accession_number = filing.get("accession_number")
        filing_date = filing.get("filing_date")
        period_end_date = filing.get("period_end_date")
        
        # Extract fiscal year from period end date
        fiscal_year = None
        if period_end_date and len(period_end_date) >= 4:
            try:
                fiscal_year = period_end_date.split('-')[0]
            except Exception:
                pass
        
        # Extract fiscal period from period end date
        fiscal_period = None
        if period_end_date and len(period_end_date) >= 10:
            try:
                date_parts = period_end_date.split('-')
                month = int(date_parts[1])
                
                # Fiscal period logic
                if filing_type == "10-K":
                    fiscal_period = "FY"
                elif filing_type == "10-Q":
                    if month in [1, 2, 3, 4]:
                        fiscal_period = "Q1"
                    elif month in [5, 6, 7]:
                        fiscal_period = "Q2"
                    else:
                        fiscal_period = "Q3"
            except Exception:
                pass
        
        # Create metadata
        metadata = {
            "ticker": ticker,
            "company_name": company_name,
            "cik": cik,
            "filing_type": filing_type,
            "accession_number": accession_number,
            "instance_url": filing.get("xbrl_file"),  # Local path to XBRL file
            "document_url": filing.get("html_file"),  # Local path to HTML file
            "primary_doc_url": filing.get("html_file"),  # Same for compatibility
            "index_url": filing.get("index_file"),  # Path to index file
            "local_base_dir": filing.get("base_dir"),  # Base directory with all files
            "is_direct_edgar": True,  # Flag to indicate this is from direct edgar
            "filing_date": filing_date,
            "period_end_date": period_end_date,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period
        }
        
        filing_metadatas.append(metadata)
    
    return filing_metadatas

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
        
        # Initialize variables
        xbrl_processed = False
        html_processed = False
        
        # Process XBRL if requested
        if include_xbrl:
            try:
                # Use the local file path from direct_edgar_downloader
                xbrl_file_path = filing_metadata.get("instance_url")
                if xbrl_file_path and os.path.exists(xbrl_file_path):
                    # Parse the XBRL file
                    from src2.xbrl.xbrl_parser import parse_xbrl_file
                    
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
                        llm_size_mb = llm_size / (1024 * 1024)
                        results["llm_file_size_bytes"] = llm_size
                        results["llm_file_size_mb"] = llm_size_mb
                        logging.info(f"LLM file size: {llm_size_mb:.2f} MB")
                        
                        # Warn about small files
                        if llm_size_mb < 1.0:
                            logging.warning(f"LLM file size is unusually small: {llm_size_mb:.2f} MB")
                        
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
                    logging.error(f"XBRL file not found: {xbrl_file_path}")
                    results["xbrl_error"] = "XBRL file not found"
            except Exception as e:
                logging.error(f"Error in XBRL processing: {str(e)}")
                results["xbrl_error"] = str(e)
        
        # Process HTML if requested
        if include_html:
            try:
                html_file_path = filing_metadata.get("document_url")
                
                if html_file_path and os.path.exists(html_file_path):
                    # Process HTML with our existing function
                    html_result = process_html_filing(filing_metadata)
                    
                    if "error" in html_result:
                        logging.error(f"Error processing HTML: {html_result['error']}")
                        results["html_error"] = html_result["error"]
                    else:
                        local_text_path = html_result.get("text_file_path")
                        
                        if local_text_path and os.path.exists(local_text_path):
                            # Check file size
                            text_size = os.path.getsize(local_text_path)
                            text_size_mb = text_size / (1024 * 1024)
                            logging.info(f"Text file size: {text_size_mb:.2f} MB")
                            
                            # Warn about small files
                            if text_size_mb < 1.0:
                                logging.warning(f"Text file size is unusually small: {text_size_mb:.2f} MB")
                            
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
                else:
                    logging.error(f"HTML file not found: {html_file_path}")
                    results["html_error"] = "HTML file not found"
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

def process_filing_with_direct_edgar(ticker, filing_type, count=1, user_agent=USER_AGENT):
    """
    Process filings for a ticker using DirectEdgarDownloader
    
    Args:
        ticker: Company ticker symbol
        filing_type: Filing type (10-K, 10-Q, etc.)
        count: Number of filings to process
        user_agent: User agent for SEC EDGAR
        
    Returns:
        List of processed filing results
    """
    # Create downloader
    downloader = DirectEdgarDownloader(user_agent)
    logging.info(f"Created DirectEdgarDownloader with user agent: {user_agent}")
    
    # Download filings
    result = downloader.download_filing(ticker, filing_type, count)
    
    if "error" in result:
        logging.error(f"Failed to download {filing_type} filings for {ticker}: {result['error']}")
        return []
    
    # Adapt to our metadata format
    filing_metadatas = adapt_direct_edgar_to_metadata(result)
    
    if not filing_metadatas:
        logging.error(f"Failed to extract filing metadata for {ticker} {filing_type}")
        return []
    
    # Process each filing with our existing pipeline
    results = []
    for filing_metadata in filing_metadatas:
        # Process the filing with our existing function
        result = process_filing(filing_metadata)
        results.append(result)
    
    return results

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
    
    # Process the filing with direct edgar downloader
    filing_results = process_filing_with_direct_edgar(ticker, filing_type, count=1)
    
    if not filing_results:
        return {"error": f"No {filing_type} filing found for {ticker}"}
    
    return {
        "ticker": ticker,
        "filing_type": filing_type,
        "result": filing_results[0]
    }

def main():
    """Main entry point for the direct pipeline"""
    parser = argparse.ArgumentParser(description="Direct SEC EDGAR Processing Pipeline")
    
    # Single filing parameters
    parser.add_argument("ticker", help="Company ticker symbol")
    parser.add_argument("--filing-type", choices=["10-K", "10-Q"], default="10-K", help="Type of filing to process")
    
    # Output options
    parser.add_argument("--output", help="Output file for results (JSON format)")
    
    # GCP options
    parser.add_argument("--skip-gcp", action="store_true", help="Skip uploading to GCP")
    
    args = parser.parse_args()
    
    # Set environment variables based on arguments
    if args.skip_gcp:
        os.environ["SKIP_GCP_UPLOAD"] = "1"
        logging.info("Setting SKIP_GCP_UPLOAD=1 environment variable")
    
    print(f"\n⏳ Starting direct SEC EDGAR pipeline for {args.ticker} {args.filing_type}...")
    
    # Process a single filing
    result = process_single_filing(
        ticker=args.ticker,
        filing_type=args.filing_type,
        gcp_upload=not args.skip_gcp
    )
    
    # Report result
    if "error" in result:
        print(f"\n❌ Error processing filing: {result['error']}")
    else:
        filing_result = result.get("result", {})
        status = filing_result.get("status", "unknown")
        
        if status == "success":
            print(f"\n✅ Successfully processed {args.ticker} {args.filing_type}")
            
            # Get file paths and sizes
            text_file = filing_result.get('text_file', {})
            llm_file = filing_result.get('llm_file', {})
            text_path = text_file.get('local_path')
            llm_path = llm_file.get('local_path')
            text_gcs_path = text_file.get('gcs_path')
            llm_gcs_path = llm_file.get('gcs_path')
            
            # Display file sizes
            text_size = text_file.get('size', 0)
            llm_size = llm_file.get('size', 0)
            text_size_mb = text_size / 1024 / 1024
            llm_size_mb = llm_size / 1024 / 1024
            
            print(f"Text file: {text_path} ({text_size_mb:.2f} MB)")
            if not args.skip_gcp:
                print(f"       GCS: gs://{GCS_BUCKET_NAME}/{text_gcs_path}")
                
            # Check if ixbrl file format was detected
            filing_metadata = filing_result.get('filing_metadata', {})
            if text_path and os.path.exists(text_path):
                with open(text_path, 'r', encoding='utf-8') as f:
                    text_content = f.read()
                    if "iXBRL VIEWER DOCUMENT DETECTED" in text_content:
                        # Extract URL if present
                        import re
                        sec_url_match = re.search(r'Visit the SEC website directly with this URL\s+(.+?)$', text_content, re.MULTILINE)
                        if sec_url_match and "CIK/ACCESSION" not in sec_url_match.group(1):
                            # Use the extracted URL if it's not a placeholder
                            sec_url = sec_url_match.group(1).strip()
                            print(f"📊 Document Format: iXBRL (requires SEC website viewer)")
                            print(f"🔗 SEC Viewer URL: {sec_url}")
                        else:
                            # Construct URL from metadata
                            cik = filing_metadata.get('cik', '').lstrip('0')
                            accession = filing_metadata.get('accession_number', '').replace('-', '')
                            file_name = os.path.basename(filing_metadata.get('document_url', ''))
                            
                            print(f"📊 Document Format: iXBRL (requires SEC website viewer)")
                            print(f"🔗 SEC Viewer URL: https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{accession}/{file_name}")
                            
                            # For Microsoft specifically, construct the exact URL
                            if filing_metadata.get('ticker') == "MSFT" and "2024" in filing_metadata.get('period_end_date', ''):
                                print(f"🔗 SEC Direct URL: https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000095017024087843/msft-20240630.htm")
                
            print(f"LLM file: {llm_path} ({llm_size_mb:.2f} MB)")
            if not args.skip_gcp:
                print(f"       GCS: gs://{GCS_BUCKET_NAME}/{llm_gcs_path}")
            
            # Check for small files
            if llm_size < 1024 * 1024:
                print(f"\n⚠️  WARNING: LLM file size is below 1MB ({llm_size_mb:.2f} MB). File may be incomplete.")
            
        elif status == "partial":
            print(f"\n⚠️ Partially processed {args.ticker} {args.filing_type}")
            print(f"Status detail: {filing_result.get('status_detail', 'unknown')}")
            
            # Show what worked
            if 'text_file' in filing_result:
                text_file = filing_result.get('text_file', {})
                text_path = text_file.get('local_path')
                text_gcs_path = text_file.get('gcs_path')
                text_size = text_file.get('size', 0)
                text_size_mb = text_size / 1024 / 1024
                
                print(f"Text file: {text_path} ({text_size_mb:.2f} MB)")
                if not args.skip_gcp:
                    print(f"       GCS: gs://{GCS_BUCKET_NAME}/{text_gcs_path}")
            
            if 'llm_file' in filing_result:
                llm_file = filing_result.get('llm_file', {})
                llm_path = llm_file.get('local_path')
                llm_gcs_path = llm_file.get('gcs_path')
                llm_size = llm_file.get('size', 0)
                llm_size_mb = llm_size / 1024 / 1024
                
                print(f"LLM file: {llm_path} ({llm_size_mb:.2f} MB)")
                if not args.skip_gcp:
                    print(f"       GCS: gs://{GCS_BUCKET_NAME}/{llm_gcs_path}")
                
                # Check for small files
                if llm_size < 1024 * 1024:
                    print(f"\n⚠️  WARNING: LLM file size is below 1MB ({llm_size_mb:.2f} MB). File may be incomplete.")
            
            # Show errors
            if 'html_error' in filing_result:
                print(f"\nHTML Error: {filing_result['html_error']}")
                
            if 'xbrl_error' in filing_result:
                print(f"\nXBRL Error: {filing_result['xbrl_error']}")
        else:
            print(f"\n❌ Failed to process: {filing_result}")
            
            # Show errors
            if 'html_error' in filing_result:
                print(f"\nHTML Error: {filing_result['html_error']}")
                
            if 'xbrl_error' in filing_result:
                print(f"\nXBRL Error: {filing_result['xbrl_error']}")
    
    # Save results to JSON file if requested
    if args.output:
        try:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"\nResults saved to {args.output}")
        except Exception as e:
            print(f"\nError saving results: {str(e)}")
    
    # Display final summary
    print("\n📊 Processing Summary:")
    if "result" in result and "text_file" in result["result"]:
        text_size = result["result"]["text_file"].get("size", 0)
        text_size_mb = text_size / 1024 / 1024
        print(f"  - Text file size: {text_size_mb:.2f} MB")
        
        if text_size_mb < 1.0:
            print(f"  ⚠️  WARNING: Text file is unusually small ({text_size_mb:.2f} MB) - possible extraction issue")
        
    if "result" in result and "llm_file" in result["result"]:
        llm_size = result["result"]["llm_file"].get("size", 0)
        llm_size_mb = llm_size / 1024 / 1024
        print(f"  - LLM file size: {llm_size_mb:.2f} MB")
        
        if llm_size_mb < 1.0:
            print(f"  ⚠️  WARNING: LLM file is unusually small ({llm_size_mb:.2f} MB) - possible extraction issue")

if __name__ == "__main__":
    main()